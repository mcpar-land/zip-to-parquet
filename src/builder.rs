use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use parquet::{
	arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use sha2::{Digest, Sha256};
use std::{
	fs::File,
	io::{BufReader, BufWriter, Read},
	path::PathBuf,
	sync::{
		atomic::{AtomicBool, Ordering},
		mpsc::{self, SyncSender},
		Arc,
	},
	thread,
};
use threadpool::ThreadPool;
use wax::{Glob, Pattern};
use zip::ZipArchive;

use crate::{error::Error, logger::Logger, Args, FileOrStdout};

pub fn run(args: &Args, terminated: Arc<AtomicBool>) -> Result<(), Error> {
	let mut total_files = 0;
	for input_glob in &args.input {
		for entry in glob::glob(input_glob).map_err(|err| Error::InvalidGlob {
			glob: input_glob.clone(),
			err,
		})? {
			let input = open_zip(&entry?)?;
			let glob = args.glob.as_ref().map(|glob| Glob::new(&glob).unwrap());
			let n_items = input
				.file_names()
				.filter(|item| match &glob {
					Some(glob) => glob.is_match(*item),
					None => true,
				})
				.count();
			total_files += n_items;
		}
	}

	let bar = make_progress_bar(total_files);

	eprintln!("Found {} total zipped files", total_files);

	let mut writer = make_writer(args)?;

	let n_cores = thread::available_parallelism().unwrap().get();
	let pool = ThreadPool::new(n_cores);

	let rx = {
		let (tx, rx) = mpsc::sync_channel::<UnzippedFile>(args.row_group_size);
		for input_glob in &args.input {
			for entry in glob::glob(input_glob).map_err(|err| Error::InvalidGlob {
				glob: input_glob.clone(),
				err,
			})? {
				let entry = entry?;
				let args = args.clone();
				let terminated = terminated.clone();
				let tx = tx.clone();
				let bar = bar.clone();
				pool.execute(move || {
					if let Err(err) =
						handle_read_zip(entry, args, terminated.clone(), tx, bar)
					{
						eprintln!("Error in zip reading thread: {}", err);
						terminated.store(true, Ordering::Relaxed);
					}
				});
			}
		}
		rx
	};

	let mut file_names = Vec::<String>::with_capacity(args.row_group_size);
	let mut file_sources =
		Vec::<Option<String>>::with_capacity(args.row_group_size);
	let mut file_contents =
		Vec::<Option<Vec<u8>>>::with_capacity(args.row_group_size);
	let mut file_hashes =
		Vec::<Option<String>>::with_capacity(args.row_group_size);

	for input in rx {
		file_names.push(input.name);
		file_sources.push(input.source);
		file_contents.push(input.body);
		file_hashes.push(input.hash);
		bar.inc(1);

		if file_names.len() >= args.row_group_size {
			writer = write_row_group(
				writer,
				&mut file_names,
				&mut file_sources,
				&mut file_contents,
				&mut file_hashes,
				&terminated,
				bar.clone(),
			)?;
		}
	}
	// write the last chunk which might be smaller than the row group size
	// ...but only if there's still some left over!
	if file_names.len() > 0 {
		writer = write_row_group(
			writer,
			&mut file_names,
			&mut file_sources,
			&mut file_contents,
			&mut file_hashes,
			&terminated,
			bar.clone(),
		)?;
	}

	writer.close()?;

	Ok(())
}

fn write_row_group(
	mut writer: ArrowWriter<FileOrStdout>,
	file_names: &mut Vec<String>,
	file_sources: &mut Vec<Option<String>>,
	file_contents: &mut Vec<Option<Vec<u8>>>,
	file_hashes: &mut Vec<Option<String>>,
	terminated: &Arc<AtomicBool>,
	bar: ProgressBar,
) -> Result<ArrowWriter<FileOrStdout>, Error> {
	// let n_items = file_names.len();
	let file_names_column =
		StringArray::from(file_names.drain(0..).collect::<Vec<String>>());
	let file_sources_column =
		StringArray::from(file_sources.drain(0..).collect::<Vec<Option<String>>>());
	let file_contents_column = BinaryArray::from(
		file_contents
			.iter()
			.map(|v| v.as_ref().map(|v| v.as_ref()))
			.collect::<Vec<Option<&[u8]>>>(),
	);
	let file_hashes_column =
		StringArray::from(file_hashes.drain(0..).collect::<Vec<Option<String>>>());
	let batch = RecordBatch::try_from_iter(vec![
		("name", Arc::new(file_names_column) as ArrayRef),
		("source", Arc::new(file_sources_column) as ArrayRef),
		("body", Arc::new(file_contents_column) as ArrayRef),
		("hash", Arc::new(file_hashes_column) as ArrayRef),
	])?;
	writer.write(&batch)?;
	writer = handle_terminate(terminated, None, writer);
	// hgmm maybe we can disable this?
	// writer.flush()?;
	// writer = handle_terminate(terminated, None, writer);
	// logger.println(format!("Wrote row group with {} items", n_items));
	file_names.clear();
	// file_names.shrink_to(0);
	file_sources.clear();
	// file_sources.shrink_to(0);
	file_contents.clear();
	// file_contents.shrink_to(0);
	file_hashes.clear();
	// file_hashes.shrink_to(0);
	Ok(writer)
}
pub struct UnzippedFile {
	pub name: String,
	pub source: Option<String>,
	pub body: Option<Vec<u8>>,
	pub hash: Option<String>,
}

fn handle_read_zip(
	path: PathBuf,
	args: Args,
	terminated: Arc<AtomicBool>,
	tx: SyncSender<UnzippedFile>,
	bar: ProgressBar,
) -> Result<(), Error> {
	let glob = args.glob.as_ref().map(|glob| Glob::new(&glob).unwrap());
	let mut input = open_zip(&path)?;
	for i in 0..input.len() {
		if terminated.load(Ordering::Relaxed) {
			return Ok(());
		}
		let file = input.by_index(i).map_err(|err| Error::Zip {
			err,
			file: path.clone(),
		})?;
		if let Some(glob) = &glob {
			if !glob.is_match(file.name()) {
				continue;
			}
			let name = file.name().to_string();
			let (body, hash) = if args.no_body && args.no_hash {
				(None, None)
			} else {
				let file_body = file
					.bytes()
					.collect::<Result<Vec<u8>, std::io::Error>>()
					.map_err(|err| Error::ReadFileInZip {
						err,
						file_name: name.clone(),
						file: path.clone(),
					})?;
				let hash = if args.no_hash {
					None
				} else {
					let mut hasher = Sha256::new();
					hasher.update(&file_body);
					let hash = hasher
						.finalize()
						.iter()
						.map(|v| format!("{:x}", v))
						.collect::<Vec<String>>()
						.join("");
					let hash_str = format!("{:x?}", hash);
					Some(hash_str)
				};
				let body = if args.no_body { None } else { Some(file_body) };
				(body, hash)
			};
			let source = if args.no_source {
				None
			} else {
				let source = path.to_string_lossy().to_string();
				Some(source)
			};
			tx.send(UnzippedFile {
				name,
				source,
				body,
				hash,
			})
			.map_err(|err| Error::Other(format!("{}", err)))?;
		}
	}
	bar.println(format!("Finished reading {}", path.to_string_lossy()));
	Ok(())
}

fn make_writer(args: &Args) -> Result<ArrowWriter<FileOrStdout>, Error> {
	let output = match (&args.output, args.stdout) {
		(Some(output), false) => FileOrStdout::File {
			buf: BufWriter::new(std::fs::File::create(&output).map_err(|err| {
				Error::WriteFile {
					err,
					target: output.clone(),
				}
			})?),
			path: output.clone(),
		},
		(None, true) => FileOrStdout::Stdout(BufWriter::new(std::io::stdout())),
		(Some(_), true) => {
			return Err(Error::InvalidOutputAndStdout);
		}
		(None, false) => {
			return Err(Error::NeedsOutputOrStdout);
		}
	};

	let props = WriterProperties::builder()
		.set_compression(Compression::SNAPPY)
		.set_max_row_group_size(args.row_group_size)
		.build();
	let schema = RecordBatch::try_from_iter(vec![
		(
			"name",
			Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
		),
		(
			"source",
			Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
		),
		(
			"body",
			Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())) as ArrayRef,
		),
		(
			"hash",
			Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
		),
	])?
	.schema();

	let writer = ArrowWriter::try_new(output, schema, Some(props))?;
	Ok(writer)
}

fn open_zip(path: &PathBuf) -> Result<ZipArchive<BufReader<File>>, Error> {
	let file = BufReader::new(std::fs::File::open(&path).map_err(|err| {
		Error::ReadFile {
			err,
			file: path.clone(),
		}
	})?);
	let input = ZipArchive::new(file).map_err(|err| Error::Zip {
		err,
		file: path.clone(),
	})?;
	Ok(input)
}

fn make_progress_bar(n_items: usize) -> ProgressBar {
	let progress_chars = "█▉▊▋▌▍▎▏  ";
	// let progress_chars = "█▓▒░  ";

	let current_bar = ProgressBar::new(n_items as u64);
	current_bar.set_draw_target(ProgressDrawTarget::stdout());
	current_bar.set_style(ProgressStyle::with_template(
			"{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({per_sec})"
		).unwrap().progress_chars(progress_chars));

	current_bar
}

fn handle_terminate(
	terminated: &Arc<AtomicBool>,
	progress: Option<&mut Logger>,
	handle: ArrowWriter<FileOrStdout>,
) -> ArrowWriter<FileOrStdout> {
	if !terminated.load(Ordering::Relaxed) {
		return handle;
	}
	if let Some(progress) = progress {
		progress.abandon();
	}
	eprintln!("Ctrl-c received! Terminating gracefully...");
	let handle = handle.into_inner().unwrap();
	let file_path = match &handle {
		FileOrStdout::Stdout(_) => None,
		FileOrStdout::File { path, .. } => Some(path.clone()),
	};
	std::mem::drop(handle);
	if let Some(path) = file_path {
		eprintln!(
			"Deleting incomplete file {}...",
			path.as_os_str().to_string_lossy()
		);
		std::fs::remove_file(&path)
			.expect("error when trying to delete incomplete file");
	}

	std::process::exit(0);
}
