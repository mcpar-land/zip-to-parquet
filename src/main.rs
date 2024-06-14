use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use clap::Parser;
use error::Error;
use logger::Logger;
use parquet::{
	arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use sha2::{Digest, Sha256};
use std::{
	io::{BufReader, BufWriter, Read},
	path::PathBuf,
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
	time::Instant,
};
use wax::{Glob, Pattern};
use zip::ZipArchive;

pub mod error;
pub mod logger;

/// Convert .zip file to parquet of all files inside
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Args {
	/// .zip file input path (Can be specified multiple times. Can be a glob. example: "**/*.zip")
	#[arg(long, short)]
	input: Vec<String>,
	/// .parquet file output path (can only be specified once)
	#[arg(long, short)]
	output: Option<PathBuf>,
	/// use stdout for output
	#[arg(long)]
	stdout: bool,
	/// do not load or include file bodies in output (significantly reduce size and time!)
	#[arg(long)]
	no_body: bool,
	/// do not include zip file source column in output
	#[arg(long)]
	no_source: bool,
	/// do not include the SHA-256 hash column in output
	#[arg(long)]
	no_hash: bool,
	/// simple logging output instead of progress bars
	#[arg(long)]
	simple: bool,
	/// filter files by glob (example: "**/*.png")
	#[arg(long, short)]
	glob: Option<String>,
}

const BLOCK_SIZE: usize = 512 * 1024 * 1024;

fn main() {
	if let Err(err) = run() {
		eprintln!("{}", err);
		std::process::exit(1);
	}
}

fn run() -> Result<(), Error> {
	let args = Args::parse();

	let terminated = Arc::new(AtomicBool::new(false));

	{
		let terminated = terminated.clone();
		ctrlc::set_handler(move || {
			terminated.store(true, std::sync::atomic::Ordering::Relaxed);
		})
		.expect("Error setting ctrl+c handler");
	};

	if let Some(glob) = &args.glob {
		Glob::new(glob).map_err(|err| Error::InvalidWaxGlob {
			err,
			glob: glob.clone(),
		})?;
	}

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
	match write(&args, writer, &terminated) {
		Ok(writer) => {
			writer.close()?;
			eprintln!("Done!")
		}
		Err(err) => {
			eprintln!("Error: {}", err);
			if let Some(output) = &args.output {
				eprintln!(
					"Deleting incomplete file {} ...",
					output.as_os_str().to_string_lossy()
				);
				std::fs::remove_file(&output)
					.expect("error when trying to delete incomplete file");
				std::process::exit(1);
			}
		}
	}

	Ok(())
}

fn write(
	args: &Args,
	mut writer: ArrowWriter<FileOrStdout>,
	terminated: &Arc<AtomicBool>,
) -> Result<ArrowWriter<FileOrStdout>, Error> {
	let mut all_inputs = Vec::new();
	for input_glob in &args.input {
		for entry in glob::glob(input_glob).map_err(|err| Error::InvalidGlob {
			glob: input_glob.clone(),
			err,
		})? {
			all_inputs.push(entry?);
		}
	}

	if all_inputs.len() == 0 {
		return Err(Error::NoInputsFound {
			globs: args.input.clone(),
		});
	}

	let mut logger = Logger::new(&args, all_inputs.len() as u64);

	for path in &all_inputs {
		writer = write_from_stream(path, writer, &args, &terminated, &mut logger)?;
	}

	logger.finish();

	Ok(writer)
}

fn write_from_stream(
	path: &PathBuf,
	mut writer: ArrowWriter<FileOrStdout>,
	args: &Args,
	terminated: &Arc<AtomicBool>,
	logger: &mut Logger,
) -> Result<ArrowWriter<FileOrStdout>, Error> {
	let glob = args.glob.as_ref().map(|glob| Glob::new(&glob).unwrap());

	let instant = Instant::now();
	logger.println(format!("Writing from {}...", path.to_string_lossy()));
	// Use a BufReader to read from a file that's larger than memory.
	let file = BufReader::new(std::fs::File::open(path).map_err(|err| {
		Error::ReadFile {
			err,
			file: path.clone(),
		}
	})?);
	let mut input = ZipArchive::new(file).map_err(|err| Error::Zip {
		err,
		file: path.clone(),
	})?;
	logger.println(format!(
		"Finished reading zip archive central directory (took {:?})",
		instant.elapsed()
	));

	let mut file_names = Vec::<String>::new();
	let mut file_sources = Vec::<Option<String>>::new();
	let mut file_contents = Vec::<Option<Vec<u8>>>::new();
	let mut file_hashes = Vec::<Option<String>>::new();
	let mut block_size: usize = 0;

	logger.next_overall(
		format!("Writing from {}...", path.to_string_lossy()),
		input.len() as u64,
	);

	for i in 0..input.len() {
		writer = handle_terminate(terminated, Some(logger), writer);
		let file = input.by_index(i).map_err(|err| Error::Zip {
			err,
			file: path.clone(),
		})?;
		logger.inc(1, &path.to_string_lossy());
		if file.is_dir() {
			continue;
		}
		if let Some(glob) = &glob {
			if !glob.is_match(file.name()) {
				continue;
			}
		}
		let file_name = file.name().to_string();
		block_size += file_name.as_bytes().len();
		let (file_body, file_hash) = if args.no_body && args.no_hash {
			(None, None)
		} else {
			let file_body = file
				.bytes()
				.collect::<Result<Vec<u8>, std::io::Error>>()
				.map_err(|err| Error::ReadFileInZip {
					err,
					file_name: file_name.clone(),
					file: path.clone(),
				})?;
			block_size += file_body.len();
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
				block_size += hash_str.as_bytes().len();
				Some(hash_str)
			};
			let body = if args.no_body { None } else { Some(file_body) };
			(body, hash)
		};

		let source = if args.no_source {
			None
		} else {
			let source = path.to_string_lossy().to_string();
			block_size += source.as_bytes().len();
			Some(source)
		};

		file_names.push(file_name);
		file_sources.push(source);
		file_contents.push(file_body);
		file_hashes.push(file_hash);

		// write to parquet file and start a new chunk when it reaches 512 mb
		if block_size >= BLOCK_SIZE {
			writer = write_chunk(
				writer,
				&mut file_names,
				&mut file_sources,
				&mut file_contents,
				&mut file_hashes,
				terminated,
				logger,
			)?;
			block_size = 0;
		}
	}

	// write the last chunk which might be smaller than 512 mb
	writer = write_chunk(
		writer,
		&mut file_names,
		&mut file_sources,
		&mut file_contents,
		&mut file_hashes,
		terminated,
		logger,
	)?;

	Ok(writer)
}

fn write_chunk(
	mut writer: ArrowWriter<FileOrStdout>,
	file_names: &mut Vec<String>,
	file_sources: &mut Vec<Option<String>>,
	file_contents: &mut Vec<Option<Vec<u8>>>,
	file_hashes: &mut Vec<Option<String>>,
	terminated: &Arc<AtomicBool>,
	logger: &mut Logger,
) -> Result<ArrowWriter<FileOrStdout>, error::Error> {
	let n_items = file_names.len();
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
	writer.flush()?;
	writer = handle_terminate(terminated, None, writer);
	logger.println(format!("Wrote chunk with {} items", n_items));
	file_names.clear();
	file_names.shrink_to(0);
	file_sources.clear();
	file_sources.shrink_to(0);
	file_contents.clear();
	file_contents.shrink_to(0);
	file_hashes.clear();
	file_hashes.shrink_to(0);
	Ok(writer)
}

// Use BufWriters to handle writing to files larger than memory.
enum FileOrStdout {
	Stdout(BufWriter<std::io::Stdout>),
	File {
		path: PathBuf,
		buf: BufWriter<std::fs::File>,
	},
}

impl std::io::Write for FileOrStdout {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			FileOrStdout::Stdout(stdout) => stdout.write_all(buf)?,
			FileOrStdout::File { buf: file, .. } => file.write_all(buf)?,
		}
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		match self {
			FileOrStdout::Stdout(stdout) => stdout.flush(),
			FileOrStdout::File { buf: file, .. } => file.flush(),
		}
	}
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
