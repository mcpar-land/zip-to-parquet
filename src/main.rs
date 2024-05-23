use anyhow::bail;
use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
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

/// Convert .zip file to parquet of all files inside
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
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
	// do not include the SHA-256 hash column in output
	#[arg(long)]
	no_hash: bool,
	/// filter files by glob (example: "**/*.png")
	#[arg(long, short)]
	glob: Option<String>,
}

const BLOCK_SIZE: usize = 512 * 1024 * 1024;

fn main() -> Result<(), anyhow::Error> {
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
		if let Err(err) = Glob::new(glob) {
			bail!("Invalid glob \"{}\": {}", glob, err);
		}
	}

	let output = match (&args.output, args.stdout) {
		(Some(output), false) => FileOrStdout::File {
			buf: BufWriter::new(std::fs::File::create(&output)?),
			path: output.clone(),
		},
		(None, true) => FileOrStdout::Stdout(BufWriter::new(std::io::stdout())),
		(Some(_), true) => {
			bail!("Must provide an output file or --stdout, but not both");
		}
		(None, false) => {
			bail!("Must provide and output file or --stdout");
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

	let mut writer = ArrowWriter::try_new(output, schema, Some(props))?;

	let mut all_inputs = Vec::new();
	for input_glob in &args.input {
		for entry in glob::glob(input_glob)? {
			all_inputs.push(entry?);
		}
	}

	if all_inputs.len() == 0 {
		bail!("No files found for glob(s) {:?}", args.input);
	}
	let progress_chars = "█▉▊▋▌▍▎▏  ";
	// let progress_chars = "█▓▒░  ";

	let bar = ProgressBar::new(0);

	bar.set_style(ProgressStyle::with_template(
		"{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({per_sec})"
	).unwrap().progress_chars(progress_chars));

	let (overall_bar, bar) = if all_inputs.len() == 1 {
		(None, bar)
	} else {
		let mp = MultiProgress::new();
		let overall_bar = ProgressBar::new(all_inputs.len() as u64);
		overall_bar.set_style(
			ProgressStyle::with_template(
				"{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({eta})",
			)
			.unwrap()
			.progress_chars(progress_chars),
		);
		let overall_bar = mp.add(overall_bar);
		overall_bar.set_position(0);
		let bar = mp.add(bar);
		(Some(overall_bar), bar)
	};

	for path in &all_inputs {
		writer = write_from_stream(path, writer, &args, &terminated, &bar)?;
		if let Some(overall_bar) = &overall_bar {
			overall_bar.inc(1);
		}
	}

	bar.finish_and_clear();
	if let Some(overall_bar) = &overall_bar {
		overall_bar.finish();
	}
	eprintln!("Done!");

	writer.close()?;

	Ok(())
}

fn write_from_stream(
	path: &PathBuf,
	mut writer: ArrowWriter<FileOrStdout>,
	args: &Args,
	terminated: &Arc<AtomicBool>,
	bar: &ProgressBar,
) -> Result<ArrowWriter<FileOrStdout>, anyhow::Error> {
	bar.reset();

	let glob = args.glob.as_ref().map(|glob| Glob::new(&glob).unwrap());

	let instant = Instant::now();
	bar.println(format!("Writing from {}...", path.to_string_lossy()));
	// Use a BufReader to read from a file that's larger than memory.
	let file = BufReader::new(std::fs::File::open(path)?);
	let mut input = ZipArchive::new(file)?;
	bar.println(format!(
		"Finished reading zip archive central directory (took {:?})",
		instant.elapsed()
	));

	let mut file_names = Vec::<String>::new();
	let mut file_sources = Vec::<Option<String>>::new();
	let mut file_contents = Vec::<Option<Vec<u8>>>::new();
	let mut file_hashes = Vec::<Option<String>>::new();
	let mut block_size: usize = 0;

	bar.set_length(input.len() as u64);

	for i in 0..input.len() {
		writer = handle_terminate(terminated, Some(&bar), writer);
		bar.inc(1);
		let file = input.by_index(i)?;
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
			let file_body =
				file.bytes().collect::<Result<Vec<u8>, std::io::Error>>()?;
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
				Some(format!("{:x?}", hash))
			};
			let body = if args.no_body { None } else { Some(file_body) };
			(body, hash)
		};

		let source = if args.no_source {
			None
		} else {
			Some(path.to_string_lossy().to_string())
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
	)?;

	bar.println(format!(
		"Finished writing {} files (took {:?})",
		input.len(),
		bar.elapsed()
	));

	Ok(writer)
}

fn write_chunk(
	mut writer: ArrowWriter<FileOrStdout>,
	file_names: &mut Vec<String>,
	file_sources: &mut Vec<Option<String>>,
	file_contents: &mut Vec<Option<Vec<u8>>>,
	file_hashes: &mut Vec<Option<String>>,
	terminated: &Arc<AtomicBool>,
) -> Result<ArrowWriter<FileOrStdout>, anyhow::Error> {
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
	writer.flush()?;
	writer = handle_terminate(terminated, None, writer);
	// println!("Wrote chunk with {} items", n_items);
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
	progress: Option<&ProgressBar>,
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
