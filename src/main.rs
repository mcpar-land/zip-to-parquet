use anyhow::bail;
use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{
	arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use std::{
	io::{BufReader, BufWriter, Read},
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
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
	/// filter files by glob (example: "**/*.png")
	#[arg(long, short)]
	glob: Option<String>,
}

const BLOCK_SIZE: usize = 512 * 1024 * 1024;

fn main() -> Result<(), anyhow::Error> {
	let args = Args::parse();

	if let Some(glob) = &args.glob {
		if let Err(err) = Glob::new(glob) {
			bail!("Invalid glob \"{}\": {}", glob, err);
		}
	}

	let output = match (&args.output, args.stdout) {
		(Some(output), false) => {
			FileOrStdout::File(BufWriter::new(std::fs::File::create(output)?))
		}
		(None, true) => FileOrStdout::Stdout(BufWriter::new(std::io::stdout())),
		(Some(_), true) => {
			bail!("Must provide an output file or --stdout, but not both");
		}
		(None, false) => {
			bail!("Must provide and output file or --stdout");
		}
	};

	eprintln!("Creating output...");

	let props = WriterProperties::builder()
		.set_compression(Compression::SNAPPY)
		.build();

	let schema = RecordBatch::try_from_iter(vec![
		(
			"name",
			Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
		),
		(
			"body",
			Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())) as ArrayRef,
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

	for path in &all_inputs {
		write_from_stream(path, &mut writer, &args)?;
	}

	writer.close()?;

	Ok(())
}

fn write_from_stream(
	path: &PathBuf,
	writer: &mut ArrowWriter<FileOrStdout>,
	args: &Args,
) -> Result<(), anyhow::Error> {
	eprintln!("Writing from {}...", path.to_string_lossy());

	let glob = args.glob.as_ref().map(|glob| Glob::new(&glob).unwrap());

	let instant = Instant::now();
	let spinny = ProgressBar::new_spinner()
		.with_message("Reading zip archive central directory...");
	spinny.enable_steady_tick(Duration::from_millis(500));
	// Use a BufReader to read from a file that's larger than memory.
	let file = BufReader::new(std::fs::File::open(path)?);
	let mut input = ZipArchive::new(file)?;
	spinny.finish_with_message(format!(
		"Finished reading zip archive central directory! (took {:?})",
		instant.elapsed()
	));

	let mut file_names = Vec::<String>::new();
	let mut file_contents = Vec::<Option<Vec<u8>>>::new();
	let mut block_size: usize = 0;

	let bar = ProgressBar::new(input.len() as u64);

	bar.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({per_sec} {eta})").unwrap().progress_chars("=>-"));

	for i in 0..input.len() {
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
		let file_body = if args.no_body {
			None
		} else {
			let file_body =
				file.bytes().collect::<Result<Vec<u8>, std::io::Error>>()?;
			block_size += file_body.len();
			Some(file_body)
		};

		file_names.push(file_name);
		file_contents.push(file_body);

		// write to parquet file and start a new chunk when it reaches 512 mb
		if block_size >= BLOCK_SIZE {
			write_chunk(writer, &mut file_names, &mut file_contents)?;
			block_size = 0;
		}
	}

	// write the last chunk which might be smaller than 512 mb
	write_chunk(writer, &mut file_names, &mut file_contents)?;

	bar.finish();

	Ok(())
}

fn write_chunk(
	writer: &mut ArrowWriter<FileOrStdout>,
	file_names: &mut Vec<String>,
	file_contents: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), anyhow::Error> {
	// let n_items = file_names.len();
	let file_names_column =
		StringArray::from(file_names.drain(0..).collect::<Vec<String>>());
	let file_contents_column = BinaryArray::from(
		file_contents
			.iter()
			.map(|v| v.as_ref().map(|v| v.as_ref()))
			.collect::<Vec<Option<&[u8]>>>(),
	);
	let batch = RecordBatch::try_from_iter(vec![
		("name", Arc::new(file_names_column) as ArrayRef),
		("body", Arc::new(file_contents_column) as ArrayRef),
	])?;
	writer.write(&batch)?;
	writer.flush()?;
	// println!("Wrote chunk with {} items", n_items);
	file_names.clear();
	file_names.shrink_to(0);
	file_contents.clear();
	file_contents.shrink_to(0);
	Ok(())
}

// Use BufWriters to handle writing to files larger than memory.
enum FileOrStdout {
	Stdout(BufWriter<std::io::Stdout>),
	File(BufWriter<std::fs::File>),
}

impl std::io::Write for FileOrStdout {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			FileOrStdout::Stdout(stdout) => stdout.write_all(buf)?,
			FileOrStdout::File(file) => file.write_all(buf)?,
		}
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		match self {
			FileOrStdout::Stdout(stdout) => stdout.flush(),
			FileOrStdout::File(file) => file.flush(),
		}
	}
}
