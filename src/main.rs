use anyhow::bail;
use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use clap::Parser;
use indicatif::ProgressBar;
use parquet::{
	arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use std::{
	io::Read,
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
};
use zip::ZipArchive;

/// Convert .zip file to parquet of all files inside
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
	/// .zip file input path
	#[arg(long, short)]
	input: Vec<PathBuf>,
	/// .parquet file output path
	#[arg(long, short)]
	output: Option<PathBuf>,
	/// use stdout for output
	#[arg(long)]
	stdout: bool,
	/// Do not load or include file bodies in output (significantly reduce size and time!)
	#[arg(long)]
	no_body: bool,
}

const BLOCK_SIZE: usize = 1024 * 1024 * 1024;

fn main() -> Result<(), anyhow::Error> {
	let args = Args::parse();

	eprintln!("Creating output...");
	let output = match (&args.output, args.stdout) {
		(Some(output), false) => FileOrStdout::File(std::fs::File::create(output)?),
		(None, true) => FileOrStdout::Stdout(std::io::stdout()),
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
			"body",
			Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())) as ArrayRef,
		),
	])?
	.schema();

	let mut writer = ArrowWriter::try_new(output, schema, Some(props))?;

	for path in &args.input {
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

	let instant = Instant::now();
	let spinny = ProgressBar::new_spinner()
		.with_message("Reading zip archive central directory...");
	spinny.enable_steady_tick(Duration::from_millis(500));
	let file = std::fs::File::open(path)?;
	let mut input = ZipArchive::new(file)?;
	spinny.finish_with_message(format!(
		"Finished reading zip archive central directory! (took {:?})",
		instant.elapsed()
	));

	let mut file_names = Vec::<String>::new();
	let mut file_contents = Vec::<Option<Vec<u8>>>::new();
	let mut block_size: usize = 0;

	let bar = ProgressBar::new(input.len() as u64);

	for i in 0..input.len() {
		bar.inc(1);
		let file = input.by_index(i)?;
		if file.is_dir() {
			continue;
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
	*file_names = Vec::new();
	*file_contents = Vec::new();
	Ok(())
}

enum FileOrStdout {
	Stdout(std::io::Stdout),
	File(std::fs::File),
}

impl std::io::Write for FileOrStdout {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			FileOrStdout::Stdout(stdout) => stdout.write(buf),
			FileOrStdout::File(file) => file.write(buf),
		}
	}

	fn flush(&mut self) -> std::io::Result<()> {
		match self {
			FileOrStdout::Stdout(stdout) => stdout.flush(),
			FileOrStdout::File(file) => file.flush(),
		}
	}
}
