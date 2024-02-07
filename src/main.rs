use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use clap::Parser;
use indicatif::ProgressBar;
use parquet::{
	arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use std::{
	fs::File,
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
	input: PathBuf,
	/// .parquet file output path
	output: PathBuf,
	/// Do not load or include file bodies in output (significantly reduce size and time!)
	#[arg(long)]
	no_body: bool,
}

const BLOCK_SIZE: usize = 1024 * 1024 * 1024;

fn main() -> Result<(), anyhow::Error> {
	let args = Args::parse();

	let instant = Instant::now();
	let spinny = ProgressBar::new_spinner()
		.with_message("Reading zip archive central directory...");
	spinny.enable_steady_tick(Duration::from_millis(500));
	let input = std::fs::File::open(&args.input)?;

	let mut input = ZipArchive::new(input)?;

	spinny.finish_with_message(format!(
		"Finished reading zip archive central directory! (took {:?})",
		instant.elapsed()
	));

	println!("Creating output...");
	let output = std::fs::File::create(&args.output)?;

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
			write_chunk(&mut writer, &mut file_names, &mut file_contents)?;
		}
	}

	// write the last chunk which might be smaller than 512 mb
	write_chunk(&mut writer, &mut file_names, &mut file_contents)?;

	writer.close()?;

	bar.finish();

	Ok(())
}

fn write_chunk(
	writer: &mut ArrowWriter<File>,
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
	file_names.clear();
	file_contents.clear();
	Ok(())
}
