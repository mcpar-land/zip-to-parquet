use arrow_array::{
	ArrayRef, BinaryArray, GenericByteArray, RecordBatch, StringArray,
};
use clap::Parser;
use itertools::Itertools;
use parquet::{
	arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use std::{io::Read, path::PathBuf, sync::Arc};

#[derive(Parser, Debug)]
struct Args {
	input: PathBuf,
	output: PathBuf,
}

fn main() -> Result<(), anyhow::Error> {
	let args = Args::parse();

	println!("Opening zip archive...");
	let input = std::fs::File::open(&args.input)?;
	let mut archive = zip::ZipArchive::new(input)?;

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
			Arc::new(BinaryArray::from(Vec::<&[u8]>::new())) as ArrayRef,
		),
	])?
	.schema();

	let mut writer = ArrowWriter::try_new(output, schema, Some(props))?;

	let mut file_names = Vec::<String>::new();
	let mut file_contents = Vec::<Vec<u8>>::new();
	let mut chunk_size: usize = 0;
	let mut chunk_count = 1;
	let mut file_count = 0;
	for i in 0..archive.len() {
		let file = archive.by_index(i)?;
		if file.is_dir() {
			continue;
		}
		file_count += 1;
		let file_name = file.name();
		println!("{}", file_name);
		chunk_size += file_name.as_bytes().len();
		let file_body =
			file.bytes().collect::<Result<Vec<u8>, std::io::Error>>()?;
		chunk_size += file_body.len();

		// write to parquet file and start a new chunk
		if chunk_size >= 512 * 1024 * 1024 || i == archive.len() - 1 {
			println!("Writing chunk {} with {} files...", chunk_count, file_count);
			let file_names_column =
				StringArray::from(file_names.drain(0..).collect::<Vec<String>>());
			let file_contents_column = BinaryArray::from(
				file_contents
					.iter()
					.map(|v| v.as_ref())
					.collect::<Vec<&[u8]>>(),
			);
			let batch = RecordBatch::try_from_iter(vec![
				("name", Arc::new(file_names_column) as ArrayRef),
				("body", Arc::new(file_contents_column) as ArrayRef),
			])?;
			writer.write(&batch)?;
			file_names.clear();
			file_contents.clear();
			chunk_size = 0;
			chunk_count += 1;
			file_count = 0;
		}
	}

	writer.close()?;

	Ok(())
}
