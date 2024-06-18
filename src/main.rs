use clap::Parser;
use error::Error;
use std::{
	io::BufWriter,
	path::PathBuf,
	sync::{atomic::AtomicBool, Arc},
};

pub mod builder;
pub mod error;
pub mod logger;

/// Convert .zip file to parquet of all files inside
#[derive(Parser, Debug, Clone)]
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
	/// specify row group size
	#[arg(long, default_value = "100")]
	row_group_size: usize,
}

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

	crate::builder::run(&args, terminated)?;

	Ok(())
}

// Use BufWriters to handle writing to files larger than memory.
pub enum FileOrStdout {
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
