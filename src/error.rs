use std::{fmt::Display, path::PathBuf};

#[derive(Debug)]
pub enum Error {
	ReadFile {
		err: std::io::Error,
		file: PathBuf,
	},
	WriteFile {
		err: std::io::Error,
		target: PathBuf,
	},
	Zip {
		err: zip::result::ZipError,
		file: PathBuf,
	},
	ReadFileInZip {
		err: std::io::Error,
		file_name: String,
		file: PathBuf,
	},
	Parquet {
		err: parquet::errors::ParquetError,
	},
	Arrow {
		err: arrow_schema::ArrowError,
	},
	InvalidWaxGlob {
		glob: String,
		err: wax::BuildError,
	},
	InvalidGlob {
		glob: String,
		err: glob::PatternError,
	},
	NoInputsFound {
		globs: Vec<String>,
	},
	Glob {
		err: glob::GlobError,
	},
	NeedsOutputOrStdout,
	InvalidOutputAndStdout,
	Other(String),
}

impl std::error::Error for Error {}

impl Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::ReadFile { err, file } => write!(
				f,
				"error reading file {}: {}",
				file.as_os_str().to_string_lossy(),
				err,
			),
			Error::WriteFile { err, target } => write!(
				f,
				"error writing to destination {}: {}",
				target.as_os_str().to_string_lossy(),
				err,
			),
			Error::Zip { err, file } => write!(
				f,
				"error reading zip file {}: {}",
				file.as_os_str().to_string_lossy(),
				err
			),
			Error::ReadFileInZip {
				err,
				file_name,
				file,
			} => {
				write!(
					f,
					"error reading file {} from zip {}: {}",
					file_name,
					file.as_os_str().to_string_lossy(),
					err,
				)
			}
			Error::Parquet { err } => write!(f, "error writing to parquet: {}", err),
			Error::Arrow { err } => write!(f, "error forming arrow array: {}", err),
			Error::InvalidWaxGlob { glob, err } => {
				write!(f, "invalid glob '{}': {}", glob, err)
			}
			Error::InvalidGlob { glob, err } => {
				write!(f, "invalid glob '{}': {}", glob, err)
			}
			Error::NoInputsFound { globs } => {
				write!(f, "no files found for glob(s): {:?}", globs)
			}
			Error::Glob { err } => write!(f, "glob error: {}", err),
			Error::NeedsOutputOrStdout => {
				write!(f, "must provide an output file or --stdout")
			}
			Error::InvalidOutputAndStdout => {
				write!(f, "must provide an output file or --stdout, but not both")
			}
			Error::Other(err) => write!(f, "other error: {}", err),
		}
	}
}

impl From<parquet::errors::ParquetError> for Error {
	fn from(err: parquet::errors::ParquetError) -> Self {
		Self::Parquet { err }
	}
}

impl From<arrow_schema::ArrowError> for Error {
	fn from(err: arrow_schema::ArrowError) -> Self {
		Self::Arrow { err }
	}
}

impl From<glob::GlobError> for Error {
	fn from(err: glob::GlobError) -> Self {
		Self::Glob { err }
	}
}
