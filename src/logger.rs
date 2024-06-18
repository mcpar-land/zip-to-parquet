use atty::Stream;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::Args;

#[derive(Clone)]
pub enum Logger {
	Bar(ProgressBar),
	Simple,
}

impl Logger {
	pub fn new(args: &Args, n_sources: u64) -> Self {
		if args.simple || !atty::is(Stream::Stderr) {
			return Logger::Simple;
		}

		let progress_chars = "█▉▊▋▌▍▎▏  ";
		// let progress_chars = "█▓▒░  ";

		let bar = ProgressBar::new(n_sources);

		bar.set_draw_target(ProgressDrawTarget::stderr());
		bar.set_style(ProgressStyle::with_template(
			"{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({per_sec})"
		).unwrap().progress_chars(progress_chars));

		Logger::Bar(bar)
	}

	pub fn inc(&self, delta: u64) {
		match self {
			Logger::Bar(bar) => bar.inc(delta),
			Logger::Simple => {}
		}
	}

	pub fn println(&self, msg: String) {
		match self {
			Logger::Bar(bar) => bar.println(msg),
			Logger::Simple => eprintln!("{}", msg),
		}
	}

	pub fn abandon(&self) {
		match self {
			Logger::Bar(bar) => bar.abandon(),
			Logger::Simple => {}
		}
	}

	pub fn finish(&self) {
		match self {
			Logger::Bar(bar) => bar.finish(),
			Logger::Simple => {}
		}
	}
}
