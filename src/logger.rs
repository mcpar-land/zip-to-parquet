use atty::Stream;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::Args;

pub enum Logger {
	Tty {
		overall_bar: Option<(MultiProgress, ProgressBar)>,
		current_bar: ProgressBar,
	},
	Simple {
		overall_progress: Option<(u64, u64)>,
		current_progress: u64,
		current_max: u64,
	},
}

impl Logger {
	pub fn new(args: &Args, n_sources: u64) -> Self {
		if args.simple || !atty::is(Stream::Stderr) {
			return Logger::Simple {
				overall_progress: if n_sources > 1 {
					Some((0, n_sources as u64))
				} else {
					None
				},
				current_progress: 0,
				current_max: 0,
			};
		}

		let progress_chars = "█▉▊▋▌▍▎▏  ";
		// let progress_chars = "█▓▒░  ";

		let current_bar = ProgressBar::new(0);

		current_bar.set_style(ProgressStyle::with_template(
			"{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({per_sec})"
		).unwrap().progress_chars(progress_chars));

		let (overall_bar, current_bar) = match n_sources {
			0..=1 => (None, current_bar),
			n_sources => {
				let mp = MultiProgress::new();
				let overall_bar = ProgressBar::new(n_sources as u64);
				overall_bar.set_style(
			ProgressStyle::with_template(
				"{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({eta})",
			)
			.unwrap()
			.progress_chars(progress_chars),
		);
				let overall_bar = mp.add(overall_bar);
				overall_bar.set_position(0);
				let bar = mp.add(current_bar);
				(Some((mp, overall_bar)), bar)
			}
		};

		Logger::Tty {
			overall_bar,
			current_bar,
		}
	}

	pub fn next_overall(&mut self, label: String, new_current_max: u64) {
		self.println(format!("Starting {}", label));
		match self {
			Logger::Tty {
				overall_bar,
				current_bar,
			} => {
				if let Some((_, overall_bar)) = overall_bar {
					overall_bar.inc(1);
				}
				current_bar.set_length(new_current_max);
				current_bar.set_position(0);
			}
			Logger::Simple {
				overall_progress,
				current_progress,
				current_max,
			} => {
				if let Some((current, _)) = overall_progress {
					*current = *current + 1;
				}
				*current_progress = 0;
				*current_max = new_current_max;
			}
		}
	}

	pub fn inc(&mut self, delta: u64, current_file: &str) {
		match self {
			Logger::Tty { current_bar, .. } => current_bar.inc(delta),
			Logger::Simple {
				current_progress,
				current_max,
				..
			} => {
				let old_progress = *current_progress;
				*current_progress = old_progress + delta;
				let old_pct =
					(old_progress as f64 / *current_max as f64 * 100.0).floor() as usize;
				let new_pct = (*current_progress as f64 / *current_max as f64 * 100.0)
					.floor() as usize;
				if new_pct > old_pct {
					eprintln!(
						"{}% ({}/{}) {}",
						new_pct, *current_progress, *current_max, current_file
					);
				}
			}
		}
	}

	pub fn println(&self, msg: String) {
		match self {
			Logger::Tty {
				current_bar,
				overall_bar,
			} => match overall_bar {
				Some((mp, _)) => {
					_ = mp.println(msg);
				}
				None => {
					current_bar.println(msg);
				}
			},
			Logger::Simple { .. } => {
				eprintln!("{}", msg);
			}
		}
	}

	pub fn abandon(&mut self) {
		match self {
			Logger::Tty {
				overall_bar,
				current_bar,
			} => {
				if let Some((_, overall_bar)) = overall_bar {
					overall_bar.abandon();
				}
				current_bar.abandon();
			}
			Logger::Simple { .. } => {}
		}
	}

	pub fn finish(&mut self) {
		match self {
			Logger::Tty {
				overall_bar,
				current_bar,
			} => {
				if let Some((_, overall_bar)) = overall_bar {
					overall_bar.finish();
				}
				current_bar.finish_and_clear();
			}
			Logger::Simple { .. } => {}
		}
	}
}
