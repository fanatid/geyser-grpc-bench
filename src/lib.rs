use {
    indicatif::{ProgressBar, ProgressStyle},
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Deserialize, Serialize)]
pub struct QuicStreamRequest {
    pub streams: u32,
    pub max_backlog: u32,
}

fn format_thousands(value: u64) -> String {
    value
        .to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .expect("invalid number")
        .join(",")
}

#[derive(Debug)]
pub struct BenchProgressBar {
    pb: ProgressBar,
    counter: u64,
    slot: u64,
}

impl Default for BenchProgressBar {
    fn default() -> Self {
        let pb = ProgressBar::no_length();
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner} received messages: {msg} / ~{bytes} in {elapsed_precise}",
            )
            .expect("valid template"),
        );
        Self {
            pb,
            counter: 0,
            slot: 0,
        }
    }
}

impl BenchProgressBar {
    pub fn set_slot(&mut self, slot: u64) {
        self.slot = slot;
    }

    pub fn inc(&mut self, size: usize) {
        self.counter += 1;
        self.pb.set_message(format!(
            "{} / slot {}",
            format_thousands(self.counter),
            self.slot
        ));
        self.pb.inc(size as u64);
    }
}
