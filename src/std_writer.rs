use std::fs::File;
use std::io::{BufWriter, Write};

pub struct StdWriter {
    writer: Option<BufWriter<File>>,
}

impl StdWriter {
    pub fn new(filename: Option<String>) -> Self {
        if let Some(filename) = filename {
            let writer = Some(BufWriter::new(
                File::create(filename).expect("Unable to create file"),
            ));
            StdWriter { writer }
        } else {
            StdWriter { writer: None }
        }
    }

    pub fn print(&mut self, buf: &str) {
        if let Some(writer) = self.writer.as_mut() {
            writer.write(buf.as_bytes()).unwrap();
        } else {
            print!("{}", &buf);
        }
    }

    pub fn println(&mut self, buf: &str) {
        if let Some(writer) = self.writer.as_mut() {
            writer.write(buf.as_bytes()).unwrap();
            writer.write("\n".as_bytes()).unwrap();
        } else {
            println!("{}", &buf);
        }
    }

    pub fn flush(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().unwrap();
        }
    }
}
