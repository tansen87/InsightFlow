use std::{
  fs::File,
  io::BufWriter,
  path::{Path, PathBuf},
  time::Instant,
};

use anyhow::Result;
use csv::{ByteRecord, ReaderBuilder, WriterBuilder};
use pinyin::ToPinyin;

use crate::io::csv::{options::CsvOptions, selection::Selection};

enum PinyinStyle {
  Upper,
  Lower,
  None,
}

pub async fn chinese_to_pinyin<P: AsRef<Path> + Send + Sync>(
  path: P,
  columns: String,
  mode: &str,
  pinyin_style: &str,
) -> Result<()> {
  let csv_options = CsvOptions::new(&path);
  let sep = csv_options.detect_separator()?;
  let parent_path = path.as_ref().parent().unwrap().to_str().unwrap();
  let file_stem = path.as_ref().file_stem().unwrap().to_str().unwrap();
  let mut output_path = PathBuf::from(parent_path);
  output_path.push(format!("{file_stem}.pinyin.csv"));

  let mut rdr = ReaderBuilder::new()
    .delimiter(sep)
    .from_reader(csv_options.rdr_skip_rows()?);

  let cols: Vec<&str> = columns.split('|').collect();
  let sel = Selection::from_headers(rdr.byte_headers()?, &cols[..])?;

  let buf_writer = BufWriter::with_capacity(256_000, File::create(output_path)?);
  let mut wtr = WriterBuilder::new().delimiter(sep).from_writer(buf_writer);

  wtr.write_record(rdr.headers()?)?;

  let style = match mode {
    "upper" => PinyinStyle::Upper,
    "lower" => PinyinStyle::Lower,
    _ => PinyinStyle::None,
  };

  let mut record = ByteRecord::new();
  while rdr.read_byte_record(&mut record)? {
    let mut new_record = Vec::new();
    for (i, field) in record.iter().enumerate() {
      let mut new_field = String::from_utf8_lossy(field).to_string();

      if sel.get_indices().contains(&i) {
        new_field = new_field
          .chars()
          .map(|c| {
            c.to_pinyin().map_or_else(
              || c.into(),
              |py| {
                let s = py.plain().to_string();
                match style {
                  PinyinStyle::Upper => s.to_uppercase(),
                  PinyinStyle::Lower => s.to_lowercase(),
                  PinyinStyle::None => s,
                }
              },
            )
          })
          .collect::<String>();
      }
      new_record.push(new_field);
    }
    wtr.write_record(&new_record)?;
    record.clear();
  }

  Ok(())
}
