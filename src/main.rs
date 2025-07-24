use std::{fs::File, io::Read, path::Path};

use anyhow::{Result, anyhow};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use pinyin::ToPinyin;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Operation {
  op: String,
  mode: Option<String>,
  column: Option<String>,
  value: Option<String>,
  offset: Option<usize>,
  length: Option<usize>,
  comparand: Option<String>,
  replacement: Option<String>,
  alias: Option<String>,
}

struct SliceOperation {
  column: String,
  mode: String,
  offset: usize,
  length: usize,
  alias: Option<String>,
}

struct StringOperation {
  column: String,
  mode: String,
  comparand: Option<String>,
  replacement: Option<String>,
  alias: Option<String>,
}

struct ProcessingContext {
  select: Option<Vec<usize>>,
  alias: Option<Vec<Option<String>>>,
  filters: Vec<Box<dyn Fn(&StringRecord) -> bool + Send + Sync>>,
  slice_ops: Vec<SliceOperation>,
  string_ops: Vec<StringOperation>,
}

impl ProcessingContext {
  fn new() -> Self {
    ProcessingContext {
      select: None,
      alias: None,
      filters: Vec::new(),
      slice_ops: Vec::new(),
      string_ops: Vec::new(),
    }
  }

  fn add_select(&mut self, columns: &[&str], header: &[String]) {
    let selected_indices: Vec<usize> = columns
      .iter()
      .filter_map(|col| header.iter().position(|h| h == *col))
      .collect();

    self.select = Some(selected_indices);
  }

  fn add_filter<F>(&mut self, filter: F)
  where
    F: Fn(&StringRecord) -> bool + Send + Sync + 'static,
  {
    self.filters.push(Box::new(filter));
  }

  fn add_slice(
    &mut self,
    column: &str,
    mode: &str,
    offset: usize,
    length: usize,
    alias: Option<String>,
  ) {
    self.slice_ops.push(SliceOperation {
      column: column.to_string(),
      mode: mode.to_string(),
      offset,
      length,
      alias,
    });
  }

  fn add_string(
    &mut self,
    column: &str,
    mode: &str,
    comparand: Option<String>,
    replacement: Option<String>,
    alias: Option<String>,
  ) {
    self.string_ops.push(StringOperation {
      column: column.to_string(),
      mode: mode.to_string(),
      comparand,
      replacement,
      alias,
    });
  }

  // And
  // fn is_valid(&self, record: &StringRecord) -> bool {
  //     self.filters.iter().all(|f| f(record))
  // }

  // Or
  fn is_valid(&self, record: &StringRecord) -> bool {
    self.filters.is_empty() || self.filters.iter().any(|f| f(record))
  }
}

fn equal_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let value = value.to_string();
  Ok(move |record: &StringRecord| record.get(idx).map_or(false, |field| field == value))
}

fn not_equal_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let value = value.to_string();
  Ok(move |record: &StringRecord| record.get(idx).map_or(true, |field| field != value))
}

fn contains_filter(
  column: &str,
  substring: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let column_idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let substring = substring.to_string();
  Ok(move |record: &StringRecord| {
    record
      .get(column_idx)
      .map_or(false, |field| field.contains(&substring))
  })
}

fn not_contains_filter(
  column: &str,
  substring: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let substring = substring.to_string();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(true, |field| !field.contains(&substring))
  })
}

fn starts_with_filter(
  column: &str,
  prefix: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let prefix = prefix.to_string();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(false, |field| field.starts_with(&prefix))
  })
}

fn not_starts_with_filter(
  column: &str,
  prefix: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let prefix = prefix.to_string();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(true, |field| !field.starts_with(&prefix))
  })
}

fn ends_with_filter(
  column: &str,
  suffix: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let suffix = suffix.to_string();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(false, |field| field.ends_with(&suffix))
  })
}

fn not_ends_with_filter(
  column: &str,
  suffix: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let suffix = suffix.to_string();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(true, |field| !field.ends_with(&suffix))
  })
}

fn is_null_filter(
  column: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(true, |field| field.trim().is_empty())
  })
}

fn is_not_null_filter(
  column: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(false, |field| !field.trim().is_empty())
  })
}

fn gt_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let value = value
    .parse::<f64>()
    .map_err(|e| anyhow!("filter value 不是有效数字: {}", e))?;
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .and_then(|field| field.parse::<f64>().ok())
      .map_or(false, |v| v > value)
  })
}

fn ge_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let value = value
    .parse::<f64>()
    .map_err(|e| anyhow!("filter value 不是有效数字: {}", e))?;
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .and_then(|field| field.parse::<f64>().ok())
      .map_or(false, |v| v >= value)
  })
}

fn lt_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let value = value
    .parse::<f64>()
    .map_err(|e| anyhow!("filter value 不是有效数字: {}", e))?;
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .and_then(|field| field.parse::<f64>().ok())
      .map_or(false, |v| v < value)
  })
}

fn le_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let value = value
    .parse::<f64>()
    .map_err(|e| anyhow!("filter value 不是有效数字: {}", e))?;
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .and_then(|field| field.parse::<f64>().ok())
      .map_or(false, |v| v <= value)
  })
}

fn between_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let parts: Vec<&str> = value.split('|').collect();
  let min = parts
    .get(0)
    .and_then(|s| s.parse::<f64>().ok())
    .unwrap_or(f64::MIN);
  let max = parts
    .get(1)
    .and_then(|s| s.parse::<f64>().ok())
    .unwrap_or(f64::MAX);
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .and_then(|field| field.parse::<f64>().ok())
      .map_or(false, |v| v >= min && v <= max)
  })
}

fn process_operations(
  input_path: &Path,
  operations: &[Operation],
  output_path: &Path,
) -> Result<()> {
  let file = File::open(input_path)?;
  let mut reader = ReaderBuilder::new().from_reader(file);
  let headers = reader
    .headers()?
    .clone()
    .iter()
    .map(|s| s.to_string())
    .collect::<Vec<_>>();

  let mut context = ProcessingContext::new();

  // 预处理阶段：解析所有操作
  for op in operations {
    match op.op.as_str() {
      "select" => {
        if let Some(column) = &op.column {
          let columns: Vec<&str> = column.split('|').collect();
          context.add_select(&columns, &headers);
          let aliases = if let Some(alias) = &op.alias {
            alias
              .split(|c| c == '|')
              .map(|s| Some(s.to_string()))
              .collect()
          } else {
            vec![None; columns.len()]
          };
          context.alias = Some(aliases);
        }
      }
      "filter" => {
        if let (Some(col), Some(mode), Some(val)) = (&op.column, &op.mode, &op.value) {
          match mode.as_str() {
            "equal" => context.add_filter(equal_filter(col, val, &headers)?),
            "not_equal" => context.add_filter(not_equal_filter(col, val, &headers)?),
            "contains" => context.add_filter(contains_filter(col, val, &headers)?),
            "not_contains" => context.add_filter(not_contains_filter(col, val, &headers)?),
            "starts_with" => context.add_filter(starts_with_filter(col, val, &headers)?),
            "not_starts_with" => context.add_filter(not_starts_with_filter(col, val, &headers)?),
            "ends_with" => context.add_filter(ends_with_filter(col, val, &headers)?),
            "not_ends_with" => context.add_filter(not_ends_with_filter(col, val, &headers)?),
            "gt" => context.add_filter(gt_filter(col, val, &headers)?),
            "ge" => context.add_filter(ge_filter(col, val, &headers)?),
            "lt" => context.add_filter(lt_filter(col, val, &headers)?),
            "le" => context.add_filter(le_filter(col, val, &headers)?),
            "between" => context.add_filter(between_filter(col, val, &headers)?),
            _ => return Err(anyhow!("not support filter mode: {}", mode)),
          }
        } else if let (Some(col), Some(mode)) = (&op.column, &op.mode) {
          match mode.as_str() {
            "is_null" => context.add_filter(is_null_filter(col, &headers)?),
            "is_not_null" => context.add_filter(is_not_null_filter(col, &headers)?),
            _ => return Err(anyhow!("not support filter mode: {}", mode)),
          }
        }
      }
      "slice" => {
        if let (Some(col), Some(mode)) = (&op.column, &op.mode) {
          let offset = op.offset.unwrap_or(1);
          let length = op.length.unwrap_or(1);
          context.add_slice(col, mode, offset, length, op.alias.clone());
        }
      }
      "string" => {
        if let (Some(col), Some(mode)) = (&op.column, &op.mode) {
          context.add_string(
            col,
            mode,
            op.comparand.clone(),
            op.replacement.clone(),
            op.alias.clone(),
          );
        }
      }
      _ => return Err(anyhow!("not support operation: {}", op.op)),
    }
  }

  // 创建输出文件和写入器
  let output_file = File::create(output_path)?;
  let mut writer = WriterBuilder::new().from_writer(output_file);

  // 写入处理后的头部
  if let Some(ref selected) = context.select {
    let mut selected_headers: Vec<String> = Vec::new();
    if let Some(ref aliases) = context.alias {
      for (i, &idx) in selected.iter().enumerate() {
        if let Some(Some(alias)) = aliases.get(i) {
          selected_headers.push(alias.clone());
        } else {
          selected_headers.push(headers[idx].clone());
        }
      }
    } else {
      for &idx in selected {
        selected_headers.push(headers[idx].clone());
      }
    }

    // 加入所有 slice 新字段名
    for slice_op in &context.slice_ops {
      let slice_name = if let Some(ref alias) = slice_op.alias {
        alias.clone()
      } else {
        format!("{}_{}", slice_op.column, slice_op.mode)
      };
      selected_headers.push(slice_name);
    }

    // 加入所有 string 新字段名
    for string_op in &context.string_ops {
      let string_name = if let Some(ref alias) = string_op.alias {
        alias.clone()
      } else {
        format!("{}_{}", string_op.column, string_op.mode)
      };
      selected_headers.push(string_name);
    }

    writer.write_record(&selected_headers)?;
  } else {
    let mut all_headers: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    for slice_op in &context.slice_ops {
      let slice_name = if let Some(ref alias) = slice_op.alias {
        alias.clone()
      } else {
        format!("{}_{}", slice_op.column, slice_op.mode)
      };
      all_headers.push(slice_name);
    }

    for string_op in &context.string_ops {
      let string_name = if let Some(ref alias) = string_op.alias {
        alias.clone()
      } else {
        format!("{}_{}", string_op.column, string_op.mode)
      };
      all_headers.push(string_name);
    }

    writer.write_record(all_headers.iter().map(|s| s.as_str()))?;
  }

  // 流式处理所有记录
  for result in reader.records() {
    let record = result?;

    // 先收集所有 slice 结果
    let mut slice_results = Vec::new();
    for slice_op in &context.slice_ops {
      let idx = headers.iter().position(|h| h == &slice_op.column);
      if let Some(idx) = idx {
        if let Some(val) = record.get(idx) {
          let new_val = match slice_op.mode.as_str() {
            "left" => val.chars().take(slice_op.offset).collect(),
            "right" => val
              .chars()
              .rev()
              .take(slice_op.offset)
              .collect::<String>()
              .chars()
              .rev()
              .collect(),
            "slice" => {
              let start = slice_op.offset as isize - 1;
              let end = start + slice_op.length as isize;
              val
                .chars()
                .skip(start.max(0) as usize)
                .take((end - start) as usize)
                .collect()
            }
            _ => val.to_string(),
          };
          slice_results.push(new_val);
        } else {
          slice_results.push(String::new());
        }
      } else {
        slice_results.push(String::new());
      }
    }

    let mut string_results = Vec::new();
    for string_op in &context.string_ops {
      if let Some(idx) = headers.iter().position(|h| h == &string_op.column) {
        let cell = record.get(idx).unwrap_or("").to_string();
        let new_val = match string_op.mode.as_str() {
          "pinyin" => {
            let py_mode_string = string_op.replacement.clone().unwrap_or("none".to_owned());
            let py_mode = py_mode_string.as_str();
            cell
              .chars()
              .map(|c| {
                c.to_pinyin().map_or_else(
                  || c.to_string(),
                  |py| match py_mode {
                    "upper" => py.plain().to_uppercase(),
                    "lower" => py.plain().to_lowercase(),
                    _ => py.plain().to_string(),
                  },
                )
              })
              .collect()
          }
          "lower" => cell.to_lowercase(),
          "upper" => cell.to_uppercase(),
          "trim" => cell.trim().to_string(),
          "ltrim" => cell.trim_start().to_string(),
          "rtrim" => cell.trim_end().to_string(),
          "replace" => {
            let comparand = string_op.comparand.as_deref().unwrap_or("");
            let replacement = string_op.replacement.as_deref().unwrap_or("");
            cell.replace(comparand, replacement)
          }
          "len" => cell.chars().count().to_string(),
          "round" => {
            if let Ok(num) = cell.parse::<f64>() {
              format!("{:.2}", num)
            } else {
              cell
            }
          }
          "squeeze" => {
            let re = regex::Regex::new(r"\s+").unwrap();
            re.replace_all(&cell, " ").into_owned()
          }
          "strip" => {
            let re = regex::Regex::new(r"[\r\n]+").unwrap();
            re.replace_all(&cell, " ").into_owned()
          }
          "reverse" => cell.chars().rev().collect(),
          "abs" => {
            if let Ok(num) = cell.parse::<f64>() {
              num.abs().to_string()
            } else {
              cell
            }
          }
          "neg" => {
            if let Ok(num) = cell.parse::<f64>() {
              (-num).to_string()
            } else {
              cell
            }
          }
          "copy" => cell.clone(),
          _ => cell,
        };
        string_results.push(new_val);
      } else {
        string_results.push(String::new());
      }
    }

    if context.is_valid(&record) {
      if let Some(selected) = &context.select {
        let mut filtered: Vec<_> = selected
          .iter()
          .map(|&idx| record.get(idx).unwrap_or(""))
          .collect();
        filtered.extend(slice_results.iter().map(|s| s.as_str()));
        filtered.extend(string_results.iter().map(|s| s.as_str()));
        writer.write_record(&filtered)?;
      } else {
        let mut all_fields: Vec<_> = record.iter().map(|s| s).collect();
        all_fields.extend(slice_results.iter().map(|s| s.as_str()));
        all_fields.extend(string_results.iter().map(|s| s.as_str()));
        writer.write_record(&all_fields)?;
      }
    }
  }

  Ok(())
}

fn main() -> Result<()> {
  // let operations = vec![
  //     Operation {
  //         op: "select".to_string(),
  //         column: Some("name,idx".to_string()),
  //         value: None,
  //     },
  //     Operation {
  //         op: "contains".to_string(),
  //         column: Some("name".to_string()),
  //         value: Some("to".to_string()),
  //     },
  // ];
  let mut file = File::open("cmd.json")?;
  let mut json_str = String::new();
  file.read_to_string(&mut json_str)?;

  let operations: Vec<Operation> = serde_json::from_str(&json_str)?;
  // let input_path = Path::new("E:/Desktop/test_data/insightSQL_data/bigfile/cat.csv");
  let input_path = Path::new("tsearch.csv");
  let output_path = Path::new("tsearch.output.csv");

  process_operations(input_path, &operations, output_path)?;

  Ok(())
}
