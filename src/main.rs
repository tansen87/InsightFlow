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
  offset: Option<String>,
  length: Option<String>,
  comparand: Option<String>,
  replacement: Option<String>,
  alias: Option<String>,
}

struct SliceOperation {
  column: String,
  mode: String,
  offset: String,
  length: String,
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
    offset: String,
    length: String,
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
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let values: Vec<String> = value.split('|').map(|s| s.to_string()).collect();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(false, |field| values.iter().any(|val| field.contains(val)))
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

fn is_in_filter(
  column: &str,
  value: &str,
  headers: &[String],
) -> Result<impl Fn(&StringRecord) -> bool + use<>> {
  let idx = headers
    .iter()
    .position(|h| h == column)
    .ok_or_else(|| anyhow!("Column not found: {}", column))?;
  let values: Vec<String> = value.split('|').map(|s| s.to_string()).collect();
  Ok(move |record: &StringRecord| {
    record
      .get(idx)
      .map_or(false, |field| values.contains(&field.to_string()))
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
            "is_null" => context.add_filter(is_null_filter(col, &headers)?),
            "is_not_null" => context.add_filter(is_not_null_filter(col, &headers)?),
            "is_in" => context.add_filter(is_in_filter(col, val, &headers)?),
            _ => return Err(anyhow!("not support filter mode: {}", mode)),
          }
        }
      }
      "slice" => {
        if let (Some(col), Some(mode)) = (&op.column, &op.mode) {
          let offset = op.offset.clone().ok_or(anyhow!("offset is required"))?;
          let length = op.length.clone().ok_or(anyhow!("length is required"))?;
          context.add_slice(col, mode, offset, length, op.alias.clone());
        }
      }
      "str" => {
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

    // 收集所有 slice 和 string 新字段名及其值的生成方式
    let mut new_field_names = Vec::new();
    let mut new_field_is_slice = Vec::new(); // true=slice, false=string
    let mut new_field_aliases = Vec::new();
    for slice_op in &context.slice_ops {
      let slice_name = if let Some(ref alias) = slice_op.alias {
        alias.clone()
      } else {
        format!("{}_{}", slice_op.column, slice_op.mode)
      };
      new_field_names.push(slice_name.clone());
      new_field_is_slice.push(true);
      new_field_aliases.push(slice_name);
    }
    for string_op in &context.string_ops {
      // 这些操作不新增列，而是就地修改
      if string_op.mode == "fill"
        || string_op.mode == "f_fill"
        || string_op.mode == "lower"
        || string_op.mode == "upper"
        || string_op.mode == "trim"
        || string_op.mode == "ltrim"
        || string_op.mode == "rtrim"
        || string_op.mode == "squeeze"
        || string_op.mode == "strip"
      {
        continue;
      }
      let string_name = if let Some(ref alias) = string_op.alias {
        alias.clone()
      } else {
        format!("{}_{}", string_op.column, string_op.mode)
      };
      new_field_names.push(string_name.clone());
      new_field_is_slice.push(false);
      new_field_aliases.push(string_name);
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

    // 只为非 fill/f_fill 的 string_ops 新增列
    for string_op in &context.string_ops {
      if string_op.mode == "fill"
        || string_op.mode == "f_fill"
        || string_op.mode == "lower"
        || string_op.mode == "upper"
        || string_op.mode == "trim"
        || string_op.mode == "ltrim"
        || string_op.mode == "rtrim"
        || string_op.mode == "squeeze"
        || string_op.mode == "strip"
      {
        continue;
      }
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
      if string_op.mode == "fill"
        || string_op.mode == "f_fill"
        || string_op.mode == "lower"
        || string_op.mode == "upper"
        || string_op.mode == "trim"
        || string_op.mode == "ltrim"
        || string_op.mode == "rtrim"
        || string_op.mode == "squeeze"
        || string_op.mode == "strip"
      {
        continue;
      }
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
  // 初始化 f_fill 缓存（每个 string_op 一个）
  let mut ffill_caches: Vec<Option<String>> = vec![None; context.string_ops.len()];

  for result in reader.records() {
    let record = result?;

    // 先收集所有 slice 结果
    let mut slice_results = Vec::new();
    for slice_op in &context.slice_ops {
      let idx = headers.iter().position(|h| h == &slice_op.column);
      let length = slice_op.length.parse::<usize>()?;
      if let Some(idx) = idx {
        if let Some(val) = record.get(idx) {
          let new_val = match slice_op.mode.as_str() {
            "left" => val.chars().take(length).collect(),
            "right" => val
              .chars()
              .rev()
              .take(length)
              .collect::<String>()
              .chars()
              .rev()
              .collect(),
            "slice" => {
              let offset = slice_op.offset.parse::<isize>()?;
              let start = offset - 1;
              let end = start + length as isize;
              val
                .chars()
                .skip(start.max(0) as usize)
                .take((end - start) as usize)
                .collect()
            }
            "split" => {
              let split_parts: Vec<&str> = val.split(&slice_op.offset).collect();
              if split_parts.len() >= length {
                split_parts[length - 1].to_string()
              } else {
                "".to_string()
              }
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

    // 先复制一份原始字段
    let mut row_fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
    let mut string_results = Vec::new();
    for (i, string_op) in context.string_ops.iter().enumerate() {
      if let Some(idx) = headers.iter().position(|h| h == &string_op.column) {
        let cell = row_fields[idx].clone();
        match string_op.mode.as_str() {
          "fill" => {
            if cell.is_empty() {
              row_fields[idx] = string_op.replacement.clone().unwrap_or_default();
            }
          }
          "f_fill" => {
            if cell.is_empty() {
              if let Some(ref cache_val) = ffill_caches[i] {
                row_fields[idx] = cache_val.clone();
              }
            } else {
              ffill_caches[i] = Some(cell.clone());
            }
          }
          // 这些操作就地修改，不新增列
          "lower" => row_fields[idx] = cell.to_lowercase(),
          "upper" => row_fields[idx] = cell.to_uppercase(),
          "trim" => row_fields[idx] = cell.trim().to_string(),
          "ltrim" => row_fields[idx] = cell.trim_start().to_string(),
          "rtrim" => row_fields[idx] = cell.trim_end().to_string(),
          "squeeze" => {
            let re = regex::Regex::new(r"\s+").unwrap();
            row_fields[idx] = re.replace_all(&cell, " ").into_owned();
          }
          "strip" => {
            let re = regex::Regex::new(r"[\r\n]+").unwrap();
            row_fields[idx] = re.replace_all(&cell, " ").into_owned();
          }
          // 其它 str 操作依然新增列
          "pinyin" => {
            let py_mode_string = string_op.replacement.clone().unwrap_or("none".to_owned());
            let py_mode = py_mode_string.as_str();
            let new_val = cell
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
              .collect();
            string_results.push(new_val);
          }
          "replace" => {
            let comparand = string_op.comparand.as_deref().unwrap_or("");
            let replacement = string_op.replacement.as_deref().unwrap_or("");
            string_results.push(cell.replace(comparand, replacement));
          }
          "regex_replace" => {
            let comparand = string_op.comparand.as_deref().unwrap_or("");
            let replacement = string_op.replacement.as_deref().unwrap_or("");
            let pattern = regex::RegexBuilder::new(comparand).build()?;
            string_results.push(pattern.replace_all(&cell, replacement).to_string());
          }
          "len" => string_results.push(cell.chars().count().to_string()),
          "round" => {
            if let Ok(num) = cell.parse::<f64>() {
              string_results.push(format!("{:.2}", num));
            } else {
              string_results.push(cell);
            }
          }
          "reverse" => string_results.push(cell.chars().rev().collect()),
          "abs" => {
            if let Ok(num) = cell.parse::<f64>() {
              string_results.push(num.abs().to_string());
            } else {
              string_results.push(cell);
            }
          }
          "neg" => {
            if let Ok(num) = cell.parse::<f64>() {
              string_results.push((-num).to_string());
            } else {
              string_results.push(cell);
            }
          }
          "copy" => string_results.push(cell.clone()),
          _ => string_results.push(cell),
        }
      } else {
        // 字段找不到时，只有新增列的操作才追加空字符串
        if string_op.mode != "fill"
          && string_op.mode != "f_fill"
          && string_op.mode != "lower"
          && string_op.mode != "upper"
          && string_op.mode != "trim"
          && string_op.mode != "ltrim"
          && string_op.mode != "rtrim"
          && string_op.mode != "squeeze"
          && string_op.mode != "strip"
        {
          string_results.push(String::new());
        }
      }
    }

    if context.is_valid(&record) {
      if let Some(selected) = &context.select {
        let mut filtered: Vec<_> = selected
          .iter()
          .map(|&idx| row_fields.get(idx).map(|s| s.as_str()).unwrap_or(""))
          .collect();
        filtered.extend(slice_results.iter().map(|s| s.as_str()));
        filtered.extend(string_results.iter().map(|s| s.as_str()));
        writer.write_record(&filtered)?;
      } else {
        let mut all_fields: Vec<_> = row_fields.iter().map(|s| s.as_str()).collect();
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
