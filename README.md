# InsightFlow

### usage (json input)
```json
[
  {
    "op": "select",
    "mode": null,
    "column": "name|idx",
    "alias": "new_name|new_idx"
  },
  {
    "op": "filter",
    "mode": "equal",
    "column": "idx",
    "value": "3"
  },
  {
    "op": "filter",
    "mode": "is_null",
    "column": "idx"
  }
]
```