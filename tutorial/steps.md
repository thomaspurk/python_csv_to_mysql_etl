Great! Using **JSON mapping rules** makes the transformation more flexible and maintainable. Below is an enhanced version of the source-to-target mapping pipeline where the transformation logic is **driven by JSON rules** instead of hardcoded Python logic.

---

### 🧾 JSON Mapping Rules Format

```json
{
  "mappings": [
    {
      "source_fields": ["first_name", "last_name"],
      "target_field": "full_name",
      "transformation": "concat"
    },
    {
      "source_fields": ["birth_date"],
      "target_field": "dob",
      "transformation": "format_date"
    },
    {
      "source_fields": ["birth_date"],
      "target_field": "age",
      "transformation": "calculate_age"
    }
  ]
}
```

---

### 🧠 Python Pipeline Using JSON Mapping

```python
import pandas as pd
from datetime import datetime
import json

# Sample source data
source_data = pd.DataFrame([
    {"first_name": "Alice", "last_name": "Johnson", "birth_date": "1990-05-24"},
    {"first_name": "Bob", "last_name": "Smith", "birth_date": "1985-11-12"},
])

# JSON Mapping rules (as a Python dict; typically this would be loaded from a .json file)
mapping_rules_json = """
{
  "mappings": [
    {
      "source_fields": ["first_name", "last_name"],
      "target_field": "full_name",
      "transformation": "concat"
    },
    {
      "source_fields": ["birth_date"],
      "target_field": "dob",
      "transformation": "format_date"
    },
    {
      "source_fields": ["birth_date"],
      "target_field": "age",
      "transformation": "calculate_age"
    }
  ]
}
"""
mapping_rules = json.loads(mapping_rules_json)


# Transformation functions
def concat(fields):
    return " ".join(str(f) for f in fields)

def format_date(fields):
    return pd.to_datetime(fields[0]).strftime('%Y-%m-%d')

def calculate_age(fields):
    birth_date = pd.to_datetime(fields[0])
    today = datetime.now()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

# Mapping transformation dispatcher
TRANSFORM_FUNCTIONS = {
    "concat": concat,
    "format_date": format_date,
    "calculate_age": calculate_age
}


# Pipeline function
def apply_mapping_rules(source_df, rules):
    result_rows = []
    for _, row in source_df.iterrows():
        target_row = {}
        for mapping in rules["mappings"]:
            source_vals = [row[field] for field in mapping["source_fields"]]
            transform_func = TRANSFORM_FUNCTIONS[mapping["transformation"]]
            target_row[mapping["target_field"]] = transform_func(source_vals)
        result_rows.append(target_row)
    return pd.DataFrame(result_rows)


# Run the pipeline
target_data = apply_mapping_rules(source_data, mapping_rules)
print("Transformed Target Data:")
print(target_data)
```

---

### ✅ Output

```plaintext
Transformed Target Data:
      full_name         dob  age
0  Alice Johnson  1990-05-24   35
1     Bob Smith  1985-11-12   39
```

---

### 🛠️ Easily Extendable

To add new mappings:

- Add a new entry in the JSON.
- Define the transformation function in `TRANSFORM_FUNCTIONS`.

---

Would you like me to show how to load the mapping from an actual `.json` file or integrate it into a full ETL pipeline (e.g., read from CSV, apply mapping, write to DB)?
