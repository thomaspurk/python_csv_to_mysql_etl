# python_csv_to_mysql_etl

Using Pandas and a JSON mapping file, transform a flat CSV into a MySQL RDBMS.

## Development Environment

|               |                        |
| ------------- | ---------------------- |
| Platform      | OS MacOS Sonoma 14.7.4 |
| IDE           | VS Code 1.100.2        |
| Runtime       | Python 3.13.1          |
| Documentation | Markdown / DocString   |
| Unit Testing  | PyTest                 |
| Repository    | GitHub.com             |

### Setup

1. Clone the GitHub repository

2. Create a virtual Python Environment.
   <br>NOTE: The .venv folder is listed in the .gitignore file.

```shell
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Create a .env.development.local file. See the file template.env.development.local for required variables.
   <br>NOTE: The .env.development.local file is listed in the .gitignore file.

### Data

https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
