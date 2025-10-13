# Copyright 2025 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import argparse
import uuid


def convert_notebook(input_filename, output_filename):
    """
    Reads a Jupyter notebook, converts SQL cells to Python cells with snowsql magic,
    and writes to a new notebook file.
    """
    try:
        with open(input_filename, "r", encoding="utf-8") as f:
            notebook = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_filename}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {input_filename}")
        return

    # Create new cells to be added
    new_cells_at_beginning = [
        {
            "cell_type": "markdown",
            "id": str(uuid.uuid4()),
            "source": "# Prepare python environment\nCreate a python virtual environment and install `ipykernel` package before running the notebook",
        },
        {
            "cell_type": "code",
            "id": str(uuid.uuid4()),
            "metadata": {"language": "python"},
            "source": "%%capture pip_install_output\n%pip install streamlit ipython",
        },
        {
            "cell_type": "markdown",
            "id": str(uuid.uuid4()),
            "source": "Install snowflake sql magics extension and configure it to connect to your Snowflake account",
        },
        {
            "cell_type": "code",
            "id": str(uuid.uuid4()),
            "metadata": {"language": "python"},
            "source": "\n".join(
                [
                    "%reload_ext snowflake_sql_magics",
                    "SNOWFLAKE_ACCOUNT = '<YOUR SNOWFLAKE ACCOUNT NAME>'",
                    "SNOWFLAKE_USER = '<YOUR SNOWFLAKE USER NAME>'",
                    "SNOWFLAKE_ROLE = '<SNOWFLAKE ROLE YOU WANT TO USE>'",
                    "SNOWFLAKE_PRIVATE_KEY_PATH = '<PATH TO YOUR PRIVATE KEY FILE FOR KEY-PAIR AUTHENTICATION>'",
                    "%snowauth --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --private_key_path $SNOWFLAKE_PRIVATE_KEY_PATH",
                ]
            ),
        },
        {
            "cell_type": "markdown",
            "id": str(uuid.uuid4()),
            "source": "## Test the connection",
        },
        {
            "cell_type": "code",
            "id": str(uuid.uuid4()),
            "metadata": {"language": "python"},
            "source": "\n".join(
                [
                    "%%snowsql test_connection",
                    "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION();",
                ]
            ),
        },
    ]

    # Prepend new cells
    notebook["cells"] = new_cells_at_beginning + notebook.setdefault("cells", [])

    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            metadata = cell.get("metadata", {})
            if metadata.get("language") == "sql":
                # Change language to python
                metadata["language"] = "python"

                # Prepend snowsql magic command
                source = cell.get("source", [])
                if isinstance(source, str):
                    source = "%%snowsql\n" + source
                elif isinstance(source, list):
                    source.insert(0, "%%snowsql\n")

                cell["source"] = source

    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(notebook, f, indent=2)
        print(f"Successfully converted {input_filename} to {output_filename}")
    except IOError as e:
        print(f"Error writing to output file {output_filename}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert Snowflake SQL notebook cells to snowsql magic cells."
    )
    parser.add_argument("input_file", help="The input Jupyter notebook file.")
    parser.add_argument(
        "output_file", help="The name for the output Jupyter notebook file."
    )
    args = parser.parse_args()

    convert_notebook(args.input_file, args.output_file)
