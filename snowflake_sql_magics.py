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

import os
import shlex
import argparse
import traceback
import re
from argparse import ArgumentParser

import pandas as pd
import snowflake.connector
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
from IPython.display import display


@magics_class
class SnowflakeSqlMagics(Magics):
    def __init__(self, shell):
        super(SnowflakeSqlMagics, self).__init__(shell)
        self.conn: snowflake.connector.Connection = None
        self._initialized = False
        self.account = None
        self.user = None
        self.role = None
        self.private_key_path = None
        self.private_key_passphrase = None

    def _process_file_directives(self, sql_query: str) -> str:
        """
        Processes file directives like FILE('path/to/file') in the SQL query.
        """

        def replace_directive(match):
            file_path = match.group(2).strip()
            try:
                with open(file_path, "r") as f:
                    content = f.read()
                # Use Snowflake's dollar-quoted string constants to handle special characters
                return f"PARSE_JSON($${content}$$)"
            except FileNotFoundError:
                return f"/* FILE NOT FOUND: {file_path} */"
            except Exception as e:
                return f"/* ERROR READING FILE {file_path}: {e} */"

        # Regex to find FILE('path') or FILE("path")
        pattern = re.compile(r"FILE\((['\"])(.*?)\1\)")
        return pattern.sub(replace_directive, sql_query)

    def _create_connection(self):
        """
        Creates a connection to Snowflake using the snowflake-connector-python library

        Checks if a connection already exists and is closed. If so, it creates a new connection.
        """
        if not self.conn or self.conn.is_closed():
            self.conn = snowflake.connector.connect(
                account=self.account,
                authenticator="SNOWFLAKE_JWT",
                user=self.user,
                role=self.role,
                private_key_file=self.private_key_path,
                private_key_file_pwd=self.private_key_passphrase,
            )

    def _snowflake_sql_executor(self, sql_query: str) -> list:
        """
        Executes a Snowflake SQL query and returns a list of results.
        Each result can be a pandas DataFrame or a string message.
        """
        self._create_connection()

        # Split the SQL query into individual statements.
        # This regex handles semicolons inside strings.
        statements = re.split(r""";(?=(?:[^'"]|'[^']*'|"[^"]*")*$)""", sql_query)
        statements = [s.strip() for s in statements if s.strip()]

        results = []
        for statement in statements:
            cur = None
            try:
                cur = self.conn.cursor()
                cur.execute(statement)

                statement_type = statement.strip().split(maxsplit=1)[0].upper()

                if statement_type in ("INSERT", "UPDATE", "DELETE", "MERGE"):
                    message_map = {
                        "INSERT": "Number of rows inserted",
                        "UPDATE": "Number of rows updated",
                        "DELETE": "Number of rows deleted",
                        "MERGE": "Number of rows affected",
                    }
                    message = message_map.get(statement_type)
                    results.append(f"{message}: {cur.rowcount}")
                    cur.fetchall()  # Consume any potential result set
                elif cur.description:  # It's a query that returns rows
                    try:
                        df = cur.fetch_pandas_all()
                        if not df.empty:
                            results.append(df)
                        elif statement_type.startswith("SHOW"):
                            results.append(
                                "Show command executed successfully, but it did not produce any results to display."
                            )
                    except snowflake.connector.errors.NotSupportedError:
                        cur.fetchall()  # Consume the result to avoid issues.
                else:
                    results.append("Statement executed successfully.")
            finally:
                if cur:
                    cur.close()
        return results

    def _parse_args(self, line: str) -> argparse.Namespace:
        """
        Parses arguments from the magic line.
        """
        parser = ArgumentParser(description="Snowflake SQL cell magic")
        parser.add_argument("--account", help="Snowflake account")
        parser.add_argument("--user", help="Snowflake user")
        parser.add_argument("--role", help="Snowflake role")
        parser.add_argument("--private_key_path", help="Path to private key file")
        parser.add_argument(
            "--private_key_passphrase", help="Passphrase for private key"
        )

        args, _ = parser.parse_known_args(shlex.split(line))
        return args

    def _initialize_connection(self, args):
        print("Initializing new Snowflake connection...")
        self.account = args.account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = args.user or os.getenv("SNOWFLAKE_USER")
        self.role = args.role or os.getenv("SNOWFLAKE_ROLE")
        self.private_key_path = args.private_key_path or os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PATH"
        )
        self.private_key_passphrase = args.private_key_passphrase or os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )

        if not all([self.account, self.user, self.role, self.private_key_path]):
            print(
                "Error: Missing connection parameters for initialization. Provide them as arguments or environment variables."
            )
            return

        try:
            self._create_connection()
            self._initialized = True
            print("Snowflake connection successful.")
        except Exception as e:
            print(f"An error occurred in initialization: {e}")
            traceback.print_exc()
            self._initialized = False
            return

    @line_magic
    def snowauth(self, line: str):
        """
        Line magic to authenticate and connect to Snowflake.

        Usage:
        %snowauth --account <account> --user <user> --role <role> --private_key_path <path> [--private_key_passphrase <passphrase>]

        Connection arguments can also be provided via environment variables:
        SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
        """
        args = self._parse_args(line)
        self._initialize_connection(args)

    @cell_magic
    def snowsql(self, line, cell):
        """
        Cell magic to execute Snowflake SQL.
        Usage:
        %%snowsql [variable_name]
        <SQL QUERY>

        You must first establish a connection using the %snowauth magic.
        """
        var_name = line.strip()

        if not self._initialized:
            print(
                "Error: Snowflake connection not initialized. Please run %snowauth first."
            )
            return

        try:
            processed_cell = self._process_file_directives(cell)
            results = self._snowflake_sql_executor(processed_cell)

            dataframe_results = [
                res for res in results if isinstance(res, pd.DataFrame)
            ]

            if var_name:
                if len(dataframe_results) > 1:
                    print(
                        f"Warning: Multiple DataFrames returned, only the last one will be assigned to '{var_name}'."
                    )

                if dataframe_results:
                    self.shell.user_ns[var_name] = dataframe_results[-1]
                    print(f"Result stored in DataFrame '{var_name}'.")
                else:
                    print(f"Warning: No DataFrame returned to assign to '{var_name}'.")

            if results:
                for res in results:
                    if isinstance(res, pd.DataFrame):
                        display(res)
                    else:
                        print(res)
            else:
                print(
                    "Query executed successfully, but it did not produce any results to display."
                )

        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()


def load_ipython_extension(ipython):
    """
    Any module file that defines a function named `load_ipython_extension`
    can be loaded as an IPython extension. This function is called when the
    extension is loaded. It is given the IPython InteractiveShell instance.
    """
    ipython.register_magics(SnowflakeSqlMagics)
