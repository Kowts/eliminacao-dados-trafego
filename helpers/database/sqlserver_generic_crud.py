import re
from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime
import unicodedata
from helpers.utils import retry

logger = logging.getLogger(__name__)

class SQLServerGenericCRUD:
    """Generic CRUD operations for any table in SQL Server."""

    def __init__(self, db_client):
        """
        Initialize the SQLServerGenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., SQLServerClient).
        """
        self.db_client = db_client

    def _get_table_columns(self, table: str, show_id: bool = False) -> List[str]:
        """
        Get the column names of a table, optionally including the 'id' column.

        Args:
            table (str): The table name.
            show_id (bool): If True, include the 'id' column. Default is False.

        Returns:
            list: List of column names.
        """
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?"""
        if not show_id:
            query += " AND COLUMN_NAME != 'ID'"
        query += " ORDER BY ORDINAL_POSITION"

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            columns = [row['COLUMN_NAME'] for row in result]
            return columns
        except Exception as e:
            logger.error(f"Failed to get table columns. Error: {e}")
            raise

    def _format_dates(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format date fields in a record to a readable string format.

        Args:
            record (dict): The record with potential date fields.

        Returns:
            dict: The record with formatted date fields.
        """
        for key, value in record.items():
            if isinstance(value, (date, datetime)):
                record[key] = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else value.strftime('%Y-%m-%d')
        return record

    def _infer_column_types(self, values: List[Tuple[Any]], columns: List[str], primary_key: str = None) -> Dict[str, str]:
        """
        Infer SQL Server-specific column types with improved type mapping.

        Args:
            values (list of tuples): Sample data for type inference.
            columns (list): Column names.
            primary_key (str, optional): Primary key column name.

        Returns:
            dict: Mapping of columns to SQL Server data types.
        """
        type_mapping = {
            int: "INT",
            float: "FLOAT",
            str: "NVARCHAR(MAX)",
            date: "DATE",
            datetime: "DATETIME2",
            bool: "BIT",
            bytes: "VARBINARY(MAX)"
        }

        inferred_types = {}
        for idx, column in enumerate(columns):
            # Sample multiple rows for better type inference
            sample_values = [row[idx] for row in values if row[idx] is not None]
            if not sample_values:
                inferred_types[column] = "NVARCHAR(MAX)"
                continue

            # Determine type based on all non-null values
            python_type = type(sample_values[0])
            for value in sample_values[1:]:
                if type(value) != python_type:
                    python_type = str  # Default to string for mixed types
                    break

            sql_type = type_mapping.get(python_type, "NVARCHAR(MAX)")

            # Add primary key constraint if applicable
            if column == primary_key:
                sql_type += " PRIMARY KEY"

            inferred_types[column] = sql_type

        return inferred_types

    def normalize_column_name(self, column_name: str, logger: Optional[logging.Logger] = None) -> str:
        """
        Enhanced column name normalization that better handles Portuguese characters
        and special cases for SQL Server compatibility.

        Args:
            column_name (str): Original column name
            logger (Optional[logging.Logger]): Logger instance

        Returns:
            str: Normalized column name suitable for SQL Server
        """
        try:
            # Portuguese character mappings
            char_mappings = {
                'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a', 'ä': 'a',
                'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
                'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
                'ó': 'o', 'ò': 'o', 'õ': 'o', 'ô': 'o', 'ö': 'o',
                'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
                'ý': 'y', 'ÿ': 'y',
                'ñ': 'n',
                'ç': 'c',
                '°': '', '²': '2', '³': '3', '€': 'eur',
                '$': 'dollar', '%': 'percent',
                '(': '', ')': '', '[': '', ']': '', '{': '', '}': '',
                '/': '_', '\\': '_', '|': '_', '-': '_', '.': '_'
            }

            # Common unit indicators to handle specially
            unit_indicators = {
                '(KB)': '',
                '(MB)': '',
                '(GB)': '',
                '(TB)': '',
                '($)': '',
                '(%)': '',
                '(#)': ''
            }

            # Convert to string and lowercase
            name = str(column_name).lower()

            # Handle unit indicators first
            for indicator, replacement in unit_indicators.items():
                if indicator.lower() in name:
                    name = name.replace(indicator.lower(), replacement)

            # Replace special characters
            for original, replacement in char_mappings.items():
                name = name.replace(original, replacement)

            # Remove any remaining diacritics
            name = ''.join(c for c in unicodedata.normalize('NFKD', name)
                        if not unicodedata.combining(c))

            # Replace any remaining non-alphanumeric chars with underscore
            name = re.sub(r'[^a-z0-9_]', '_', name)

            # Replace multiple underscores with single underscore
            name = re.sub(r'_+', '_', name)

            # Remove leading/trailing underscores
            name = name.strip('_')

            # Convert to camelCase
            parts = name.split('_')
            camel_case = parts[0] + ''.join(p.capitalize() for p in parts[1:])

            # Handle SQL Server reserved words
            sql_reserved_words = {
                'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'backup', 'begin',
                'between', 'by', 'case', 'check', 'column', 'constraint', 'create',
                'database', 'default', 'delete', 'desc', 'distinct', 'drop', 'exec',
                'exists', 'foreign', 'from', 'full', 'group', 'having', 'in', 'index',
                'inner', 'insert', 'into', 'is', 'join', 'key', 'left', 'like', 'not',
                'null', 'or', 'order', 'outer', 'primary', 'procedure', 'right', 'rownum',
                'select', 'set', 'table', 'top', 'truncate', 'union', 'unique', 'update',
                'values', 'view', 'where', 'date', 'type', 'de', 'para', 'com', 'sem',
                'ou', 'mas', 'não', 'sim', 'ainda', 'então', 'porque', 'quando', 'onde',
                'como', 'quem', 'qual', 'qualquer', 'algum', 'nenhum', 'muito', 'pouco',
                'mais', 'menos', 'tanto', 'cada', 'outro', 'mesmo', 'mesma', 'diferente',
                'mesmo', 'mesma', 'tudo', 'todos', 'todas', 'alguma', 'algumas', 'nenhum',
                'nenhuma', 'alguns', 'algumas', 'outros', 'outras', 'cada', 'cada um',
                'cada uma', 'algum', 'alguma', 'nenhum', 'nenhuma', 'muito', 'muita',
                'pouco', 'pouca', 'mais', 'menos', 'tanto', 'tanta', 'outro', 'outra',
                'mesmo', 'mesma', 'diferente', 'mesmo', 'mesma', 'tudo', 'todos', 'todas',
                'alguma', 'algumas', 'nenhum', 'nenhuma', 'alguns', 'algumas', 'outros',
                'outras', 'cada', 'cada um', 'cada uma', 'algum', 'alguma', 'nenhum',
                'nenhuma', 'muito', 'muita', 'pouco', 'pouca', 'mais', 'menos', 'tanto',
                'tanta', 'outro', 'outra', 'mesmo', 'mesma', 'diferente', 'mesmo',
                'mesma', 'tudo', 'todos', 'todas', 'alguma', 'algumas', 'nenhum',
                'nenhuma', 'alguns', 'algumas', 'outros', 'outras', 'cada', 'cada um'
            }

            if camel_case.lower() in sql_reserved_words:
                camel_case += 'Col'

            # Ensure name starts with a letter
            if not camel_case[0].isalpha():
                camel_case = 'n' + camel_case

            # Truncate to SQL Server's limit
            camel_case = camel_case[:128]

            if logger:
                logger.debug(f"Normalized column name: {column_name} -> {camel_case}")

            return camel_case

        except Exception as e:
            if logger:
                logger.error(f"Error normalizing column name '{column_name}': {str(e)}")
            # Return a safe fallback name
            return f"column_{abs(hash(str(column_name))) % 1000}"

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]]) -> Tuple[bool, Dict[str, str]]:
        """
        Create a table with normalized column names if it does not already exist.

        Args:
            table (str): The name of the table to create
            columns (List[str]): List of original column names
            values (List[Tuple[Any]]): Sample data to infer column types

        Returns:
            Tuple[bool, Dict[str, str]]:
                - bool: True if table was created, False if it already existed
                - Dict[str, str]: Mapping of original to normalized column names
        """
        try:
            # Split schema and table name
            schema_name = table.split('.')[0] if '.' in table else 'dbo'
            table_name = table.split('.')[-1]

            # Check if table exists
            check_query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
            """

            result = self.db_client.execute_query(check_query, (schema_name, table_name))
            table_exists = result[0][0] > 0 if result else False

            if table_exists:
                logger.info(f"Table '{table}' already exists.")
                # Return existing column mapping
                existing_columns_query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
                """
                existing_cols = self.db_client.execute_query(existing_columns_query, (schema_name, table_name))
                existing_mapping = {col: col for col, in existing_cols}
                return False, existing_mapping

            # Create mapping of original to normalized names
            column_mapping = {col: self.normalize_column_name(col) for col in columns}

            # Handle duplicate normalized names
            seen_names = {}
            for original, normalized in column_mapping.items():
                if normalized in seen_names:
                    count = seen_names[normalized] + 1
                    seen_names[normalized] = count
                    column_mapping[original] = f"{normalized}{count}"
                else:
                    seen_names[normalized] = 1

            # Log the column mapping
            logger.info("Column name mapping:")
            for original, normalized in column_mapping.items():
                logger.info(f"  {original} -> {normalized}")

            # Create list of values with columns in the new order
            normalized_columns = list(column_mapping.values())

            # Infer column types using normalized names
            column_types = self._infer_column_types(values, normalized_columns)

            # Create the columns definition string
            columns_def = ", ".join([f"[{col}] {dtype}" for col, dtype in column_types.items()])

            # Ensure schema exists
            create_schema_query = """
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = ?)
            BEGIN
                EXEC('CREATE SCHEMA [{schema_name}]')
            END
            """
            self.db_client.execute_query(create_schema_query, (schema_name,))

            # Create table query
            create_query = f"""
            CREATE TABLE [{schema_name}].[{table_name}] (
                {columns_def}
            )
            """
            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully with normalized columns")

            return True, column_mapping

        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {str(e)}")
            raise

    def _get_valid_columns(self, table: str, provided_columns: List[str] = None) -> List[str]:
        """
        Get and validate non-identity columns for a table.

        Args:
            table (str): The table name (can include schema)
            provided_columns (List[str], optional): List of columns to validate against

        Returns:
            List[str]: List of valid column names in correct order
        """
        try:
            # Split schema and table name
            schema_name = table.split('.')[0] if '.' in table else 'dbo'
            table_name = table.split('.')[-1]

            # Get non-identity columns
            query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            AND COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 0
            ORDER BY ORDINAL_POSITION
            """

            result = self.db_client.execute_query(query, (table_name,), fetch_as_dict=True)
            actual_columns = [row['COLUMN_NAME'] for row in result]

            if not provided_columns:
                return actual_columns

            # Validate provided columns
            valid_columns = [col for col in provided_columns
                            if col.upper() in (col.upper() for col in actual_columns)]

            if len(valid_columns) != len(actual_columns):
                logger.warning(f"Column mismatch. Expected: {actual_columns}, Got: {valid_columns}")

            return valid_columns

        except Exception as e:
            logger.error(f"Error getting valid columns for {table}: {str(e)}")
            raise

    @retry(max_retries=3, delay=2, backoff=1.5, exceptions=(Exception,), logger=logger)
    def table_exists(self, table: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
        """

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=False)
            return result[0][0] > 0
        except Exception as e:
            logger.error(f"Failed to check if table '{table}' exists. Error: {e}")
            raise

    def cleanup_values(
        self,
        values: List[Tuple[Any, ...]],
        column_names: List[str],
        column_types: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
        log_sample_size: int = 3
    ) -> List[Tuple[Any, ...]]:
        """
        Clean and cast values based on expected SQL types.

        :param values: List of rows (tuples) to clean
        :param column_names: List of column names (must match tuple structure)
        :param column_types: Optional dictionary of column types ('int', 'float', 'date', 'str')
        :param logger: Optional logger for debugging
        :param log_sample_size: Number of rows to print for debug
        :return: Cleaned list of tuples
        """
        cleaned_values = []

        for row in values:
            cleaned_row = []
            for idx, val in enumerate(row):
                col = column_names[idx]
                col_type = column_types.get(col, 'str') if column_types else 'str'

                try:
                    if val is None:
                        cleaned_row.append(None)

                    elif col_type == 'int':
                        cleaned_row.append(int(val) if str(val).strip().isdigit() else None)

                    elif col_type == 'float':
                        if isinstance(val, str) and ',' in val:
                            # Substituir vírgula por ponto para valores decimais
                            val_converted = val.replace(',', '.')
                            cleaned_row.append(float(val_converted) if val_converted.strip() else None)
                        else:
                            cleaned_row.append(float(val) if str(val).strip() else None)

                    elif col_type == 'date':
                        if isinstance(val, str):
                            val = val.strip()
                            if val:
                                # tenta vários formatos comuns
                                for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                                    try:
                                        cleaned_row.append(datetime.strptime(val, fmt).date())
                                        break
                                    except ValueError:
                                        continue
                                else:
                                    cleaned_row.append(None)
                            else:
                                cleaned_row.append(None)
                        else:
                            cleaned_row.append(val)

                    elif isinstance(val, str):
                        val = val.strip()
                        if not val or val.lower() in ('nan', 'nat', 'none'):
                            cleaned_row.append(None)
                        else:
                            cleaned_row.append(val.replace('\x00', ''))

                    else:
                        cleaned_row.append(str(val))

                except Exception as e:
                    if logger:
                        logger.warning(f"Failed to clean value '{val}' in column '{col}': {e}")
                    cleaned_row.append(None)

            cleaned_values.append(tuple(cleaned_row))

        if logger and cleaned_values:
            sample_size = min(log_sample_size, len(cleaned_values))
            logger.debug(f"Sample data for insertion (first {sample_size} rows):")
            for i in range(sample_size):
                logger.debug(f"Row {i+1}: {cleaned_values[i]}")

        return cleaned_values

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None) -> bool:
        """
        Create new records in the specified table with enhanced type handling and error detection.

        Args:
            table (str): The table name
            values (List[Tuple[Any]]): List of tuples of values to insert
            columns (List[str], optional): List of column names. If None, columns will be inferred

        Returns:
            bool: True if records were inserted successfully
        """
        if columns is None:
            columns = self._get_table_columns(table)

        if not isinstance(values, list):
            values = [values]
        elif not all(isinstance(v, tuple) for v in values):
            raise ValueError("Values must be a tuple or a list of tuples.")

        try:
            # Create the table if it doesn't exist and get column mapping
            _, column_mapping = self.create_table_if_not_exists(table, columns, values)

            # Get the normalized column names from keys
            normalized_columns = [col for col in column_mapping.values()]

            # Get valid columns using helper method
            valid_columns = self._get_valid_columns(table, normalized_columns)

            # Create the INSERT query with normalized column names
            columns_str = ", ".join([f"[{col}]" for col in valid_columns])
            placeholders = ", ".join(["?" for _ in valid_columns])
            query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

            # cleanup values
            string_values = self.cleanup_values(values, columns, column_types=None, logger=logger)

            # Execute the batch insert with cleaned values
            self.db_client.execute_batch_query(query, string_values)
            logger.info(f"Successfully inserted {len(values)} records into {table}")
            return True

        except Exception as e:
            logger.error(f"Failed to insert records into {table}. Error: {str(e)}")

            # Print more detailed error info for debugging
            if values:
                try:
                    # Try to identify the problematic record
                    for i, val_tuple in enumerate(values[:10]):  # Check first 10 records
                        for j, val in enumerate(val_tuple):
                            if isinstance(val, str) and '\x00' in val:
                                logger.error(f"Found null byte in record {i}, column {j} (value: {repr(val)})")
                            elif isinstance(val, str) and val.lower() in ('nan', 'none', 'nat'):
                                logger.error(f"Found problematic string value in record {i}, column {j}: {val}")
                except Exception as debug_err:
                    logger.error(f"Error while debugging values: {str(debug_err)}")

            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def read(self, table: str, columns: List[str] = None, where: str = "", params: Tuple[Any] = None, show_id: bool = False, batch_size: int = None) -> List[Dict[str, Any]]:
        """
        Read records from the specified table with optional batch support.

        Args:
            table (str): The table name.
            columns (list, optional): List of column names to retrieve. If None, all columns will be retrieved.
            where (str, optional): WHERE clause for filtering records.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            show_id (bool, optional): If True, include the 'id' column. Default is False.
            batch_size (int, optional): If provided, limits the number of records returned per batch.

        Returns:
            list: List of records as dictionaries.
        """
        if columns is None:
            columns = self._get_table_columns(table, show_id=show_id)

        columns_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {columns_str} FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" ORDER BY {columns[0]} OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        try:
            result = self.db_client.execute_query(query, params, fetch_as_dict=True)
            records = [self._format_dates(row) for row in result]
            logger.info(f"Records read successfully, {len(records)} rows found.")
            return records
        except Exception as e:
            logger.error(f"Failed to read records. Error: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def update(self, table: str, updates: Dict[str, Any], where: str, params: Tuple[Any]) -> bool:
        """
        Update records in the specified table.

        Args:
            table (str): The table name.
            updates (dict): Dictionary of columns and their new values.
            where (str): WHERE clause for identifying records to update.
            params (tuple): Tuple of parameters for the WHERE clause.

        Returns:
            bool: True if records were updated successfully, False otherwise.
        """
        set_clause = ", ".join([f"{col} = ?" for col in updates.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        values = tuple(updates.values()) + params
        try:
            self.db_client.execute_query(query, values)
            logger.info("Records updated successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to update records. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def delete(self, table: str, where: str = "", params: Tuple[Any] = None, batch_size: int = None) -> bool:
        """
        Delete records from the specified table with optional batch processing.

        Args:
            table (str): The table name.
            where (str, optional): WHERE clause for identifying records to delete. If empty, all records will be deleted.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            batch_size (int, optional): If provided, deletes records in batches.

        Returns:
            bool: True if records were deleted successfully, False otherwise.
        """

        # Check if the table exists before attempting to delete
        if not self.table_exists(table):
            logger.warning(f"Table '{table}' does not exist. Delete operation aborted.")
            return False

        query = f"DELETE FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        try:
            self.db_client.execute_query(query, params)
            logger.info("Records deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to delete records. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def execute_raw_query(self, query: str, params: Optional[Any] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw SQL query.

        Args:
            query (str): The SQL query to execute.
            params (Any, optional): Parameters to bind to the query. Default is None.

        Returns:
            Optional[List[Dict[str, Any]]]: If the query is a SELECT query, returns a list of dictionaries
                                        representing rows. Otherwise, returns None.
        """
        try:
            is_select_query = query.strip().lower().startswith('select')
            if is_select_query:
                # Execute the SELECT query and get results as dictionaries
                result = self.db_client.execute_query(query, params, fetch_as_dict=True)
                # Format dates in each record
                formatted_result = [self._format_dates(record) for record in result]
                return formatted_result
            else:
                # Execute the non-SELECT query
                self.db_client.execute_query(query, params)
                logger.info("Query executed successfully.")
                return None
        except Exception as e:
            logger.error(f"Failed to execute raw query. Error: {e}")
            raise
