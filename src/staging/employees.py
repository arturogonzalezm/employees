import polars as pl
import re
import sys
import logging
from datetime import datetime

from src.constants import employee_file_path, employee_log_file_path

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (e.g., INFO, DEBUG, WARNING)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    filename=employee_log_file_path,  # Specify the log file
    filemode='w'  # Set the log file mode to 'w' (overwrite) or 'a' (append)
)


def read_csv_to_dataframe(file_path):
    try:
        return pl.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error reading CSV file: {str(e)}")
        sys.exit(1)


def add_underscores_to_column_names(df):
    def add_underscores(column_name):
        return re.sub(r'(?<=[a-z])(?=[A-Z])', '_', column_name)

    return df.rename({col: add_underscores(col) for col in df.columns})


def rename_columns_to_lowercase_with_underscores(df):
    return df.rename({col: col.lower().replace(" ", "_") for col in df.columns})


def data_quality_checks(df):
    # Data Completeness Check
    if df.is_empty():
        logging.warning("Empty DataFrame!")

    # Check if 'JoiningYear' column contains valid years
    validate_joining_years(df)

    # Check for duplicate rows based on certain columns
    check_duplicate_rows(df)


def validate_joining_years(df):
    # Assuming 'JoiningYear' is the name of the column
    joining_year_column = df['JoiningYear']

    # Validate each value in the column
    for index, year in enumerate(joining_year_column.to_list()):
        try:
            # Attempt to convert the value to an integer (year)
            year = int(year)
            # Check if it falls within a valid range (e.g., 1900 to the current year)
            current_year = datetime.now().year
            if 1900 <= year <= current_year:
                continue  # Valid year, continue to the next value
            else:
                logging.warning(f"Invalid year found at index {index}: {year}")
        except ValueError:
            logging.warning(f"Non-integer value found at index {index}: {year}")
        except Exception as e:
            logging.error(f"Error validating year at index {index}: {str(e)}")


def check_duplicate_rows(df):
    # Create an SQLContext
    ctx = pl.SQLContext()

    # Register the DataFrame as a temporary SQL table
    ctx.register('my_table', df)
    # Find duplicate rows based on specified columns
    # duplicate_rows = df.groupby(columns_to_check).agg(pl.count("*").alias("count")).filter(pl.col("count") > 1)

    # Check for duplicate rows based on 'Name' and 'Email' columns using SQL
    duplicate_rows = ctx.execute("""
    SELECT Name,Email,Address,Date_of_Birth,Country,Education,JoiningYear,City,PaymentTier,Age,Gender,EverBenched,ExperienceInCurrentDomain,LeaveOrNot,
    COUNT(*) as count
    FROM my_table
     GROUP BY Name,Email,Address,Date_of_Birth,Country
    HAVING count > 1
    """).collect()

    if not duplicate_rows.is_empty():
        logging.warning("Duplicate rows found:")
        logging.warning(duplicate_rows.to_pandas())


def main():
    # Define the path to the CSV file
    csv_file_path = employee_file_path

    # Read the CSV file into a Polars DataFrame with logging
    df = read_csv_to_dataframe(csv_file_path)

    if df is None:
        # Handle the case where reading the CSV file failed
        sys.exit(1)

    # Perform data quality checks
    data_quality_checks(df)

    # Add underscores between capital letters in column names
    df = add_underscores_to_column_names(df)

    # Rename the columns to lowercase with underscores
    renamed_df = rename_columns_to_lowercase_with_underscores(df)

    # Log the final DataFrame
    logging.info("Final DataFrame:\n%s", renamed_df)
