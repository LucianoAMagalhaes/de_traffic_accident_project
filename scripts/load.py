import pandas as pd
from sqlalchemy import create_engine
import sys

def load_data_to_postgres(file_path, table_name, db_config):
    """
    Load DataFrame to PostgreSQL database.
    
    Parameters:
    - file_path: str, path to the CSV file.
    - table_name: str, name of the table in the database.
    - db_config: dict, database configuration with keys 'user', 'password', 'host', 'port', 'database'.
    """

    try:
        # Read file
        df = pd.read_csv(file_path)
        print(f"Data loaded from {file_path} with shape {len(df)} rows and {len(df.columns)} columns.")

        # Create database string connection
        db_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

        print(f"Connecting to database {db_config['database']} at {db_config['host']}:{db_config['port']}...")
        engine = create_engine(db_string)

        # Load DataFrame to PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data loaded into table {table_name} successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    csv_file_path = '../data/processed/deaths_transformed.csv'
    table_name = 'traffic_accidents'
    db_config = {
        'user': 'postgres',
        'password': 'xperia',
        'host': 'localhost',
        'port': '5432',
        'database': 'dataengineering'
    }

    load_data_to_postgres(csv_file_path, table_name, db_config)
