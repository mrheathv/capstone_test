import duckdb
import pandas as pd

DB_PATH = "db/sales.duckdb"

def db_query(sql: str, params=None) -> pd.DataFrame:
    """
    Execute a SQL query against the DuckDB database.
    
    Args:
        sql: The SQL query string to execute
        params: Optional dictionary of parameters for parameterized queries
        
    Returns:
        pandas DataFrame with query results
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(sql, params or {}).fetchdf()
    finally:
        con.close()

if __name__ == "__main__":
    # Test the function
    result = db_query("SELECT * FROM accounts LIMIT 5")
    print(result)