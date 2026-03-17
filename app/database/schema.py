import duckdb
from .connection import DB_PATH

def get_schema_info() -> str:
    """Extract database schema with sample values for better context"""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        # Get all tables and views
        tables_df = con.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchdf()
        
        schema_info = ["Database Schema:\n"]
        
        for _, row in tables_df.iterrows():
            table_name = row['table_name']
            table_type = row['table_type']
            
            # Get columns for this table
            columns_df = con.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'main' 
                  AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchdf()
            
            schema_info.append(f"\n{table_type}: {table_name}")
            
            for _, col in columns_df.iterrows():
                col_name = col['column_name']
                col_type = col['data_type']
                
                # Get sample values for this column
                try:
                    # Special handling for text fields - don't show full content
                    if col_type in ['VARCHAR', 'TEXT'] and col_name.lower() in ['comment', 'description', 'notes']:
                        schema_info.append(f"  - {col_name} ({col_type}) [contains text notes]")
                    else:
                        samples = con.execute(f"""
                            SELECT DISTINCT {col_name}
                            FROM {table_name}
                            WHERE {col_name} IS NOT NULL
                            LIMIT 5
                        """).fetchdf()
                        
                        if not samples.empty:
                            sample_vals = samples[col_name].tolist()
                            # Truncate long strings
                            sample_vals = [str(v)[:50] + "..." if len(str(v)) > 50 else str(v) for v in sample_vals]
                            sample_str = ", ".join(sample_vals)
                            schema_info.append(f"  - {col_name} ({col_type}) [examples: {sample_str}]")
                        else:
                            schema_info.append(f"  - {col_name} ({col_type})")
                except:
                    schema_info.append(f"  - {col_name} ({col_type})")
        
        return "\n".join(schema_info)
    finally:
        con.close()


def get_business_context() -> str:
    """Provide business context and table relationships"""
    return """
    Business Context & Table Relationships:

    KEY RELATIONSHIPS:
    - accounts.account_id → sales_pipeline.account_id (one-to-many)
    - accounts.account_id → interactions.account_id (one-to-many)
    - products.product_id → sales_pipeline.product_id (one-to-many)
    - sales_teams.sales_agent → sales_pipeline.sales_agent (one-to-many)

    IMPORTANT VIEWS (use these for common queries):
    - v_open_work: Outstanding work items (deals in 'Engaging' stage from last 30 days)
    - v_pipeline_snapshot: Current state of all deals
    - v_accounts_summary: Account overview with last touch date
    - v_interactions_norm: Normalized interaction history

    BUSINESS RULES:
    - Deal stages: Prospecting → Engaging → Won/Lost
    - "Outstanding items" or "open work" = deals in 'Engaging' stage
    - "Last touch" = most recent interaction date with an account

EXAMPLE QUERIES:

Q: "Show me all accounts in the technology sector"
A: SELECT account_id, account, sector, revenue FROM accounts WHERE sector = 'technolgy';

Q: "What deals does Elease Gluck have?"
A: SELECT * FROM sales_pipeline WHERE sales_agent = 'Elease Gluck';

Q: "Show me engaging stage deals"
A: SELECT * FROM v_pipeline_snapshot WHERE deal_stage = 'Engaging';

Q: "Which accounts does a sales agent work with?"
A: SELECT DISTINCT a.account_id, a.account, sp.sales_agent 
   FROM accounts a 
   JOIN sales_pipeline sp ON a.account_id = sp.account_id 
   WHERE sp.sales_agent = 'Sales Agent Name';

Q: "Which accounts haven't been contacted recently?"
A: SELECT account_id, account_name, last_touch FROM v_accounts_summary WHERE last_touch < CURRENT_DATE - INTERVAL '30 days';

CRITICAL: When querying the 'accounts' table, the company name column is 'account' NOT 'account_name'.
    """