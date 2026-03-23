import json
import os
from database import db_query, get_schema_info, get_business_context
from typing import Dict, Any

_AGENT_MODELS = {
    "OpenAI":   "gpt-4o-mini",
    "Claude":   "claude-3-haiku-20240307",
    "DeepSeek": "deepseek-chat",
    "Gemini":   "gemini-1.5-flash",
}


def _get_completion_client(provider: str):
    """Return (client, model, is_anthropic) for the given provider."""
    model = _AGENT_MODELS.get(provider, _AGENT_MODELS["OpenAI"])
    if provider == "Claude":
        import anthropic
        return anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY")), model, True
    from openai import OpenAI
    if provider == "DeepSeek":
        return OpenAI(api_key=os.environ.get("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com"), model, False
    if provider == "Gemini":
        return OpenAI(api_key=os.environ.get("GEMINI_API_KEY"), base_url="https://generativelanguage.googleapis.com/v1beta/openai/"), model, False
    return OpenAI(), model, False

def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Basic validation to ensure SQL is safe to execute.
    Returns (is_valid, error_message)
    """
    sql_upper = sql.upper().strip()
    
    # Check 1: Must start with SELECT
    if not sql_upper.startswith("SELECT"):
        return False, "Only SELECT queries are allowed"
    
    # Check 2: No dangerous keywords
    dangerous_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            return False, f"Dangerous keyword detected: {keyword}"
    
    return True, ""


def generate_sql_with_retry(user_question: str, max_attempts: int = 2, provider: str = "OpenAI") -> tuple[str, str, int]:
    """
    Generate SQL with error recovery.
    Returns: (sql, error_message, total_tokens)
    If successful, error_message is empty string.
    """
    client, model, is_anthropic = _get_completion_client(provider)
    schema = get_schema_info()
    context = get_business_context()

    last_error = ""
    last_sql = ""
    total_tokens = 0

    for attempt in range(max_attempts):
        if attempt == 0:
            # First attempt - normal prompt
            prompt = f"""You are a SQL expert. Given this database schema and a user question, generate a valid DuckDB SQL query.

{schema}

{context}

User question: {user_question}

Generate ONLY the SQL query, no explanation. Use read-only SELECT statements only.
Prefer using the views when appropriate for the question.
"""
        else:
            # Retry attempt - include error feedback
            prompt = f"""Your previous SQL query failed with this error:

Error: {last_error}

Previous query:
{last_sql}

Here is the schema again:
{schema}

User question: {user_question}

Please fix the query. Pay careful attention to:
1. Use the EXACT column names from the schema
2. Check which table/view has the columns you need
3. Generate ONLY the corrected SQL query, no explanation.
"""
        
        if is_anthropic:
            api_resp = client.messages.create(
                model=model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            sql = api_resp.content[0].text.strip()
        else:
            api_resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
            )
            if api_resp.usage:
                total_tokens += api_resp.usage.total_tokens
            sql = api_resp.choices[0].message.content.strip()

        # Clean markdown
        if sql.startswith("```"):
            sql = "\n".join(sql.split("\n")[1:])
        if sql.endswith("```"):
            sql = "\n".join(sql.split("\n")[:-1])
        sql = sql.strip()

        # Validate
        is_valid, error_msg = validate_sql(sql)
        if not is_valid:
            last_error = error_msg
            last_sql = sql
            continue

        # Try to execute
        try:
            db_query(sql)  # Test execution
            return sql, "", total_tokens  # Success!
        except Exception as e:
            last_error = str(e)
            last_sql = sql
            # Continue to next attempt

    # All attempts failed
    return last_sql, last_error, total_tokens



# wrap our SQL generation in a tool handler function. 
def text_to_sql_handler(args: Dict[str, Any]) -> str:
    """
    Tool handler for generating and executing SQL from natural language.
    This wraps our existing generate_sql_with_retry logic.
    Provider is read from st.session_state["_agent_llm_provider"] if set.
    """
    import streamlit as st
    question = args.get("question", "")

    if not question:
        return "Error: No question provided."

    provider = st.session_state.get("_agent_llm_provider", "OpenAI")

    # Generate SQL with retry logic
    sql, error, _ = generate_sql_with_retry(question, max_attempts=2, provider=provider)

    if error:
        return f"SQL generation failed: {error}\n\nLast attempted SQL:\n```sql\n{sql}\n```"
    
    # Execute the validated SQL
    try:
        results_df = db_query(sql)
        
        if results_df.empty:
            return f"No results found.\n\nSQL used:\n```sql\n{sql}\n```"
        
        # Show SQL query + results
        return f"**SQL Query:**\n```sql\n{sql}\n```\n\nFound {len(results_df)} results:\n\n```\n{results_df.to_string(index=False)}\n```"
    
    except Exception as e:
        return f"Error executing query: {str(e)}\n\nSQL:\n```sql\n{sql}\n```"