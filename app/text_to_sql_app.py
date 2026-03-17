import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

import streamlit as st
from agent import (
    agent_answer,
    open_work_handler,
    text_to_sql_handler,
    Tool,
    register_tool
)
from database import db_query


def get_sales_agents() -> list:
    """Fetch list of unique sales agents from the database"""
    try:
        df = db_query("SELECT DISTINCT sales_agent FROM sales_teams ORDER BY sales_agent")
        return df['sales_agent'].tolist()
    except:
        return ["Unknown"]


# Register the text_to_sql tool
register_tool(Tool(
    name="text_to_sql",
    description="Generate and execute SQL queries from natural language questions about the sales database. Use this for flexible, ad-hoc queries about accounts, deals, interactions, products, and sales teams.",
    parameters={
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": "The natural language question to convert to SQL."
            }
        },
        "required": ["question"]
    },
    handler=text_to_sql_handler
))

# Register the open_work tool
register_tool(Tool(
    name="open_work",
    description="Get a list of outstanding work items and tasks that need attention. This shows deals in 'Engaging' stage from the last 30 days. Use this for questions about 'what to work on', 'outstanding items', 'tasks today', or 'open work'.",
    parameters={
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "description": "Maximum number of items to return (default: 25)"
            },
            "sales_agent": {
                "type": "string",
                "description": "Optional: filter by sales agent name"
            }
        }
    },
    handler=open_work_handler
))


# Streamlit UI
st.set_page_config(page_title='Sales Chatbot', layout='centered')
st.title('Sales Data Chatbot')

# Sidebar with user context
with st.sidebar:
    st.header("User Context")
    
    agents = get_sales_agents()
    
    if "current_user" not in st.session_state:
        st.session_state.current_user = agents[0] if agents else "Unknown"
    
    selected_agent = st.selectbox(
        "Acting as:",
        options=agents,
        index=agents.index(st.session_state.current_user) if st.session_state.current_user in agents else 0
    )
    
    st.session_state.current_user = selected_agent
    st.success(f"✓ Logged in as: {selected_agent}")
    
    st.divider()
    
    st.header("How it works")
    st.markdown("""
    1. Ask a question in plain English
    2. AI chooses the right tool(s)
    3. Query executes with your context
    4. Results displayed in chat
    """)
    
    st.divider()
    st.caption("Tables available:")
    st.code("accounts, interactions, products, sales_pipeline, sales_teams")


# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "Hi! Ask me anything about your sales data. I'll search the database to answer your questions."
        }
    ]

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Chat input
user_question = st.chat_input("Ask a question about your sales data...")
if user_question:
    st.session_state.messages.append({"role": "user", "content": user_question})
    with st.chat_message("user"):
        st.markdown(user_question)
    
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            reply = agent_answer(user_question)
            st.markdown(reply)

    st.session_state.messages.append({"role": "assistant", "content": reply})
