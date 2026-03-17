import streamlit as st
from database import db_query


def open_work_handler(args):
    """
    Predefined tool for fetching outstanding work items.
    Automatically fitlers by current user unless override specified. 

    Args:
        args: Dictionary with optional 'limit' and 'sales_agent' keys
        
    Returns:
        Formatted string of outstanding work items
    """ 

    limit = args.get('limit', 25)
    sales_agent = args.get('sales_agent')

    # use current user from session state if no agent specified
    if not sales_agent and 'current_user' in st.session_state:
        sales_agent = st.session_state.current_user

    try:
        sql = """
            SELECT 
                account_id,
                account AS account_name,
                deal_stage,
                sales_agent,
                product,
                activity_type,
                status_lc,
                d_interaction AS last_activity_date,
                comment
            FROM v_open_work
        """
        
        conditions = []
        if sales_agent:
            conditions.append(f"LOWER(sales_agent) = LOWER('{sales_agent}')")

            if conditions:
                sql += " WHERE " + " AND ".join(conditions)

            sql += f" ORDER BY d_interaction DESC NULLS LAST LIMIT {limit}"

            results_df = db_query(sql)

            if results_df.empty:
                return f"No outstanding work items found for sales agent '{sales_agent}'."
            
        # Format results
        lines = [f"**Outstanding Work Items for {sales_agent}** ({len(results_df)} found):"]
        for _, row in results_df.iterrows():
            acct = row.get("account_name", "Unknown")
            stage = row.get("deal_stage", "")
            prod = row.get("product", "")
            activity = row.get("activity_type", "")
            status = row.get("status_lc", "")
            date = row.get("last_activity_date", "")
            comment = row.get("comment", "")
            
            line = f"- **{acct}** • {stage} • Product: {prod}"
            if activity:
                line += f" • Last: {activity} ({status}) on {date}"
            if isinstance(comment, str) and comment.strip():
                snippet = (comment[:80] + "...") if len(comment) > 80 else comment
                line += f"\n  _{snippet}_"
            
            lines.append(line)
        
        return "\n".join(lines)
        
    except Exception as e:
        return f"Error fetching open work: {str(e)}"           

