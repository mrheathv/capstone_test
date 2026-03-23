import json
import os
import streamlit as st
from openai import OpenAI
from typing import Dict, Any
from .tools import TOOLS, get_tools_for_openai

_AGENT_MODELS = {
    "OpenAI":   "gpt-4o-mini",
    "Claude":   "claude-3-haiku-20240307",
    "DeepSeek": "deepseek-chat",
    "Gemini":   "gemini-1.5-flash",
}


def _to_anthropic_tools(openai_tools: list) -> list:
    """Convert OpenAI tool schema to Anthropic tool schema."""
    return [
        {
            "name": t["function"]["name"],
            "description": t["function"]["description"],
            "input_schema": t["function"]["parameters"],
        }
        for t in openai_tools
    ]


def agent_answer(user_question: str, max_iterations: int = 5, provider: str = "OpenAI") -> str:
    """
    Agent that uses ReAct pattern to answer questions with multiple tools.
    
    Args:
        user_question: The user's natural language question
        tools_registry: Dictionary of available tools {name: Tool}
        max_iterations: Maximum number of reasoning loops (safety limit)
        
    Returns:
        Final synthesized answer as a string
    """
    model = _AGENT_MODELS.get(provider, _AGENT_MODELS["OpenAI"])
    tools_for_openai = get_tools_for_openai()

    current_user = st.session_state.get('current_user', 'Unknown')

    system_message = f"""You are a helpful sales assistant with access to a CRM database.

    Current User: {current_user}

    You have multiple tools available:
    - text_to_sql: For flexible, ad-hoc queries about any data in the database
    - open_work: For quickly getting outstanding work items (automatically filtered for current user)

    IMPORTANT: For questions asking about multiple things (like "open work AND deals closing soon"):
    1. Call open_work first
    2. Then call text_to_sql for the additional information
    3. After gathering all information, provide a synthesized, prioritized answer combining both results

    Do NOT just return raw tool output - always provide a final synthesized answer after gathering information.
    """

    # Make provider available to tool handlers (e.g. text_to_sql_handler) via session state
    st.session_state["_agent_llm_provider"] = provider

    try:
        if provider == "Claude":
            return _agent_answer_claude(user_question, system_message, model, tools_for_openai, max_iterations, provider)
        else:
            return _agent_answer_openai_compat(user_question, system_message, model, tools_for_openai, max_iterations, provider)
    except Exception as e:
        return f"An error occurred while processing your request: {str(e)}"


def _agent_answer_openai_compat(user_question: str, system_message: str, model: str, tools_for_openai: list, max_iterations: int, provider: str) -> str:
    """ReAct agent loop for OpenAI-compatible providers (OpenAI, DeepSeek, Gemini)."""
    if provider == "DeepSeek":
        client = OpenAI(api_key=os.environ.get("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com")
    elif provider == "Gemini":
        client = OpenAI(api_key=os.environ.get("GEMINI_API_KEY"), base_url="https://generativelanguage.googleapis.com/v1beta/openai/")
    else:
        client = OpenAI()

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_question},
    ]

    for iteration in range(max_iterations):
        print(f"\n{'='*60}")
        print(f"ITERATION {iteration + 1}")
        print(f"{'='*60}")

        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools_for_openai,
            tool_choice='auto',
        )

        message = response.choices[0].message

        if not message.tool_calls:
            print("\n✓ LLM PROVIDED FINAL ANSWER (no more tool calls)")
            print(f"Answer: {message.content[:200]}...")
            return message.content or "I'm not sure how to help with that."

        print(f"\n→ LLM WANTS TO CALL {len(message.tool_calls)} TOOL(S):")
        for tc in message.tool_calls:
            print(f"  - {tc.function.name}({tc.function.arguments})")

        messages.append(message)
        print(f"\n→ ADDED assistant message to conversation (now {len(messages)} messages)")

        for tool_call in message.tool_calls:
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)

            print(f"\n→ EXECUTING: {tool_name}")

            tool = TOOLS.get(tool_name)
            if not tool:
                result = f"Error: Tool '{tool_name}' not found."
            else:
                result = tool.handler(tool_args)

            print(f"→ RESULT: {len(result)} characters")
            print(f"  Preview: {result[:150]}...")

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": tool_name,
                "content": result,
            })
            print(f"→ ADDED tool result to conversation (now {len(messages)} messages)")

        print(f"\n→ END OF ITERATION {iteration + 1}")

    return "I've gathered information but reached my processing limit"


def _agent_answer_claude(user_question: str, system_message: str, model: str, tools_for_openai: list, max_iterations: int, provider: str) -> str:
    """ReAct agent loop for Claude (Anthropic SDK)."""
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    anthropic_tools = _to_anthropic_tools(tools_for_openai)

    messages = [{"role": "user", "content": user_question}]

    for iteration in range(max_iterations):
        print(f"\n{'='*60}")
        print(f"ITERATION {iteration + 1} (Claude)")
        print(f"{'='*60}")

        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=system_message,
            messages=messages,
            tools=anthropic_tools,
        )

        # Append assistant turn
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            # Extract text from content blocks
            text_parts = [b.text for b in response.content if hasattr(b, "text")]
            final = " ".join(text_parts).strip()
            print("\n✓ CLAUDE PROVIDED FINAL ANSWER")
            return final or "I'm not sure how to help with that."

        # Collect tool use blocks
        tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
        if not tool_use_blocks:
            text_parts = [b.text for b in response.content if hasattr(b, "text")]
            return " ".join(text_parts).strip() or "I'm not sure how to help with that."

        print(f"\n→ CLAUDE WANTS TO CALL {len(tool_use_blocks)} TOOL(S):")
        tool_results = []
        for block in tool_use_blocks:
            print(f"  - {block.name}({block.input})")
            tool = TOOLS.get(block.name)
            if not tool:
                result = f"Error: Tool '{block.name}' not found."
            else:
                result = tool.handler(block.input)
            print(f"→ RESULT: {len(result)} characters")
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result,
            })

        messages.append({"role": "user", "content": tool_results})
        print(f"\n→ END OF ITERATION {iteration + 1}")

    return "I've gathered information but reached my processing limit"
    
