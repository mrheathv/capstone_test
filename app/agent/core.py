import json
import streamlit as st
from openai import OpenAI
from typing import Dict, Any
from .tools import TOOLS, get_tools_for_openai

def get_openai_client():
    """Initialize and return an OpenAI client"""
    return OpenAI()

def agent_answer(user_question: str, max_iterations: int = 5) -> str:
    """
    Agent that uses ReAct pattern to answer questions with multiple tools.
    
    Args:
        user_question: The user's natural language question
        tools_registry: Dictionary of available tools {name: Tool}
        max_iterations: Maximum number of reasoning loops (safety limit)
        
    Returns:
        Final synthesized answer as a string
    """
    client = get_openai_client()

    # Convert tools to OpenAI format
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
    
    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_question}  
    ]

    try: 
        for iteration in range(max_iterations):
            print(f"\n{'='*60}")
            print(f"ITERATION {iteration + 1}")
            print(f"{'='*60}")
            # Ask LLM what to do next
            response = client.chat.completions.create(
                model='gpt-4o-mini',
                messages=messages,
                tools=tools_for_openai,
                tool_choice='auto'
            )

            message = response.choices[0].message

            # if no tool calls, LLM has final answer
            if not message.tool_calls:
                print("\n✓ LLM PROVIDED FINAL ANSWER (no more tool calls)")
                print(f"Answer: {message.content[:200]}...")
                return message.content or "I'm not sure how to help with that."
                   

            print(f"\n→ LLM WANTS TO CALL {len(message.tool_calls)} TOOL(S):")
            for tc in message.tool_calls:
                print(f"  - {tc.function.name}({tc.function.arguments})")
            # Assistant's reasoning to conversation
            # Append the assistant's message so that the LLM remembers wht it just decided to do. Without it
            # the conversation would have gaps. 
            messages.append(message)
            print(f"\n→ ADDED assistant message to conversation (now {len(messages)} messages)")

            # Execute each tool the LLM requested
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
                    "content": result
                })
                print(f"→ ADDED tool result to conversation (now {len(messages)} messages)")
            
            # End of inner for loop - back to outer iteration loop
            print(f"\n→ END OF ITERATION {iteration + 1}")
        
        # OUTSIDE the for loop (dedent twice - align with 'for iteration')
        return "I've gathered information but reached my processing limit"
        
    except Exception as e:
        return f"An error occurred while processing your request: {str(e)}"
    
