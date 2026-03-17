from typing import Dict, Any, Callable
from dataclasses import dataclass

@dataclass
class Tool:
    """Specification for an agent tool"""
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema format for OpenAI
    handler: Callable[[Dict[str, Any]], str]

# Global tool registry
TOOLS: Dict[str, Tool] = {}

def register_tool(tool: Tool):
    """Register a tool in the global registry"""
    TOOLS[tool.name] = tool

def get_tools_for_openai():
    """Convert tool registry to OpenAI function calling format"""
    return [
        {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters
            }
        }
        for tool in TOOLS.values()
    ]