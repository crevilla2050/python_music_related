from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class Message:

    # Class representing a message with a key and optional parameters
    key: str  # The key identifying the message type
    params: Dict[str, Any] | None = None  # Optional dictionary of parameters for the message

    def to_dict(self):
        # Convert the message to a dictionary format
        # Returns a dictionary with 'key' and 'params' fields
        # If params is None, it defaults to an empty dictionary
        return {
            "key": self.key,
            "params": self.params or {}
        }
