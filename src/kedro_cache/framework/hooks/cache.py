from dataclasses import dataclass


@dataclass
class NodeCache:
    """Dataclass to store cache information for a node."""

    node_name: str
    input_hash: str
    output_hash: str
    function_hash: str
