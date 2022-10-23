import inspect
from typing import Any, Callable, Dict

import joblib


def hash_datadict(d: Dict[str, Any]) -> str:
    """Hash a dictionary of data.

    Args:
        d: The dictionary to hash.

    Returns:
        The hash of the dictionary.
    """
    # calculate hash for each input
    hash_set = {joblib.hash((k, v), coerce_mmap=True) for k, v in d.items()}

    # calculate hash for the set of hashes
    hash = joblib.hash(hash_set)

    # check that hash is not None
    assert hash is not None, "Hashing failed"
    return hash


def hash_function_body(f: Callable) -> str:
    """Hash the source code of a function.

    Args:
        f: The function to hash.

    Returns:
        The hash of the function.
    """
    # calculate hash for function body
    function_body = inspect.getsource(f)
    function_hash = joblib.hash(function_body)

    # check that hash is not None
    assert function_hash is not None, "Hashing failed"
    return function_hash
