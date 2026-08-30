import json
from importlib.resources import files


def load_packaged_config(relative_path: str) -> dict:
    """Load a JSON config bundled in the cebl_data package.

    Args:
        relative_path (str): The filename or path of the JSON file relative
            to the "cebl_data/configs/" directory.

    Returns:
        dict: A Python dictionary containing the parsed JSON data.
    """
    with files("cebl_data").joinpath("configs", relative_path).open(encoding="utf-8") as f:
        return json.load(f)