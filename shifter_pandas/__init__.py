"""
Utilities functions.
"""


def standardize_property(words: str) -> str:
    """
    Get standardize the property name for the Pandas datasource.
    """
    return "".join(word[0].upper() + word[1:].lower() for word in words.replace("-", "_").split(" "))
