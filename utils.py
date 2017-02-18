from string import ascii_letters
from random import choice


def random_word(n):
    """Create arbitrary camel casing string from ASCII symbols.

    Args:
    - n: lenght of string"""
    return ''.join(choice(ascii_letters) for i in range(n))
