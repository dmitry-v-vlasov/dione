import re


def urlify(s: str):
    # Remove all non-word characters (everything except numbers and letters)
    s = re.sub(r"[^\w\s]", '', s)
    # Replace all runs of whitespace with a single dash
    s = re.sub(r"\s+", '-', s)
    return s


def clear_nonwords(s: str):
    # Remove all non-word characters (everything except numbers and letters)
    s = re.sub(r"[^\w\s]", '', s)
    return s.strip()

def to_snake_case(s: str):
    s = clear_nonwords(s)
    s = re.sub(r"\s+", '_', s)
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    s = re.sub('__([A-Z])', r'_\1', s)
    s = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()

