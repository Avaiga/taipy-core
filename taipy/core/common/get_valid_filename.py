import re


def get_valid_filename(s):
    """
    Source: https://github.com/django/django/blob/main/django/utils/text.py
    """
    s = str(s).strip().replace(" ", "_")
    return re.sub(r"(?u)[^-\w.]", "", s)
