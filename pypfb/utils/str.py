import base64
import sys


if sys.version_info[0] == 3:
    PY3 = True
else:
    PY3 = False


# https://stackoverflow.com/a/42377964/1030110
# required to load JSON without 'unicode' keys and values in Python 2
#
# TODO: remove in Python 3
def str_hook(obj):
    if PY3:
        return obj
    return {
        k.encode("utf-8")
        if isinstance(k, unicode)
        else k: v.encode("utf-8")
        if isinstance(v, unicode)
        else v
        for k, v in obj
    }


def encode(raw_value):
    return base64.b64encode(raw_value).rstrip("=")


def decode(encoded_value):
    return base64.b64decode(encoded_value + "=" * (-len(encoded_value) % 4))
