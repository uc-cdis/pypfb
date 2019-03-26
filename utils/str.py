import base64


# https://stackoverflow.com/a/42377964/1030110
def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k, unicode) else k: v.encode('utf-8') if isinstance(v, unicode) else v for
            k, v in obj}


def encode(raw_value):
    return base64.b64encode(raw_value).rstrip("=")


def decode(encoded_value):
    return base64.b64decode(encoded_value).rstrip("=")
