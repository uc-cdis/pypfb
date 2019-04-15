from fastavro import reader


def records(filename):
    with open(filename, 'rb') as f:
        r = reader(f)
        for record in r:
            yield record


def schema(filename):
    with open(filename, 'rb') as f:
        s = reader(f).schema
    return s
