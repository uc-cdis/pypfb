from fastavro import reader, writer


class PFBFile:
    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def records(self):
        with open(self.filename, self.mode) as f:
            records = reader(f)
            for record in records:
                yield record
