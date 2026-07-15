from pfb.writer import PFBWriter

with PFBWriter("test.pfb") as writer:
    writer.set_schema(
        [
            {
                "type": "record",
                "name": "sample",
                "fields": [{"name": "id", "type": "string"}],
            }
        ]
    )
    writer.set_metadata({"nodes": [], "misc": {}})
    writer.set_gen3metadata(
        {
            "name": "example-dataset",
            "title": "Example Dataset",
            "description": "A simple example",
            "licenses": [{"name": "CC0-1.0"}],
        }
    )
    writer.write(iterable=[{"name": "sample", "object": {"id": "1"}}])
