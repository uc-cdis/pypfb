from pfb.base import PFBBase
from pfb.reader import PFBReader
from pfb.commands import show

# base = PFBBase("test.avro")

# print base.schema(base)


# reader = PFBReader("test.avro")

# while reader.__next__():
# 	print reader

show({}, "test.avro", 5)