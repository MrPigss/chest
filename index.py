import struct
from random import randint
from time import perf_counter

row = struct.Struct("2I")
decode_rows = row.iter_unpack
pack = row.pack_into
test = tuple()
test_d = {randint(0, 48909): randint(0, 345879) for _ in range(20)}
j = 0
buffer = bytearray(8 * 20)

def index(file: str) -> set:
    with open(file, "rb") as f:
        data = f.read()
        a = dict(decode_rows(data))
        return a


with open("index.bin", "wb+") as f:
    start = perf_counter()
    for i in test_d:
        pack(buffer, 8 * j, i, test_d[i])
        j += 1
    else:
        t = i
    print(perf_counter() - start)
    f.write(buffer)

x = index("index.bin")

if t in x:
    x[t] = 1

print(x)
