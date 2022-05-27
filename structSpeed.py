from pathlib import Path
# from struct import pack, unpack
# from sys import byteorder
from time import perf_counter_ns
from chest import ChestDatabase

a = 2048
b = 2048

# p = Path("testdb.db")
avg_times = []
# db = ChestDatabase(p, "r+")

# for _ in range(100):
#     start = perf_counter()
#     db._populate_free_blocks()
#     avg_times.append(perf_counter() - start)
# print(f"{(1/(sum(avg_times) / len(avg_times)))*100:.1f} ops/sec, {sum(avg_times):4}")

# db.close('','','')

for i in range(1_000_000):
    start = perf_counter_ns()
    a^b
    avg_times.append(perf_counter_ns()-start)

# assert a+b == a^b
print(a+b)
print(a^b)
print(f"{(1/(sum(avg_times) / len(avg_times)))} avg , {sum(avg_times):4} total")
