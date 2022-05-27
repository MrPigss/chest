import cProfile
import os
from pathlib import Path
from time import perf_counter, time_ns
from typing import List

from msgspec import msgpack

from chest import ChestDatabase

avg_times: List[int] = []
p = Path("testdb.db")
d = msgpack.Decoder()


def f():
    with ChestDatabase(p, "r+") as db:

        for i in range(10_000):
            entry = msgpack.encode(
                {
                    "id": i,
                    "email": "jill_tran@krog.fishing",
                    "username": "jill88",
                    "profile": {
                        "name": "Jill Tran",
                        "company": "Krog",
                        "dob": "1988-04-09",
                        "address": "33 Harden Street, Matheny, Missouri",
                        "location": {"lat": 48.879835, "long": 138.105851},
                        "about": "Qui ad nisi enim ex excepteur ut ad nostrud non quis. Quis magna sit ut deserunt occaecat enim incididunt.",
                    },
                    "apiKey": "f72263de-34c3-4a74-9a69-e69518ef5822",
                    "roles": ["admin"],
                    "createdAt": time_ns(),
                    "updatedAt": "2013-05-15T10:55:05.786Z",
                }
            )
            start = perf_counter()
            db[i] = entry
            # db[i]
            avg_times.append(perf_counter() - start)
        print(f"{1/(sum(avg_times) / len(avg_times)):.1f} ops/sec, {sum(avg_times):4}")

        # avg = []
        # for i in range(90_000):
        #     start = perf_counter()
        #     del db[i]
        #     avg.append(perf_counter() - start)
        # print(f"{1/(sum(avg) / len(avg)):.1f} ops/sec, {sum(avg):4}")


    # for i in range(4000, 70000):
    #     db[i] = msgpack.encode(
    #         {
    #             "name": "Jill Tran",
    #             "company": "Krog",
    #             "dob": "1988-04-09",
    #             "address": "33 Harden Street, Matheny, Missouri",
    #             "location": {"lat": 48.879835, "long": 138.105851},
    #             "about": "Qui ad nisi enim ex excepteur ut ad nostrud non quis. Quis magna sit ut deserunt occaecat enim incididunt.",
    #         }
    #     )


# f()
cProfile.run("f()", "prof/test.prof")
os.system(
    f"gprof2dot -n0 -e0 -f pstats ./prof/test.prof | dot -Tpng -o ./prof/test.png"
)


# f()
