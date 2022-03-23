import _thread as Thread
import os
from pathlib import Path
from threading import Event, Lock
from typing import Dict, List, Literal, MutableMapping, Tuple

from msgspec import DecodeError
from msgspec.msgpack import Decoder, Encoder


def lenb(bytes: bytes):
    return len(bytes).to_bytes(4, "big", signed=False)


base_flags = os.O_CREAT | os.O_RDWR | os.O_BINARY


def index_optimised(path, flags):
    return os.open(path, flags | base_flags | os.O_SEQUENTIAL)


def data_optimised(path, flags):
    return os.open(path, flags | base_flags | os.O_RANDOM)


class ChestException(Exception):
    "Base class for exceptions during usage of ChestDB."


class IndexException(ChestException):
    "Something went wrong when trying to load the indexfile."


class ChestDatabase(MutableMapping):
    MIN_BLOCK_SIZE = 0x04

    def __init__(self, path: Path, mode: Literal["r", "r+"] = "r"):
        #threading stuff
        self._shutdown_lock = Lock()

        self._modified: Event = Event()

        self._is_running = True
        # _indexfile contains the indexes with their respective locations in the _datafile,
        # _datafile contains all the data in binary format pointed to by _indexfile.
        self._indexfile: Path = path
        self._datafile: Path = path.with_suffix(".bin")

        # writemode, readonly or read and write.
        self._mode = mode

        # The index is an in-memory dict, mirroring the index file.
        # It maps indexes to their position in the datafile
        self._index: Dict[str | bytes | int, int] = dict()

        # free_blocks keep track of the free blocks in the file.
        # These free bocks might be reused later on.
        self._free_space: List[Tuple[int, int]] = []
        self._fragments: List[Tuple[int, int]] = []

        # initializing an encoder and decoder wich will be used for the index.
        self._decoder = Decoder(Dict[str | bytes | int, int])
        self._encoder = Encoder()

        # checks that everything exists
        self._check()
        self._load()

        Thread.start_new_thread(self._threaded_index_writer, ())

    def _check(self):
        if not self._mode in {"r", "r+"}:
            raise AttributeError(
                f'access_mode is not one of ("r", "r+"), :{self._mode}'
            )

        if not isinstance(self._indexfile, Path):
            raise TypeError("path is not an instance of pathlib.Path")

        if not self._indexfile.exists():
            if self._mode == "r":
                raise FileNotFoundError(
                    f"""File can't be found, use access_mode='r+' if you wan to create it.\nPath: <{self._indexfile.absolute()}>,
                        """
                )

        if self._mode == "r+":
            self.fh_index = open(self._indexfile, "rb+", buffering=0, opener=index_optimised)
            self.fh_data = open(self._datafile, "rb+", buffering=0, opener=data_optimised)
        else:
            self.fh_index = self._indexfile.open("rb", buffering=0)
            self.fh_data = self._datafile.open("rb", buffering=0)

    def _load(self):
        # ?: needs a way to populate free_blocks
        self._populate_free_blocks()

        try:
            self.fh_index.seek(0)
            self._index = self._decoder.decode(self.fh_index.read())

        except DecodeError:

            self.fh_index.seek(0)
            if self.fh_index.read():
                raise IndexException("Index might be corrupt.")

            pass

        except OSError:
            raise IndexException

    def _populate_free_blocks(self):
        self.fh_data.seek(0)
        while len((block := self.fh_data.read(4))) == 4:
            size = int.from_bytes(block, "big", signed=True)
            if size < 0:
                size = ~size
                pos = self.fh_data.tell()
                self._setfree(pos - 4, size)
            try:
                self.fh_data.seek(size, 1)
            except OSError:
                print("oops")

        print(self._free_space)
        # pass

    def _commit(self):
        self._modified.set()

    def _addval(self, key: bytes, val: bytes):
        self.fh_data.seek(0, 2)

        pos = int(self.fh_data.tell())
        self.fh_data.write(lenb(val) + val)

        self._index[key] = pos
        self._commit()

    def _setval(self, key, val, pos):
        self.fh_data.seek(pos)
        self.fh_data.write(lenb(val) + val)

        self._index[key] = pos
        self._commit()

    def __setitem__(self, key, value):
        # todo handle input checks
        new_size = len(value)
        if key in self._index:
            old_pos = self._index[key]  # pos from index
            self.fh_data.seek(old_pos)  # go to pos
            old_size = int.from_bytes(self.fh_data.read(4), "big", signed=False)  # read size from data file

            if new_size > old_size:  # bigger

                # new data doesn't fit, free the used storage
                self._setfree(old_pos, old_size)

                # check for room elsewhere or add to the end of file
                if new_pos := self._claim_free_space(new_size):
                    self._setval(key, value, new_pos)

                else:
                    self._addval(key, value)

            elif new_size < old_size:  # smaller
                if (free := (old_size - new_size)) > 4:  # there should be at least 5 bytes of free space because 4 are used for storing size
                    self._setval(key, value, old_pos)
                    new_free_pos = old_pos + new_size + 4
                    new_free_size = free - 4
                    self._setfree(new_free_pos, new_free_size)

                else:
                    value += b"\0" * free
                    self._setval(key, value, old_pos)
            else:  # same size
                self._setval(key, value, old_pos)
        else:
            if (new_pos := self._claim_free_space(new_size)) != None:
                self._setval(key, value, new_pos)
            else:
                self._addval(key, value)

    def _claim_free_space(self, size) -> int:
        for i, block in enumerate(self._free_space):
            free_pos = block[0]
            free_size = block[1]

            if size == free_size:
                del self._free_space[i]
                return free_pos

            if size < free_size:
                del self._free_space[i]
                new_free_pos = free_pos + size + 4
                new_free_size = free_size - size - 4
                self._setfree(new_free_pos, new_free_size)
                return free_pos

        return None

    def __getitem__(self, key):
        # todo handle the errors

        pos = self._index[key]  # may raise KeyError

        self.fh_data.seek(pos)
        dat = memoryview(self.fh_data.read(512))
        siz = dat[0:4]
        dat = dat[4 : int.from_bytes(siz, "big", signed=True) + 4]
        return dat

    def __delitem__(self, key) -> None:
        self._modified = True

        pos = self._index.pop(key)
        self.fh_data.seek(pos)

        siz = int.from_bytes(self.fh_data.read(4), "big", signed=True)
        self._setfree(pos, siz)

        # self._commit()

    def __enter__(self):
        return self

    def __iter__(self):
        return iter(self._index) if self._index != None else iter([])

    def close(self, exc_type, exc_value, exc_traceback):
        self._commit()
        self._is_running = False
        self._shutdown_lock.acquire()
        self.fh_index.flush()
        self.fh_data.flush()
        self.fh_data.close()
        self.fh_index.close()

    __exit__ = close

    def _setfree(self, pos, siz):
        # todo: sort, ...

        # check free blocks for a chance of coalescing
        for i, free in enumerate(self._free_space):
            # check for free blocks before new free block
            if 4 + sum(free) == pos:
                pos = free[0]
                siz = siz + free[1] + 4
                del self._free_space[i]
                break

            # check for free blocks after new free block
            elif (pos + siz + 4) == free[0]:
                siz = siz + free[1] + 4
                del self._free_space[i]
                break

        self.fh_data.seek(pos)
        self.fh_data.write((~siz).to_bytes(4, "big", signed=True))
        self.fh_data.write(b"\0" * siz)
        # if siz < 220: # block is too small, no use to keep checking, add to fragments list, can be used later
        #     return
        self._free_space.append((pos, siz))

    def __len__(self):
        raise NotImplemented

    def _threaded_index_writer(self):
        with self._shutdown_lock:
            while self._is_running:
                self._modified.wait()
                self.fh_index.seek(0)
                self.fh_index.write(self._encoder.encode(self._index))
                self.fh_index.truncate()
                self._modified.clear()