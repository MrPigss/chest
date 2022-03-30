import os
from pathlib import Path
from typing import Dict, List, Literal, MutableMapping, Tuple

from msgspec import DecodeError
from msgspec.msgpack import Decoder, Encoder

base_flags = os.O_CREAT | os.O_RDWR | os.O_BINARY


def lenb(bytes: bytes):
    return len(bytes).to_bytes(4, "big", signed=False)


def index_optimised(path, flags):
    return os.open(path, flags | base_flags | os.O_SEQUENTIAL)


def data_optimised(path, flags):
    return os.open(path, flags | base_flags | os.O_RANDOM)


class ChestDatabase(MutableMapping):
    PREFIX_SIZE = 0x04
    BUFFER_SIZE = 0x200
    MIN_BLOCKSIZE = 0x10

    def __init__(self, path: Path, mode: Literal["r", "r+"] = "r"):

        # _indexfile contains the indexes with their respective locations in the _datafile,
        # _datafile contains all the data in binary format pointed to by _indexfile.
        self._indexfile: Path = path
        self._datafile: Path = path.with_suffix(".bin")

        # writemode, readonly or read and write.
        self._mode = mode

        # The index is an in-memory dict, mirroring the index file.
        # It maps indexes to their position in the datafile
        self._index: Dict[str | bytes | int, int] = dict()

        # A buffer to write the encoded messagepack bytes into
        self._buffer: bytearray = bytearray(32)

        # free_blocks keep track of the free blocks in the file.
        # These free bocks might be reused later on.
        # fragments are blocks that can't be reused, (too small).
        self._free_space: List[Tuple[int, int]] = []
        self._fragments: List[Tuple[int, int]] = []

        # initializing an encoder and decoder wich will be used for the index.
        self._decoder = Decoder(Dict[str | bytes | int, int])
        self._encoder = Encoder()

        # checks that everything exists
        self._check()
        self._load()

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
            self.fh_index = open(
                self._indexfile, "rb+", buffering=0, opener=index_optimised
            )
            self.fh_data = open(
                self._datafile, "rb+", buffering=0, opener=data_optimised
            )
        else:
            self.fh_index = self._indexfile.open("rb", buffering=0)
            self.fh_data = self._datafile.open("rb", buffering=0)

    def _load(self):
        self._populate_free_blocks()

        try:
            self.fh_index.seek(0)
            self._index = self._decoder.decode(self.fh_index.read())

        except DecodeError:

            self.fh_index.seek(0)
            if self.fh_index.read():
                raise DecodeError("Index might be corrupt.")

            pass

    def _populate_free_blocks(self):
        self.fh_data.seek(0)

        while len((block := self.fh_data.read(4))) == 4:

            size = int.from_bytes(block, "big", signed=True)

            if size < 0:
                size = ~size
                pos = self.fh_data.tell()
                self._setfree(pos - 4, size)
                self.fh_data.seek(size, 1)

    def _commit(self):
        self._index_writer()

    def _index_writer(self):
        self.fh_index.seek(0)
        self._encoder.encode_into(self._index, self._buffer)
        self.fh_index.write(self._buffer)
        self.fh_index.truncate()

    def _addval(self, key, val: bytes):
        self.fh_data.seek(0, 2)

        pos = self.fh_data.tell()
        self.fh_data.write(lenb(val) + val)

        self._index[key] = pos
        # self._commit()

    def _setval(self, key, val: bytes, pos: int):
        self.fh_data.seek(pos)
        self.fh_data.write(lenb(val) + val)

        self._index[key] = pos
        # self._commit()

    def __getitem__(self, key):

        pos = self._index[key]  # may raise KeyError

        self.fh_data.seek(pos)
        dat = self.fh_data.read(self.BUFFER_SIZE)

        siz = int.from_bytes(dat[: self.PREFIX_SIZE], "big", signed=True)

        if siz > (x := (self.BUFFER_SIZE - self.PREFIX_SIZE)):
            dat += self.fh_data.read(siz - x)

        dat = dat[self.PREFIX_SIZE : siz + self.PREFIX_SIZE]
        return dat

    def __delitem__(self, key) -> None:
        self._modified = True

        pos = self._index.pop(key)
        self.fh_data.seek(pos)

        siz = int.from_bytes(self.fh_data.read(self.PREFIX_SIZE), "big", signed=True)
        self._setfree(pos, siz)

        # self._commit()

    def __setitem__(self, key: int | str | bytes, value: bytes):

        # if not isinstance(key, int | str | bytes):
        #     raise KeyInputError

        # if not isinstance(key, int | str | bytes):
        #     raise DataInputError

        new_size = len(value)
        if key in self._index:
            old_pos = self._index[key]  # pos from index
            self.fh_data.seek(old_pos)  # go to pos
            old_size = int.from_bytes(
                self.fh_data.read(self.PREFIX_SIZE), "big", signed=False
            )  # read size from data file

            if new_size > old_size:  # bigger

                # check for room elsewhere or add to the end of file
                if new_pos := self._claim_free_space(new_size):
                    self._setval(key, value, new_pos)

                else:
                    self._addval(key, value)

                # new data doesn't fit, free the used storage
                self._setfree(old_pos, old_size)

            elif new_size < old_size:  # smaller
                if (
                    free := (old_size - new_size)
                ) > self.PREFIX_SIZE:  # there should be at least 5 bytes of free space because 4 are used for storing size
                    self._setval(key, value, old_pos)
                    new_free_pos = old_pos + new_size + self.PREFIX_SIZE
                    new_free_size = free - self.PREFIX_SIZE
                    self._setfree(new_free_pos, new_free_size)

                else:
                    # value += b"\0" * free
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
                new_free_pos = free_pos + size + self.PREFIX_SIZE
                new_free_size = free_size - size - self.PREFIX_SIZE
                self._setfree(new_free_pos, new_free_size)
                return free_pos

    def _setfree(self, pos, siz):
        # check free blocks for a chance of coalescing
        for i, free in enumerate(self._free_space):
            # check for free blocks before new free block
            if 4 + sum(free) == pos:
                pos = free[0]
                siz = siz + free[1] + self.PREFIX_SIZE
                del self._free_space[i]
                break

            # check for free blocks after new free block
            elif (pos + siz + self.PREFIX_SIZE) == free[0]:
                siz = siz + free[1] + self.PREFIX_SIZE
                del self._free_space[i]
                break

        self.fh_data.seek(pos)
        self.fh_data.write((~siz).to_bytes(self.PREFIX_SIZE, "big", signed=True))

        if (
            siz < self.MIN_BLOCKSIZE
        ):  # block is too small, no use to keep checking, add to fragments list, can be used later
            self._fragments.append(pos)
            return

        self._free_space.append((pos, siz))
        return None

    def __enter__(self):
        return self

    def __iter__(self):
        return iter(self._index)

    def close(self, t, v, tr):
        self._commit()
        self.fh_index.flush()
        self.fh_data.flush()
        self.fh_data.close()
        self.fh_index.close()

    __exit__ = close

    def __len__(self) -> int:
        return super().__len__()


class PersistentDict:
    def __init__(self):
        pass
