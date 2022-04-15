import os
from functools import partial
from mmap import ACCESS_READ, mmap
from pathlib import Path
from struct import Struct, unpack_from
from sys import byteorder
from time import perf_counter
from typing import Dict, List, Literal, Mapping, MutableMapping, Tuple

from msgspec.msgpack import Decoder, Encoder

base_flags = os.O_CREAT | os.O_RDWR | os.O_BINARY


def lenb(bytes: bytes):
    return len(bytes).to_bytes(4, byteorder, signed=False)


class _BaseChestDB:

    BUFFER_SIZE = 0x200
    PREFIX_SIZE = 0x04

    def __init__(self, path: Path):

        if not isinstance(path, Path):
            raise TypeError("path is not an instance of pathlib.Path")

        # _indexfile contains the indexes with their respective locations in the _datafile,
        # _datafile contains all the data in binary format pointed to by _indexfile.
        self._indexfile: Path = path
        self._datafile: Path = path.with_suffix(".bin")

        # The index is an in-memory dict, mirroring the index file.
        # It maps indexes to their position in the datafile
        self._index: Dict[str | bytes | int, int] = dict()


class _RO_ChestDatabase(_BaseChestDB, Mapping):
    def __init__(self, path: Path):
        super().__init__(path)

        if not self._indexfile.exists():
            raise FileNotFoundError(
                f"""File can't be found, use access_mode='r+' if you wan to create it.\nPath: <{self._indexfile.absolute()}>,
                    """
            )
        # initializing a packer and unpacker wich will be used for the index.
        self._row_struct = Struct("II")
        self._row_unpack = self._row_struct.iter_unpack

        self._indexfile = self._indexfile.open("rb", buffering=0)
        self._datafile = self._datafile.open("rb", buffering=0)

        self.fh_index = mmap(self._indexfile.fileno(), 0, access=ACCESS_READ)
        self.fh_data = mmap(self._datafile.fileno(), 0, access=ACCESS_READ)

        self._index = dict(self._row_unpack(self.fh_index.read()))

    def __getitem__(self, key: int | str | bytes) -> bytes:

        pos = self._index[key]  # may raise KeyError
        self.fh_data.seek(pos)
        siz = unpack_from('I', self.fh_data[:4])[0]

        return self.fh_data[pos + 4:pos + siz + 4]


    def __enter__(self):
        return self

    def __iter__(self):
        return iter(self._index)

    def close(self, t, v, tr):
        self._indexfile.close()
        self._datafile.close()
        self.fh_data.close()
        self.fh_index.close()

    __exit__ = close

    def __len__(self) -> int:
        return super().__len__()


class _RW_ChestDatabase(_BaseChestDB, MutableMapping):
    PREFIX_SIZE = 0x04
    BUFFER_SIZE = 0x200
    MIN_BLOCKSIZE = 0x10
    FIXMAP = 0x80
    MAP_16 = 0xDE
    MAP_32 = 0xDF

    def __init__(self, path: Path, mode: Literal["r", "r+"] = "r"):
        super().__init__(path)
        # free_blocks keep track of the free blocks in the file.
        # These free bocks might be reused later on.
        # fragments are blocks that can't be reused, (too small).
        self._free_space: List[Tuple[int, int]] = []
        self._fragments: List[Tuple[int, int]] = []

        # initializing a packer and unpacker wich will be used for the index.
        self._row_struct = Struct("II")
        self._row_pack = self._row_struct.pack
        self._row_unpack = self._row_struct.iter_unpack
        self._buffer = bytearray()

        self._indexfile.touch(exist_ok=True)
        self._datafile.touch(exist_ok=True)

        self.fh_index = self._indexfile.open('rb+', buffering=0)
        self.fh_data = self._datafile.open('rb+', buffering=0)
        self._buffer = bytearray(self.fh_index.read())
        self._index = dict(self._row_unpack(self._buffer))

        self._populate_free_blocks()

    def _populate_free_blocks(self):
        self.fh_data.seek(0)

        while len((block := self.fh_data.read(4))) == 4:

            size = int.from_bytes(block, byteorder, signed=True)

            if size < 0:
                size = ~size
                pos = self.fh_data.tell()
                # todo fix redundant shizzle
                self._setfree(pos - 4, size)
            self.fh_data.seek(size, 1)

    def commit(self, cleanup=False):
        if cleanup:
            for i,k in enumerate(self._index):
                self._buffer[i*8:] = self._row_pack(k,self._index[k])

        self.fh_index.seek(0)
        self.fh_index.write(self._buffer)
        if cleanup:
            self.fh_index.truncate(len(self._index)*8)

    def _addval(self, key, val: bytes):
        self.fh_data.seek(0, 2)

        pos = self.fh_data.tell()
        self.fh_data.write(lenb(val) + val)
        self._buffer.extend(self._row_pack(key,pos))
        self._index[key] = pos

    def _setval(self, key, val: bytes, pos: int):
        self.fh_data.seek(pos)
        self.fh_data.write(lenb(val) + val)

        self._buffer.extend(self._row_pack(key,pos))
        self._index[key] = pos


    def __getitem__(self, key: int | str | bytes) -> bytes:

        pos = self._index[key]  # may raise KeyError

        self.fh_data.seek(pos)
        dat = self.fh_data.read(self.BUFFER_SIZE)

        siz = int.from_bytes(dat[: self.PREFIX_SIZE], byteorder, signed=True)

        if siz > (x := (self.BUFFER_SIZE - self.PREFIX_SIZE)):
            dat += self.fh_data.read(siz - x)

        dat = dat[self.PREFIX_SIZE : siz + self.PREFIX_SIZE]
        return dat

    def __delitem__(self, key) -> None:
        pos = self._index.pop(key)
        self.fh_data.seek(pos)

        siz = int.from_bytes(self.fh_data.read(self.PREFIX_SIZE), byteorder, signed=True)
        self._setfree(pos, siz)

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
                self.fh_data.read(self.PREFIX_SIZE), byteorder, signed=False
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
        self.fh_data.write((~siz).to_bytes(self.PREFIX_SIZE, byteorder, signed=True))

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
        self.commit(cleanup=True)
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


def ChestDatabase(path: Path, mode: Literal["r", "r+"] = None):
    if not mode in {"r", "r+"}:
        raise AttributeError(f'access_mode is not one of ("r", "r+"), :{mode}')
    return _RO_ChestDatabase(path) if mode == "r" else _RW_ChestDatabase(path)
