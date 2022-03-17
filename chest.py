from pathlib import Path
from typing import Dict, Literal, MutableMapping, Tuple

from msgspec.msgpack import Decoder, Encoder


class ChestDatabase(MutableMapping):
    FIXMAP = 0x80
    MAP_16 = 0xDE
    MAP_32 = 0xDF

    def __init__(self, path: Path, mode: Literal["r", "r+"] = "r"):

        # _indexfile contains the indexes with their respective locations in the _datafile,
        # _datafile contains all the data in binary format pointed to by _indexfile.
        self._indexfile: Path = path
        self._datafile: Path = path.with_suffix(".bin")

        self._mode = mode

        # The index is an in-memory dict, mirroring the index file.
        self._index: dict | None = None  # maps keys to (pos, siz) pairs
        self._free_blocks = []
        self._modified = False

        self._decoder = Decoder(Dict[str | bytes, Tuple[int, int]])
        self._encoder = Encoder()

        self._check()

        self.fh_index = self._indexfile.open("rb+", buffering=0)
        self.fh_data = self._datafile.open("rb+", buffering=0)

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
                    f"""File can't be found, use access_mode='r+' if you wan to create it.
                        Path: <{self._indexfile.absolute()}>,
                        """
                )
            # generate the directory if it doesn't exist
            self._indexfile.parent.mkdir(parents=True, exist_ok=True)

            # create all the files if they don't already exist
            self._indexfile.touch()
            self._datafile.touch(exist_ok=True)

        if not self._indexfile.is_file():
            raise FileNotFoundError(
                f"""path does not lead to a file: <{self._indexfile.absolute()}>."""
            )

    def _load(self):
        # ?: needs a way to populate free_blocks

        _decode = self._decoder.decode
        index = dict()

        try:
            self.fh_index.seek(0)
            index = _decode(self.fh_index.read())
        except Exception as e:
            pass

        self._index = index if index else None

    def _commit(self):
        # ? should commit be copy on write?

        _encode = self._encoder.encode
        if self._index is None or not self._modified:
            return

        self.fh_index.seek(0)
        self.fh_index.write(_encode(self._index))

    def _is__open(self):
        # todo implement this somewhere
        return self._index is None

    def _addval(self, key, val):
        # ? commit after every change or not, maybe use buffer?
        self._modified = True

        self.fh_data.seek(0, 2)
        pos = int(self.fh_data.tell())
        size = self.fh_data.write(val)

        self._index[key] = (pos, size)
        # self._commit()

    def _setval(self, key, val, pos):
        # ? see comment on _addval
        self._modified = True

        self.fh_data.seek(pos)
        size = self.fh_data.write(val)

        self._index[key] = (pos, size)
        # self._commit()

    def __setitem__(self, key, val):
        # todo fix better persistance algorithm
        # ? instead of all the isinstance check, use try catch -> better to ask forgiveness than permission
        if not self._mode == "r+":
            raise PermissionError("Storage is _openend as read only")

        if isinstance(key, str):
            key = key.encode("utf-8")

        if not isinstance(key, (bytes, bytearray)):
            raise TypeError("keys must be bytes or strings")

        if isinstance(val, str):
            val = val.encode("utf-8")

        if not isinstance(val, (bytes, bytearray)):
            raise TypeError("values must be bytes or strings")

        new_length = len(val)

        if not self._index:
            self._index = dict()

        if key in (index := self._index):
            pos, size = index[key]

            if new_length <= size:
                self._setval(key, val, pos)
                return

        for i, block in enumerate(self._free_blocks):
            block_pos = block[0]
            block_size = block[1]

            if new_length == block_size:
                self._setval(key, val, block_pos)
                del self._free_blocks[i]
                return

            elif new_length < block_size:
                self._setval(key, val, block_pos)
                self._free_blocks[i] = (block_pos + new_length, block_size - new_length)
                return

        self._addval(key, val)

    def close(self, exc_type, exc_value, exc_traceback):
        # ? actually close everything
        self._commit()
        self.fh_index.flush()
        self.fh_data.flush()
        self.fh_data.close()
        self.fh_index.close()

    def __getitem__(self, key):
        # todo handle the errors
        if isinstance(key, str):
            key = key.encode("utf-8")

        pos, siz = self._index[key]  # may raise KeyError

        self.fh_data.seek(pos)
        dat = self.fh_data.read(siz)
        return dat

    def __enter__(self):
        return self

    def __iter__(self):
        return iter(self._index) if self._index != None else iter([])

    def __delitem__(self, key) -> None:
        if not self._mode == "r+":
            raise PermissionError("Storage is _openend as read only")

        # ? forgiveness > permisson
        if isinstance(key, str):
            key = key.encode("utf-8")

        self._modified = True

        self._setfree(*self._index.pop(key))

        self._commit()

    __exit__ = close

    def __len__(self) -> int:
        # * really isn't usefull yet
        if self._index == None or len(self._index) == 0:
            return 0
        try:
            self.self.fh_index.seek(0)
            with memoryview(self.fh_index.read(5)) as view:
                match view[0]:
                    case x if (x & 0b11110000) == self.FIXMAP:
                        return x & 0b00001111
                    case self.MAP_16:
                        return int.from_bytes(view[1:3], "big")
                        # return int.from_bytes(f.read(2), "big", signed=False)
                    case self.MAP_32:
                        return int.from_bytes(view[1:5], "big")
                        # return int.from_bytes(f.read(4), "big", signed=False)
                    case _:
                        return 0
        except IndexError:
            return 0

    def _setfree(self, start, length):
        # todo test this better, sorting, performance, ...
        for s, l in self._free_blocks:
            if (s + l + 1) == start:
                self._free_blocks.append((s, l + length))
                return
        self._free_blocks.append((start, length))
        self._free_blocks = sorted(self._free_blocks, key=lambda t: t[1])


# todo create transaction logic
class Transaction:
    def __init__(self, database: ChestDatabase):
        self._db = database

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._db._commit()
