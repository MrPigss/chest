from _thread import get_ident, allocate_lock
from time import sleep


class TurnLock:
    def __init__(self):
        self._block = allocate_lock()
        self._owner = None
        self._releaser = None

    def acquire(self):
        me = get_ident()
        if self._owner == me:
            return 1

        while self._releaser == me:
            sleep(0)

        self._block.acquire()
        self._owner = me
        return 1

    __enter__ = acquire

    def release(self):
        self._releaser = self._owner
        self._owner = None

        self._block.release()

    def __exit__(self, t, v, tb):
        self.release()
