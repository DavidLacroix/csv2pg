import io


class StringIteratorIO(io.TextIOBase):
    """
    from: https://gist.github.com/anacrolix/3788413
    """

    def __init__(self, iter):
        self._iter = iter
        self._left = ""

    def readable(self):
        return True

    def _read1(self, n=None):
        while not self._left:
            try:
                self._left = next(self._iter)
            except StopIteration:
                break
        ret = self._left[:n]
        self._left = self._left[len(ret) :]
        return ret

    def read(self, n=None):
        _list = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                _list.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                _list.append(m)
        return "".join(_list)

    def readline(self):
        _list = []
        while True:
            i = self._left.find("\n")
            if i == -1:
                _list.append(self._left)
                try:
                    self._left = next(self._iter)
                except StopIteration:
                    self._left = ""
                    break
            else:
                _list.append(self._left[: i + 1])
                self._left = self._left[i + 1 :]
                break
        return "".join(_list)
