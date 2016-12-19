from __future__ import absolute_import

from ffodbc._ffodbc import lib, ffi

from ffodbc.cursor import Cursor


class Connection(object):
    def __init__(self, connstr=None, **kwargs):
        self._henv = lib.Initialize()
        self._hdbc = None
        self._connect(connstr, **kwargs)

    def _connect(self, connstr, **kwargs):
        if connstr is None:
            pairs = [k.upper() + '=' + str(v) for k, v in kwargs.items()]
            connstr = ';'.join(pairs)

        buf = ffi.new('char[]', connstr.encode('utf-16-le'))
        self._hdbc = lib.NewConnection(self._henv, ffi.cast('SQLWCHAR*', buf),
                                       len(connstr))

    def close(self):
        """Close the connection now"""
        if self._hdbc != ffi.NULL:
            lib.CloseConnection(self._henv, self._hdbc)
            self._hdbc = ffi.NULL

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __del__(self):
        self.close()
