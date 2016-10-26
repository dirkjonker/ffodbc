from __future__ import absolute_import

from collections import namedtuple
from decimal import Decimal

from ffodbc._ffodbc import lib, ffi
from ffodbc.exceptions import ProgrammingError
from ffodbc.sqltypes import TYPEMAP
from ffodbc.tools import _raise_error


ColumnDescription = namedtuple('ColumnDescription', [
    'name', 'type_code', 'display_size',
    'internal_size', 'precision', 'scale', 'null_ok'
])


class Cursor(object):
    def __init__(self, connection):
        self._connection = connection
        self._cursor = lib.NewCursor(self._connection._hdbc)
        self._opened = True

        self._arraysize = 1
        self._arrayptr = 0

        self.description = None
        self.rowcount = -1

    def _call(self, ret):
        """Call an ODBC function and handle errors"""
        if self._opened is False:
            raise ProgrammingError("Calling on a closed cursor")
        if ret not in (0, 1):
            error = lib.ExtractError(self._cursor.handle, 3)
            if error:
                _raise_error(error)
        return ret

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if not isinstance(value, int) or value <= 0:
            raise ValueError('Arraysize must be an integer > 0')
        self._call(lib.CursorSetArraysize(self._cursor, value))
        self._arraysize = value

    def callproc(self, procname, parameters):
        """Call a stored database procedure with the given name"""
        return self

    def close(self):
        """Close the cursor now"""
        if self._opened:
            self._call(lib.CloseCursor(self._cursor))
            self._cursor = ffi.NULL
            self._opened = False

    def _set_description(self):
        description = []
        col = self._cursor.firstcol
        while col:
            colname = ffi.string(col.name).decode('utf-8')
            ctype = TYPEMAP.get(col.data_type, str)
            if ctype is Decimal:
                precision = col.size
                scale = col.decimal_digits
            else:
                precision = scale = None
            d = ColumnDescription(colname, ctype, col.display_size, col.size,
                                  precision, scale, bool(col.nullable))
            description.append(d)
            col = col.next
        self.description = description

    def execute(self, operation, parameters=None):
        """Execute a statement"""
        b_stmt = operation.encode('utf-8')
        if parameters is None:
            self._call(lib.CursorExecDirect(self._cursor, b_stmt))
        else:
            raise NotImplementedError("Only SELECT queries are supported")
        self._set_description()
        self.rowcount = lib.CursorRowCount(self._cursor)
        return self

    def executemany(self, operation, seq_of_parameters):
        """Execute a statement with a sequence of parameters"""
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        return self

    def fetchone(self):
        """Fetch a single result row from the cursor

        For now we just tell the ODBC driver to return all values
        as character arrays and we let Python convert it to the correct
        data type. If you use PyPy these type conversions will magically
        be super fast. Using CPython this will be dramatically slow for
        large result sets.

        This can be improved for CPython, but you are better off with
        a library like ceODBC if you need CPython and want something fast.

        We use an "array pointer" that points to the current row
        of a rowset that is already fetched in memory. As soon as
        we deplete this buffer we reset the array pointer to 0 which
        will trigger a new call to SQLFetch when requesting a new row.
        """
        if self._arrayptr == 0:
            ret = self._call(lib.CursorFetch(self._cursor))
            if ret == 1:
                return
        row = []
        col = self._cursor.firstcol
        for d in self.description:
            total_size = (d.display_size + 1) * self._arraysize
            start = self._arrayptr * (d.display_size + 1)
            end = start + col.indicator[self._arrayptr]
            raw = ffi.buffer(col.data_array, total_size)[start:end]
            val = d.type_code(raw)
            row.append(val)
            col = col.next
        self._arrayptr += 1
        if self._arrayptr >= self._arraysize:
            self._arrayptr = 0
        return tuple(row)

    def fetchmany(self, size=None):
        """Fetch many result rows from the cursor"""
        if size is None:
            size = self._arraysize
        rows = []
        for i in range(size):
            rows.append(self.fetchone())
        return rows

    def fetchall(self):
        """Fetch all result rows from the cursor"""
        rows = []
        result = self.fetchone()
        while result:
            rows.append(result)
            result = self.fetchone()
        return rows

    def nextset(self):
        pass

    def prepare(self, stmt):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size):
        pass

    def __del__(self):
        self.close()
