import datetime
from collections import namedtuple
from decimal import Decimal

from ffodbc._ffodbc import lib, ffi
from ffodbc.exceptions import ProgrammingError
from ffodbc.sqltypes import TYPEMAP
from ffodbc.tools import _raise_error, unmarshal_date, unmarshal_datetime


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
        self._rowptr = 0

        self.description = None

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def _call(self, ret):
        """Call an ODBC function and handle errors."""
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
        if not isinstance(value, int):
            raise TypeError('Arraysize must be type int > 0')
        if value <= 0:
            raise ValueError('Arraysize must be > 0')
        self._call(lib.CursorSetArraysize(self._cursor, value))
        self._arraysize = value

    def callproc(self, procname, parameters):
        """Call a stored database procedure with the given name."""
        return self

    def close(self):
        """Close the cursor now."""
        if self._opened:
            self._call(lib.CloseCursor(self._cursor))
            self._cursor = ffi.NULL
            self._opened = False

    def _set_description(self):
        description = []
        col = self._cursor.firstcol
        while col:
            colname = bytes(ffi.buffer(col.name))[:col.name_len * 2].decode('utf-16-le')
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
        """Execute a statement."""
        c_stmt = ffi.new('char[]', operation.encode('utf-16-le'))
        if parameters is None:
            self._call(lib.CursorExecDirect(self._cursor, ffi.cast('SQLWCHAR*', c_stmt), len(operation)))
        else:
            raise NotImplementedError("Parameterized queries not yet supported")
        self._set_description()
        return self

    def executemany(self, operation, seq_of_parameters):
        """Execute a statement with a sequence of parameters."""
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        return self

    def fetchone(self):
        """Fetch a single result row from the cursor.

        A "row pointer" points to the current row
        of a rowset that is already fetched in memory. As soon as
        we deplete this buffer we reset the array pointer to 0 which
        will trigger a new call to SQLFetch when requesting a new row.
        """
        if self._rowptr == 0:
            ret = self._call(lib.CursorFetch(self._cursor))
            if ret == 1:
                return
        row = []
        col = self._cursor.firstcol
        for d in self.description:
            if col.indicator[self._rowptr] == -1:
                row.append(None)
                col = col.next
                continue
            if d.type_code is int:
                raw = ffi.cast('SQLINTEGER*', col.data_array)
                row.append(raw[self._rowptr])
                col = col.next
                continue
            if d.type_code is float:
                raw = ffi.cast('double*', col.data_array)
                row.append(raw[self._rowptr])
                col = col.next
                continue
            if d.type_code is datetime.date:
                raw = ffi.cast('DATE_STRUCT*', col.data_array)
                row.append(unmarshal_date(raw[self._rowptr]))
                col = col.next
                continue
            if d.type_code is datetime.datetime:
                raw = ffi.cast('TIMESTAMP_STRUCT*', col.data_array)
                row.append(unmarshal_datetime(raw[self._rowptr]))
                col = col.next
                continue
            # if d.type_code is Decimal:
            #     raw = ffi.cast('SQL_NUMERIC_STRUCT*', col.data_array)
            #     val = ffi.string(raw.val)
            #     print(val)
            #     row.append(Decimal((raw.sign, (int(val, 16),), raw.scale)))
            #     col = col.next
            #     continue
            if d.type_code is str and col.data_type < 0:  # unicode
                dsize = d.display_size * 2
                uc = True
            else:
                dsize = d.display_size + 1  # char[] with nul terminator
                uc = False
            total_size = dsize * self._arraysize
            start = self._rowptr * dsize
            end = start + col.indicator[self._rowptr]
            raw = ffi.buffer(col.data_array, total_size)[start:end]
            if uc:
                val = raw.decode('utf-16-le')
            else:
                val = d.type_code(raw.decode('utf-8'))
            row.append(val)
            col = col.next
        self._rowptr += 1
        if self._rowptr >= self._arraysize:
            self._rowptr = 0
        return tuple(row)

    def fetchmany(self, size=None):
        """Fetch many result rows from the cursor."""
        if size is None:
            size = self._arraysize
        rows = []
        for i in range(size):
            rows.append(self.fetchone())
        return rows

    def fetchall(self):
        """Fetch all result rows from the cursor."""
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
