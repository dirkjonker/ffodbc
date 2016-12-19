"""
These tests require a MSSQL database running on localhost
"""

from datetime import date, datetime
from decimal import Decimal

import pytest

import ffodbc
from ffodbc.tools import unmarshal_date

CONNSTR = 'DRIVER=FreeTDS;SERVER=localhost;PORT=1433;UID=sa;PWD=P@55w0rd;DATABASE=test;'


def test_connect():
    conn = ffodbc.connect(CONNSTR)
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    assert cur.fetchone()[0] == 1
    cur.close()
    conn.close()


def test_kwargs_connect():
    conn = ffodbc.connect(driver="FreeTDS", server='localhost',
                          port=1433, db="test", uid="sa", pwd="P@55w0rd")
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    assert cur.fetchone()[0] == 1
    cur.close()
    conn.close()


@pytest.fixture
def connection():
    conn = ffodbc.connect(CONNSTR)
    cur = conn.cursor()
    cur.execute("IF EXISTS (SELECT * FROM sys.tables WHERE name = 'test') DROP TABLE test;")
    cur.execute("CREATE TABLE test (value NVARCHAR(20), date DATE);")
    stmt = '\n'.join("INSERT INTO test (value, date) VALUES (N'Hallo, {}!', '2016-01-28');"
                     .format(i) for i in range(100))
    cur.execute(stmt)
    cur.close()
    yield conn
    cur = conn.cursor()
    cur.execute("DROP TABLE test;")
    cur.close()
    conn.close()


@pytest.fixture
def cursor(connection):
    cur = connection.cursor()
    yield cur
    cur.close()


def test_programming_error(cursor):
    with pytest.raises(ffodbc.exceptions.ProgrammingError):
        cursor.execute("SLECTE * FORM table;")


def test_data_error(cursor):
    with pytest.raises(ffodbc.exceptions.ProgrammingError):
        cursor.execute("SELECT * FROM non_existing_table;")


def test_cursor_description_null(cursor):
    assert cursor.description is None


def test_cursor_description(cursor):
    cursor.execute("""
        SELECT
            'foo' AS col1,
            CAST(NULL AS CHAR(1)) AS col2,
            CAST('42' AS INT) AS col3,
            CAST('3.14' AS DECIMAL(5,2)) AS col4,
            CAST('12345678901234567890.1234567890' AS DECIMAL(38,10)) AS col5,
            CAST('2016-10-26' AS DATE) AS col6,
            CAST('2016-10-26 20:42:17.392' AS DATETIME) AS col7;
    """)
    expected = [
        ("col1", str, 3, 3, None, None, False),
        ("col2", str, 1, 1, None, None, True),
        ("col3", int, 10, 10, None, None, True),
        ("col4", Decimal, 6, 5, 5, 2, True),
        ("col5", Decimal, 39, 38, 38, 10, True),
        ("col6", date, 10, 10, None, None, True),
        ("col7", datetime, 23, 23, None, None, True)
    ]
    expected = [ffodbc.cursor.ColumnDescription(*x) for x in expected]
    assert cursor.description == expected


def test_cursor_array_fetch(cursor):
    """Test proper alignment of SQLFetch with SQL_ATTR_ROW_ARRAY_SIZE > 1."""
    cursor.arraysize = 100
    cursor.execute("SELECT TOP 100 value, date FROM test;")
    rows = cursor.fetchmany(100)
    assert rows[0] == ('Hallo, 0!', date(2016, 1, 28))
    assert rows[50][0] == 'Hallo, 50!'
    assert rows[99][0] == 'Hallo, 99!'
    assert len(rows) == 100


def test_cursor_illegal_arraysize_raises(cursor):
    """Test illegal settings for arraysize."""
    with pytest.raises(ValueError):
        cursor.arraysize = 0
    with pytest.raises(ValueError):
        cursor.arraysize = -1
    with pytest.raises(TypeError):
        cursor.arraysize = "foo"


def test_cursor_fetch(cursor):
    cursor.execute("SELECT 1 AS column1;")
    result = cursor.fetchone()
    assert result[0] == 1


def test_cursor_fetch_2(cursor):
    cursor.execute("SELECT 1, 2, 42")
    result = cursor.fetchone()
    assert result == (1, 2, 42)


def test_cursor_fetch_3(cursor):
    """Test proper string length."""
    cursor.execute("SELECT 'foo' AS a, 'bar' AS b, 'baz' AS c;")
    result = cursor.fetchone()
    assert result == ("foo", "bar", "baz")


def test_cursor_fetch_4(cursor):
    """Test proper string length for normal CHARs."""
    cursor.execute("SELECT CAST('foo' AS CHAR(3)) AS v;")
    result = cursor.fetchone()
    assert result[0] == "foo"


def test_cursor_fetch_date(cursor):
    """Test proper DATE handling."""
    cursor.execute("SELECT CAST('1986-01-28' AS DATE) AS v;")
    result = cursor.fetchone()
    assert result[0] == date(1986, 1, 28)


def test_cursor_fetch_datetime(cursor):
    """Test proper DATETIME handling."""
    cursor.execute("SELECT CAST('2016-12-25 14:42:07' AS DATETIME) AS v;")
    result = cursor.fetchone()
    assert result[0] == datetime(2016, 12, 25, 14, 42, 7)


def test_cursor_fetch_datetime(cursor):
    """Test proper DATETIME handling."""
    cursor.arraysize = 2
    cursor.execute("""
        SELECT CAST('2013-04-18 22:07:42' AS DATETIME) AS v
        UNION ALL
        SELECT CAST('2016-01-28 11:42:07' AS DATETIME) AS v
    """)
    result = cursor.fetchall()
    assert result[0][0] == datetime(2013, 4, 18, 22, 7, 42)
    assert result[1][0] == datetime(2016, 1, 28, 11, 42, 7)


def test_cursor_fetch_datetime2(cursor):
    """Test proper DATETIME2 handling."""
    cursor.execute("SELECT CAST('2016-12-25 14:42:07.123456' AS DATETIME2) AS v;")
    result = cursor.fetchone()
    assert result[0] == datetime(2016, 12, 25, 14, 42, 7, 123456)


def test_cursor_fetch_datetime2_0(cursor):
    """Test proper DATETIME2(0) handling."""
    cursor.execute("SELECT CAST('2016-12-25 14:42:07' AS DATETIME2(0)) AS v;")
    result = cursor.fetchone()
    assert result[0] == datetime(2016, 12, 25, 14, 42, 7)


def test_cursor_fetch_datetime2_3(cursor):
    """Test proper DATETIME2(3) handling."""
    cursor.execute("SELECT CAST('2016-12-25 14:42:07.777' AS DATETIME2(3)) AS v;")
    result = cursor.fetchone()
    assert result[0] == datetime(2016, 12, 25, 14, 42, 7, 777000)


def test_cursor_fetch_decimal(cursor):
    """Test proper DECIMAL/NUMERIC handling."""
    cursor.execute("SELECT CAST('12345.678' AS NUMERIC(8, 3)) AS v;")
    result = cursor.fetchone()
    assert result[0] == Decimal('12345.678')


def test_cursor_fetch_decimal_large(cursor):
    """Test proper huge DECIMAL/NUMERIC handling."""
    cursor.execute("SELECT CAST('1234567890123456789012345678.1234567890' AS NUMERIC(38, 10)) AS v;")
    result = cursor.fetchone()
    assert result[0] == Decimal('1234567890123456789012345678.1234567890')


def test_cursor_fetch_double(cursor):
    """Test proper DECIMAL/NUMERIC handling."""
    cursor.execute("SELECT CAST('12345.678' AS FLOAT) AS v;")
    result = cursor.fetchone()
    assert result[0] == 12345.678


def test_unicode_short(cursor):
    """Test proper unicode fetching."""
    cursor.execute("SELECT CAST(N'Hallo \u24b9\u24d8\u24e1\u24da!' AS NVARCHAR(12)) AS v;")
    result = cursor.fetchone()
    assert result[0] == 'Hallo Ⓓⓘⓡⓚ!'


def test_unicode_emoji(cursor):
    """Test proper unicode fetching for emojis."""
    cursor.execute("SELECT CAST(NCHAR(0xD83D) + NCHAR(0xDE00) AS NVARCHAR(3)) AS v;")
    result = cursor.fetchone()
    assert result[0] == '😀'


def test_calling_closed_cursor(cursor):
    """Calling a function on a closed cursor is a ProgrammingError."""
    cursor.close()
    with pytest.raises(ffodbc.exceptions.ProgrammingError):
        cursor.execute("SELECT 1")


def test_rowcount_insert(cursor):
    """Check updating of rowcount after INSERT."""
    cursor.execute("INSERT test (value) VALUES ('test');")
    assert cursor.rowcount == 1


def test_rowcount_update(cursor):
    """Check updating of rowcount after UPDATE."""
    cursor.execute("UPDATE test SET value = N'foo' WHERE value LIKE N'Hallo, 9[0-9]!';")
    assert cursor.rowcount == 10


def test_rowcount_delete(cursor):
    """Check updating of rowcount after DELETE."""
    cursor.execute("DELETE TOP (7) FROM test;")
    assert cursor.rowcount == 7


def test_cursor_reuse(cursor):
    """Cursor re-use without closing should not be a problem."""
    cursor.execute("SELECT TOP 20 value FROM test;")
    assert cursor.fetchone()[0] == 'Hallo, 0!'
    # run a new query even though it has an active set
    cursor.execute("SELECT TOP 20 value FROM test ORDER BY 1 DESC;")
    assert cursor.fetchone()[0] == 'Hallo, 99!'


def test_cursor_underalloc(cursor):
    """Test underallocating the arraysize and fetchmany does not cause errors."""
    cursor.arraysize = 8
    cursor.execute("SELECT TOP 10 value FROM test")
    rows = cursor.fetchall()
    assert len(rows) == 10
    assert rows[0][0] == 'Hallo, 0!'
    assert rows[-1][0] == 'Hallo, 9!'


def test_cursor_overalloc(cursor):
    """Test overallocating the arraysize and fetchmany does not cause errors."""
    cursor.arraysize = 20
    cursor.execute("SELECT TOP 10 value FROM test")
    rows = cursor.fetchmany(20)
    assert len(rows) == 10
    assert rows[0][0] == 'Hallo, 0!'
    assert rows[-1][0] == 'Hallo, 9!'
