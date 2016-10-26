"""
These tests require a MySQL database running on localhost
"""

from decimal import Decimal

import pytest

import ffodbc
from ffodbc.sqltypes import unmarshal_date

CONNSTR = "DRIVER=MySQL;DATABASE=dirk;UID=dirk;PWD=dirk;"


def test_connect():
    conn = ffodbc.connect(CONNSTR)
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    cur.fetchone()


def test_kwargs_connect():
    ffodbc.connect(driver="MySQL", db="dirk", uid="dirk", pwd="dirk")


@pytest.fixture(scope='module')
def connection():
    conn = ffodbc.connect(CONNSTR)
    yield conn
    conn.close()


@pytest.fixture
def cursor(connection):
    cur = connection.cursor()
    yield cur
    cur.close()


def test_information_schema(cursor):
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES LIMIT 3")


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
            NULL AS col2,
            CAST('42' AS INT) AS col3,
            CAST('3.14' AS DECIMAL(5,2)) AS col4,
            CAST('2016-10-26' AS DATE) AS col5;
    """)
    expected = [
        ("col1", str, 3, 3, None, None, False),
        ("col2", str, 0, 0, None, None, True),
        ("col3", int, 19, 19, None, None, False),
        ("col4", Decimal, 6, 5, 5, 2, False),
        ("col5", unmarshal_date, 17, 10, None, None, True)
    ]
    assert cursor.description == expected


def test_cursor_arraysize(cursor):
    cursor.arraysize = 100
    cursor.execute("SELECT * FROM test LIMIT 100;")
    rows = cursor.fetchmany()
    assert len(rows) == 100


def test_cursor_illegal_arraysize_raises(cursor):
    with pytest.raises(ValueError):
        cursor.arraysize = 0
    with pytest.raises(ValueError):
        cursor.arraysize = -1
    with pytest.raises(ValueError):
        cursor.arraysize = "foo"


def test_cursor_fetch(cursor):
    cursor.execute("SELECT 1 AS column1;")
    result = cursor.fetchone()
    assert result == (1,)


def test_cursor_fetch_2(cursor):
    cursor.execute("SELECT 1, 2, 42")
    result = cursor.fetchone()
    assert result == (1, 2, 42)


def test_cursor_fetch_3(cursor):
    cursor.execute("SELECT 'foo', 'bar', 'baz'")
    result = cursor.fetchone()
    assert result == ("foo", "bar", "baz")


def test_calling_closed_cursor(cursor):
    cursor.close()
    with pytest.raises(ffodbc.exceptions.ProgrammingError):
        cursor.execute("SELECT 1")


def test_rowcount_select(cursor):
    cursor.execute("SELECT 1 UNION ALL SELECT 2")
    assert cursor.rowcount == 2


def test_rowcount_insert(cursor):
    cursor.execute("INSERT INTO test (value) VALUES ('test');")
    assert cursor.rowcount == 1
