# ffodbc
Python ODBC library using cffi

The goal of this project is to create an ODBC library that handles Microsoft SQL Server,
unicode and supports Python 3, PyPy3 and SQLAlchemy. It should be very fast at both
selecting and inserting rows, with the ceODBC as the benchmark to beat.

Python 2 will never be supported. Async features may be added in the future.

Warning: under heavy development. The project may change name, structure and could contain
breaking changes at each commit. Use at your own risk.

Tested only on:
- Python 3.5
- PyPy3 5.5 beta
- Microsoft SQL Server 2016 Linux version in a Docker container
- macOS

## Why?
Most Python libraries for Microsoft SQL Server are either not compatible with PyPy
or are very slow:

- pyodbc: not compatible with PyPy, does not use parameter arrays for executes, so is slow
  when inserting many rows. Unicode support is flaky, but a big refactor is about to be
  released for v4, possibly solving those problems.
- pypyodbc: pure python, but also does not use parameter arrays, pretty slow.
- ceODBC: not compatible with PyPy, not available on pypi. But it works very well and is
  very fast.
- pymssql: does not send parameterized queries, but rather compiles statements itself. Is also
  pretty slow doing executemany's.
- python-tds: the most compatible library of them all, but also the slowest, even using PyPy.

So why the need for speed? When doing ETL or data visualization or analysis, you want to
be able to read and write data as fast as possible.

Why only SQL Server? Well, for Postgres, MySQL and Oracle there are already pretty good
libraries available, such as psycopg2 for Postgres, which is really fast.
