from ffodbc.connection import Connection

# Python DBAPI 2.0 globals
apilevel = '2.0'
threadsafety = 0
paramstyle = 'qmark'


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

__all__ = [connect]
