import datetime

from ffodbc._ffodbc import ffi, lib
from ffodbc.exceptions import ProgrammingError, DataError, DatabaseError


def unmarshal_datetime(val):
    """This function is slow! Should handle conversion in C instead."""
    return datetime.datetime(val.year, val.month, val.day,
                             val.hour, val.minute, val.second,
                             val.fraction // 1000)


def unmarshal_date(val):
    """This function is slow! Should handle conversion in C instead."""
    return datetime.date(val.year, val.month, val.day)


def _raise_error(error):
    state = ffi.string(error.state).decode('utf-8')
    status_code = state[:2]
    message = '[{}] {}'.format(state, ffi.string(error.text).decode('utf-8'))

    # free the memory before raising any error
    lib.FreeError(error)
    error = ffi.NULL

    if status_code == '42':
        raise ProgrammingError(message)
    if status_code == '22':
        raise DataError(message)
    raise DatabaseError(message)
