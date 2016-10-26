from __future__ import absolute_import

from ffodbc._ffodbc import ffi, lib
from ffodbc.exceptions import ProgrammingError, DataError, DatabaseError


def _raise_error(error):
    state = ffi.string(error.state)
    status_code = state[:2]
    message = '[{}] {}'.format(state, ffi.string(error.text))

    # free the memory before raising any error
    lib.FreeError(error)
    error = ffi.NULL

    if status_code == '42':
        raise ProgrammingError(message)
    if status_code == '22':
        raise DataError(message)
    raise DatabaseError(message)
