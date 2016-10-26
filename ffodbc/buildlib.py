import os

import cffi

ffi = cffi.FFI()


ffi.cdef("typedef void * SQLHANDLE;")
ffi.cdef("typedef SQLHANDLE SQLHENV;")
ffi.cdef("typedef SQLHANDLE SQLHDBC;")
ffi.cdef("typedef SQLHANDLE SQLHSTMT;")

ffi.cdef("typedef void * SQLPOINTER;")
ffi.cdef("typedef signed short int SQLSMALLINT;")
ffi.cdef("typedef unsigned short int SQLUSMALLINT;")
ffi.cdef("typedef signed long int SQLINTEGER;")
ffi.cdef("typedef unsigned char SQLCHAR;")
ffi.cdef("typedef long SQLLEN;")
ffi.cdef("typedef unsigned long SQLULEN;")

ffi.cdef("""typedef struct Column {
  SQLUSMALLINT index;
  SQLCHAR name[255];
  SQLSMALLINT data_type;
  SQLSMALLINT target_type;
  SQLULEN size;
  SQLULEN display_size;
  SQLSMALLINT decimal_digits;
  SQLSMALLINT nullable;
  SQLPOINTER data_array;
  SQLLEN *indicator;
  struct Column *next;
} SQLCOLUMN;
""")

ffi.cdef("typedef enum state {OPENED, CLOSED} cursor_state;")

ffi.cdef("""typedef struct Cursor {
  SQLHSTMT handle;
  cursor_state state;
  SQLULEN arraysize;
  SQLCOLUMN *firstcol;
} SQLCURSOR;
""")

ffi.cdef("""typedef struct Error {
  char state[7];
  int native;
  char text[256];
} ODBCERROR;
""")

ffi.cdef("SQLHENV Initialize();")
ffi.cdef("SQLHDBC NewConnection(char *connstr, SQLHENV env);")
ffi.cdef("void CloseConnection(SQLHENV henv, SQLHDBC hdbc);")

ffi.cdef("void FreeError(ODBCERROR *error);")

ffi.cdef("SQLCURSOR * NewCursor(SQLHDBC hdbc);")
ffi.cdef("int CloseCursor(SQLCURSOR *cursor);")
ffi.cdef("int CursorSetArraysize(SQLHSTMT hstmt, SQLULEN arraysize);")
ffi.cdef("long CursorRowCount(SQLCURSOR *cursor);")
ffi.cdef("int CursorExecDirect(SQLCURSOR *cursor, char *stmt);")
ffi.cdef("int CursorFetch(SQLCURSOR *cursor);")
ffi.cdef("ODBCERROR *ExtractError(SQLHANDLE handle, SQLSMALLINT type);")

loc = os.path.dirname(os.path.abspath(__file__))
code_loc = os.path.join(loc, "ffodbc.c")
ffi.set_source("ffodbc._ffodbc", open(code_loc, "r").read(),
               libraries=["c", "odbc"],
               include_dirs=["/usr/local/include"],
               library_dirs=["/usr/local/lib"])

ffi.compile(verbose=True)
