import os

import cffi

ffi = cffi.FFI()


ffi.cdef("typedef void * SQLHANDLE;")
ffi.cdef("typedef SQLHANDLE SQLHENV;")
ffi.cdef("typedef SQLHANDLE SQLHDBC;")
ffi.cdef("typedef SQLHANDLE SQLHSTMT;")

ffi.cdef("typedef void * SQLPOINTER;")
ffi.cdef("typedef signed short SQLSMALLINT;")
ffi.cdef("typedef unsigned short int SQLUSMALLINT;")
ffi.cdef("typedef int SQLINTEGER;")
ffi.cdef("typedef unsigned int SQLUINTEGER;")
ffi.cdef("typedef double SQLDOUBLE;")
ffi.cdef("typedef unsigned char SQLCHAR;")
ffi.cdef("typedef signed char SQLSCHAR;")
ffi.cdef("typedef long SQLLEN;")
ffi.cdef("typedef unsigned long SQLULEN;")
ffi.cdef("typedef unsigned short WCHAR;")
ffi.cdef("typedef WCHAR SQLWCHAR;")

ffi.cdef("""typedef struct tagDATE_STRUCT {
    SQLSMALLINT    year;
    SQLUSMALLINT   month;
    SQLUSMALLINT   day;
} DATE_STRUCT;
""")

ffi.cdef("""typedef struct tagTIMESTAMP_STRUCT {
    SQLSMALLINT    year;
    SQLUSMALLINT   month;
    SQLUSMALLINT   day;
    SQLUSMALLINT   hour;
    SQLUSMALLINT   minute;
    SQLUSMALLINT   second;
    SQLUINTEGER    fraction;
} TIMESTAMP_STRUCT;
""")

# ffi.cdef("typedef TIMESTAMP_STRUCT SQL_TIMESTAMP_STRUCT;")

ffi.cdef("""typedef struct tagSQL_NUMERIC_STRUCT
{
    SQLCHAR precision;
    SQLSCHAR scale;
    SQLCHAR sign;    /* 1=pos 0=neg */
    SQLCHAR val[16];
} SQL_NUMERIC_STRUCT;
""")

ffi.cdef("""typedef struct Column {
    SQLUSMALLINT index;
    SQLWCHAR name[255];
    SQLSMALLINT name_len;
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
    SQLSMALLINT numcols;
    SQLCOLUMN *firstcol;
    SQLLEN rowcount;
    SQLULEN rows_fetched;
    SQLUSMALLINT *row_status;
} SQLCURSOR;
""")

ffi.cdef("""typedef struct Error {
    char state[7];
    int native;
    char text[256];
} ODBCERROR;
""")

ffi.cdef("SQLHENV initialize();")
ffi.cdef("""
    SQLHDBC create_connection(SQLHENV env, SQLWCHAR *connstr, SQLLEN connstrlen);
""")
ffi.cdef("void close_connection(SQLHENV henv, SQLHDBC hdbc);")

ffi.cdef("void free_error(ODBCERROR *error);")

ffi.cdef("SQLCURSOR * create_cursor(SQLHDBC hdbc);")
ffi.cdef("int close_cursor(SQLCURSOR *cursor);")
ffi.cdef("""
    int cursor_execdirect(SQLCURSOR *cursor, SQLWCHAR *stmt, SQLLEN stmtlen);
""")
ffi.cdef("int cursor_fetch(SQLCURSOR *cursor);")
ffi.cdef("ODBCERROR *extract_error(SQLHANDLE handle, SQLSMALLINT type);")

loc = os.path.dirname(os.path.abspath(__file__))
code_loc = os.path.join(loc, "ffodbc.c")
ffi.set_source("ffodbc._ffodbc", open(code_loc, "r").read(),
               libraries=["c", "odbc"],
               include_dirs=["/usr/local/include"],
               library_dirs=["/usr/local/lib"])

ffi.compile(verbose=True)
