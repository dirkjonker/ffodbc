#include "ffodbc.h"


// Column contains information about a column
// including the pointer to the array of data
// where the ODBC drivers writes output to
typedef struct Column {
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

typedef enum cursorState {OPENED, CLOSED} cursor_state;

// Cursor contains all attributes of a cursor
typedef struct Cursor {
  SQLHSTMT handle;
  cursor_state state;
  SQLULEN arraysize;
  SQLCOLUMN *firstcol;
} SQLCURSOR;


// Error contains all error information
typedef struct Error {
  char state[7];
  int native;
  char text[256];
} ODBCERROR;


// FreeError cleans up the memory taken by the error
void FreeError(ODBCERROR *error) {
  if (error)
    free(error);
  error = NULL;
}


// ExtractError retrieves diagnostics information from
// the database after an error has occurred.
// Figuring out the type of Error and raising the actual
// Error is handled by the Python code.
ODBCERROR *ExtractError(SQLHANDLE handle, SQLSMALLINT type) {
  SQLSMALLINT len;
  SQLRETURN ret;
  ODBCERROR *error;

  error = (ODBCERROR*)malloc(sizeof(ODBCERROR));

  ret = SQLGetDiagRec(type, handle, 1, (SQLCHAR*)error->state,
                      (SQLINTEGER*)&error->native,
                      (SQLCHAR*)error->text, sizeof(error->text), &len);
  if (SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "[%s:%d] %s\n\n", error->state, error->native, error->text);
  } else {
    free(error);
    error = NULL;
  }
  return error;
}


// tryODBC deals with the return value of any ODBC command
static int tryODBC(SQLRETURN rc, char *f, SQLHANDLE handle, SQLSMALLINT type) {
  if (!SQL_SUCCEEDED(rc)) {
    fprintf(stderr, "The ODBC driver reported an error running %s\n\n", f);
    ExtractError(handle, type);
  }
  return (int)rc;
}


// Initialize sets up the ODBC environment
// and sets the ODBC version to 3
SQLHENV Initialize() {
  SQLHENV henv;

  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  tryODBC(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0),
           "SQLSetEnvAttr", henv, SQL_HANDLE_ENV);
  return henv;
}


// NewConnection makes a connection using an ODBC driver
SQLHDBC NewConnection(SQLHENV henv, char *connstr) {
  SQLHDBC hdbc;

  tryODBC(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc),
           "SQLAllocHandle", henv, SQL_HANDLE_ENV);
  tryODBC(SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connstr, strlen(connstr),
                            NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
           "SQLDriverConnect", hdbc, SQL_HANDLE_DBC);
  return hdbc;
}


// CloseConnection closes a connection and environment
void CloseConnection(SQLHENV henv, SQLHDBC hdbc) {
  tryODBC(SQLDisconnect(hdbc), "SQLDisconnect", hdbc, SQL_HANDLE_DBC);

  tryODBC(SQLFreeHandle(SQL_HANDLE_DBC, hdbc),
          "SQLFreeHandle(dbc)", hdbc, SQL_HANDLE_DBC);

  tryODBC(SQLFreeHandle(SQL_HANDLE_ENV, henv),
          "SQLFreeHandle(env)", henv, SQL_HANDLE_ENV);
}


// free_results clears the cursor for re-use or for closing
// inspired by pyodbc
static void free_results(SQLCURSOR *cursor) {

}


// dealloc_columns frees all memory taken by columns
static void dealloc_columns(SQLCOLUMN *col) {
  SQLCOLUMN *nextcol;

  while (col) {
    nextcol = col->next;
    if (col->data_array) free(col->data_array);
    free(col);
    col = nextcol;
  }
}


// NewCursor creates a statement handle for a database handle
SQLCURSOR * NewCursor(SQLHDBC hdbc) {
  SQLCURSOR *cursor;

  cursor = (SQLCURSOR*)malloc(sizeof(SQLCURSOR));
  cursor->firstcol = NULL;
  cursor->arraysize = 1L;
  cursor->state = CLOSED;

  tryODBC(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &cursor->handle),
           "SQLAllocHandle", hdbc, SQL_HANDLE_DBC);
  tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_CURSOR_TYPE,
                          SQL_CURSOR_FORWARD_ONLY, 0),
           "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);
  return cursor;
}


// CursorSetArraysize sets the size for the array
// that it fetch when calling SQLFetch
int CursorSetArraysize(SQLCURSOR *cursor, SQLULEN arraysize) {
  SQLRETURN ret;

  ret = tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_ROW_ARRAY_SIZE,
                         (SQLPOINTER)arraysize, 0),
           "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);
  cursor->arraysize = arraysize;
  return ret;
}


// CursorRowCount returns the number of rows
// affected by an UPDATE, INSERT, or DELETE statement
long CursorRowCount(SQLCURSOR *cursor) {
  SQLLEN rowcount;

  tryODBC(SQLRowCount(cursor->handle, &rowcount),
          "SQLRowCount", cursor->handle, SQL_HANDLE_STMT);
  return rowcount;
}


// bindCol allocates memory for the driver to output
// column data into
static void bindCol(SQLHSTMT hstmt, SQLULEN arraysize, struct Column *col) {
  SQLSMALLINT target_type;
  SQLLEN alloc_size;
  // char message[100];

  switch (col->data_type) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
    case SQL_UNICODE_CHAR:
    case SQL_UNICODE_VARCHAR:
    case SQL_UNICODE_LONGVARCHAR:
      // sprintf(message, "This is a character thing!\n");
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(char) * col->size + 1;
      break;
    case SQL_SMALLINT:
      // sprintf(message, "Such a small Integer!\n");
      // target_type = SQL_C_SSHORT;
      // alloc_size = sizeof(short int);
      // break;
    case SQL_INTEGER:
      // sprintf(message, "This is a normal Integer!\n");
      // target_type = SQL_C_SLONG;
      // alloc_size = sizeof(long int);
      // break;
    case SQL_BIGINT:
      // sprintf(message, "This is a Biggy Integer!\n");
      // target_type = SQL_C_SBIGINT;
      // alloc_size = sizeof(long long int);
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(char) * col->size + 1;
      break;
    case SQL_REAL:
      // sprintf(message, "Small floaty thingy!\n");
      // target_type = SQL_C_FLOAT;
      // alloc_size = sizeof(float);
    case SQL_FLOAT:
    case SQL_DOUBLE:
      // sprintf(message, "This is a floating point thingy!\n");
      // target_type = SQL_C_DOUBLE;
      // alloc_size = sizeof(double);
      // break;
    case SQL_DECIMAL:
    case SQL_NUMERIC:
      // sprintf(message, "This is a decimal type!\n");
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(char) * col->size + 2;  // 1 extra for the dot
      break;
    case SQL_TYPE_DATE:
    case SQL_DATETIME:
    case SQL_TYPE_TIMESTAMP:
      // sprintf(message, "This is a date or timey type!\n");
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(char) * col->size + 8;
      break;
    default:
      // sprintf(message, "Could not determine type: %d :(\n", col->data_type);
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(char) * col->size + 2;
  }

  // fprintf(stdout, "allocated %ld bytes\n", alloc_size * arraysize);
  col->display_size = alloc_size - 1;

  col->target_type = target_type;
  col->data_array = (SQLPOINTER)malloc(alloc_size * arraysize);
  col->indicator = (SQLLEN*)malloc(sizeof(SQLLEN) * arraysize);
  tryODBC(SQLBindCol(hstmt, col->index, target_type,
                     col->data_array, alloc_size, col->indicator),
          "SQLBindCol", hstmt, SQL_HANDLE_STMT);
}


// checkExecuteResult checks the columns after a SQLExecute
static int checkExecuteResult(SQLCURSOR *cursor) {
  SQLSMALLINT numcols;

  SQLCOLUMN *thiscol, *lastcol = NULL;
  SQLSMALLINT name_len;
  SQLSMALLINT namebuf_size;

  tryODBC(SQLNumResultCols(cursor->handle, &numcols),
          "SQLNumResultCols", cursor->handle, SQL_HANDLE_STMT);

  if (numcols == 0) {
    return -1;
  }

  for (SQLUSMALLINT i=1; i <= numcols; i++) {
    thiscol = (SQLCOLUMN*)(malloc(sizeof(SQLCOLUMN)));
    thiscol->index = i;
    thiscol->next = NULL;
    thiscol->data_array = NULL;
    namebuf_size = sizeof(thiscol->name);

    tryODBC(SQLDescribeCol(cursor->handle, i, (SQLCHAR*)&thiscol->name,
                           namebuf_size,
                           &name_len, &thiscol->data_type,
                           &thiscol->size, &thiscol->decimal_digits,
                           &thiscol->nullable),
            "SQLDescribeCol", cursor->handle, SQL_HANDLE_STMT);

    if (name_len > namebuf_size)
      fprintf(stderr, "Column name for column %d was truncated to %d characters",
             i, namebuf_size);

    bindCol(cursor->handle, cursor->arraysize, thiscol);

    /*printf("%d. name: %s, type: %d, length: %ld, nullable: %d\n", i,
           thiscol->name, thiscol->data_type, thiscol->size,
           thiscol->nullable);*/

    if (i == 1) {
      cursor->firstcol = thiscol;
    } else {
      lastcol->next = thiscol;
    }
    lastcol = thiscol;
  }

  return 0;
}


// CloseCursor does all that is necessary to nicely clean up a cursor
// Python client needs to set cursor reference to ffi.NULL
// for some reason it doesn't work to do that here
int CloseCursor(SQLCURSOR *cursor) {
  SQLRETURN ret;

  if (cursor->firstcol)
    dealloc_columns(cursor->firstcol);

  if (cursor->state == OPENED) {
    tryODBC(SQLCloseCursor(cursor->handle),
            "SQLCloseCursor", cursor->handle, SQL_HANDLE_STMT);
  }

  if (cursor) {
    ret = tryODBC(SQLFreeHandle(SQL_HANDLE_STMT, cursor->handle),
                  "SQLFreeHandle(stmt)", cursor->handle, SQL_HANDLE_STMT);
    free(cursor);
  } else {
    ret = 0;
  }
  return ret;
}


// CursorFetch fetches a result set
int CursorFetch(SQLCURSOR *cursor) {
  SQLRETURN ret;

  if (!cursor) {
    fprintf(stderr, "Calling fetch on a closed cursor!\n");
    return 100;
  }

  if (!(cursor->firstcol)) {
    fprintf(stderr, "No execution! Nothing to fetch!\n");
    return 2;
  }

  ret = SQLFetch(cursor->handle);

  if (ret == SQL_NO_DATA) return 1;

  cursor->state = OPENED;
  return 0;
}


// CursorExecDirect executes a statement on a handle without preparation
int CursorExecDirect(SQLCURSOR *cursor, char *stmt) {
  int err;

  if (!cursor) {
    fprintf(stderr, "Calling fetch on a closed cursor!\n");
    return 100;
  }

  err = tryODBC(SQLExecDirect(cursor->handle, (SQLCHAR*)stmt, strlen(stmt)),
                "SQLExecDirect", cursor->handle, SQL_HANDLE_STMT);
  if (err != 0) {
    return err;
  }
  return checkExecuteResult(cursor);
}
