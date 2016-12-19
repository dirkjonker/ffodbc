#include <stdio.h>
#include <wchar.h>
#include <sql.h>
#include <sqltypes.h>

#include "ffodbc.h"


#define COLNAME_LEN 255

// Column contains information about a column
// including the pointer to the array of data
// where the ODBC drivers writes output to
typedef struct Column {
  SQLUSMALLINT index;
  SQLWCHAR name[COLNAME_LEN];
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

typedef enum cursorState {OPENED, ALLOCATED, CLOSED} cursor_state;

// Cursor contains all attributes of a cursor
typedef struct Cursor {
  SQLHSTMT handle;
  cursor_state state;
  SQLULEN arraysize;
  SQLSMALLINT numcols;
  SQLCOLUMN *firstcol;
  SQLLEN rowcount;
  SQLULEN rows_fetched;
  SQLUSMALLINT *row_status;
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
  // if (SQL_SUCCEEDED(ret)) {
    // fprintf(stderr, "[%s:%d] %s\n\n", error->state, error->native, error->text);
  // } else {
  if (!SQL_SUCCEEDED(ret)) {
    free(error);
    error = NULL;
  }
  return error;
}


// tryODBC deals with the return value of any ODBC command
static int tryODBC(SQLRETURN rc, char *f, SQLHANDLE handle, SQLSMALLINT type) {
  if (rc == SQL_NO_DATA) {
    return (int)rc;
  }
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
SQLHDBC NewConnection(SQLHENV henv, SQLWCHAR *connstr, SQLLEN connstrlen) {
  SQLHDBC hdbc;

  tryODBC(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc),
           "SQLAllocHandle", henv, SQL_HANDLE_ENV);
  tryODBC(SQLDriverConnectW(hdbc, NULL, connstr, connstrlen,
                            NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
           "SQLDriverConnectW", hdbc, SQL_HANDLE_DBC);
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

// free_results clears the cursor for re-use or for closing
static void free_results(SQLCURSOR *cursor) {
  if (cursor->state == OPENED) {
    tryODBC(SQLFreeStmt(cursor->handle, SQL_CLOSE),
            "SQLFreeStmt", cursor->handle, SQL_HANDLE_STMT);
    // tryODBC(SQLFreeStmt(cursor->handle, SQL_UNBIND),
    //         "SQLFreeStmt", cursor->handle, SQL_HANDLE_STMT);

    if (cursor->row_status) {
      free(cursor->row_status);
      cursor->row_status = NULL;
    };

    dealloc_columns(cursor->firstcol);

    cursor->rowcount = -1;
    cursor->state = CLOSED;
  }
}

// NewCursor creates a statement handle for a database handle
SQLCURSOR * NewCursor(SQLHDBC hdbc) {
  SQLCURSOR *cursor;

  cursor = (SQLCURSOR*)malloc(sizeof(SQLCURSOR));
  cursor->firstcol = NULL;
  cursor->arraysize = 1L;
  cursor->rowcount = -1;
  cursor->state = CLOSED;

  tryODBC(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &cursor->handle),
           "SQLAllocHandle", hdbc, SQL_HANDLE_DBC);
  tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_CURSOR_TYPE,
                          SQL_CURSOR_FORWARD_ONLY, 0),
           "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);
  return cursor;
}


// // CursorSetArraysize sets the size for the array
// // that it fetch when calling SQLFetch
// int CursorSetArraysize(SQLCURSOR *cursor, SQLULEN arraysize) {
//   SQLRETURN ret;
//
//   ret = tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_ROW_ARRAY_SIZE,
//                          (SQLPOINTER)arraysize, 0),
//            "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);
//   cursor->arraysize = arraysize;
//   return ret;
// }


// update_cursor_rowcount updates the number of rows
// affected by an UPDATE, INSERT, or DELETE statement
void update_cursor_rowcount(SQLCURSOR *cursor) {
  tryODBC(SQLRowCount(cursor->handle, &cursor->rowcount),
          "SQLRowCount", cursor->handle, SQL_HANDLE_STMT);
}


// set_fetch_attributes sets all the correct attributes
// we need to prepare for some proper fetchin'
static void set_fetch_attributes(SQLCURSOR *cursor) {
  // fprintf(stdout, "Setting arraysize to: %d\n", (int)cursor->arraysize);
  tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_ROW_ARRAY_SIZE,
                         (SQLPOINTER)cursor->arraysize, 0),
          "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);

  // how many rows are actually fetched after SQLFetch is called
  tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_ROWS_FETCHED_PTR,
                         (SQLPOINTER)&cursor->rows_fetched, 0),
          "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);

  cursor->row_status = (SQLUSMALLINT*)malloc(sizeof(SQLUSMALLINT) * cursor->arraysize);
  tryODBC(SQLSetStmtAttr(cursor->handle, SQL_ATTR_ROW_STATUS_PTR,
                         (SQLPOINTER)cursor->row_status, 0),
          "SQLSetStmtAttr", cursor->handle, SQL_HANDLE_STMT);
}


// bindCol allocates memory for the driver to output
// column data into
static void bindCol(SQLHSTMT hstmt, SQLULEN arraysize, struct Column *col) {
  SQLSMALLINT target_type;
  SQLLEN alloc_size, padding;

  padding = 0;

  switch (col->data_type) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(SQLCHAR) * col->size + 1;  // NUL bit
      break;
    case SQL_UNICODE_CHAR:
    case SQL_UNICODE_VARCHAR:
    case SQL_UNICODE_LONGVARCHAR:
      target_type = SQL_C_WCHAR;
      alloc_size = sizeof(SQLWCHAR) * col->size;
      break;
    case SQL_SMALLINT:
      // target_type = SQL_C_SSHORT;
      // alloc_size = sizeof(short int);
      // break;
    case SQL_INTEGER:
      target_type = SQL_C_LONG;
      alloc_size = sizeof(SQLINTEGER);
      break;
    case SQL_BIGINT:
      target_type = SQL_C_SBIGINT;
      alloc_size = sizeof(long long int);
      break;
    case SQL_REAL:
      // target_type = SQL_C_FLOAT;
      // alloc_size = sizeof(float);
    case SQL_FLOAT:
    case SQL_DOUBLE:
      target_type = SQL_C_DOUBLE;
      alloc_size = sizeof(double);
      break;
    case SQL_DECIMAL:
    case SQL_NUMERIC:
      // target_type = SQL_C_NUMERIC;
      // alloc_size = sizeof(SQL_NUMERIC_STRUCT);
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(SQLCHAR) * col->size + 2;  // 1 extra for the dot
      padding = 1;
      break;
    case SQL_TYPE_DATE:
      target_type = SQL_C_TYPE_DATE;
      alloc_size = sizeof(DATE_STRUCT);
      break;
    case SQL_DATETIME:
    case SQL_TYPE_TIMESTAMP:
      target_type = SQL_C_TIMESTAMP;
      alloc_size = sizeof(TIMESTAMP_STRUCT);
      break;
    default:
      target_type = SQL_C_CHAR;
      alloc_size = sizeof(SQLCHAR) * col->size + 1;
  }

  // fprintf(stdout, "allocated %ld bytes\n", alloc_size * arraysize);
  col->display_size = col->size + padding;

  col->target_type = target_type;
  col->data_array = (SQLPOINTER)malloc(alloc_size * arraysize);
  col->indicator = (SQLLEN*)malloc(sizeof(SQLLEN) * arraysize);
  tryODBC(SQLBindCol(hstmt, col->index, target_type,
                     col->data_array, alloc_size, col->indicator),
          "SQLBindCol", hstmt, SQL_HANDLE_STMT);
}


// checkExecuteResult checks the columns after a SQLExecute
static int checkExecuteResult(SQLCURSOR *cursor) {
  SQLCOLUMN *thiscol, *lastcol = NULL;
  SQLSMALLINT namebuf_size;

  tryODBC(SQLNumResultCols(cursor->handle, &cursor->numcols),
          "SQLNumResultCols", cursor->handle, SQL_HANDLE_STMT);

  if (cursor->numcols == 0) {
    return -1;
  }

  for (SQLUSMALLINT i=1; i <= cursor->numcols; i++) {
    thiscol = (SQLCOLUMN*)(malloc(sizeof(SQLCOLUMN)));
    thiscol->index = i;
    thiscol->next = NULL;
    thiscol->data_array = NULL;
    namebuf_size = COLNAME_LEN;

    tryODBC(SQLDescribeColW(cursor->handle, i, (SQLWCHAR*)&thiscol->name,
                            namebuf_size,
                            &thiscol->name_len, &thiscol->data_type,
                            &thiscol->size, &thiscol->decimal_digits,
                            &thiscol->nullable),
            "SQLDescribeColW", cursor->handle, SQL_HANDLE_STMT);

    if (thiscol->name_len > namebuf_size)
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

  free_results(cursor);

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

  // fprintf(stdout, "Arraysize: %d, Rows fetched: %d\n", (int)cursor->arraysize, (int)cursor->rows_fetched);

  if (ret == SQL_NO_DATA) return 1;

  cursor->state = OPENED;
  return 0;
}


// CursorExecDirect executes a statement on a handle without preparation
int CursorExecDirect(SQLCURSOR *cursor, SQLWCHAR *stmt, SQLLEN stmtlen) {
  int err;

  if (!cursor) {
    fprintf(stderr, "Calling fetch on a closed cursor!\n");
    return 100;
  }

  free_results(cursor);

  set_fetch_attributes(cursor);

  err = tryODBC(SQLExecDirectW(cursor->handle, stmt, stmtlen), //strlen((char*)stmt)) / 2,
                "SQLExecDirectW", cursor->handle, SQL_HANDLE_STMT);
  if (err != 0) {
    return err;
  }
  cursor->state = OPENED;
  update_cursor_rowcount(cursor);
  return checkExecuteResult(cursor);
}
