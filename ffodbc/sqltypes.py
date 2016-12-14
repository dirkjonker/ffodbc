import datetime
from decimal import Decimal


TYPEMAP = {
    (-9): str,  # SQL_UNICODE_VARCHAR / SQL_WVARCHAR
    (-8): str,  # SQL_UNICODE_CHAR / SQL_WCHAR
    (-7): bool,  # SQL_BIT
    (-6): int,  # SQL_TINYINT
    (-5): int,  # SQL_BIGINT
    1: str,  # SQL_CHAR
    2: Decimal,  # SQL_NUMERIC
    3: Decimal,  # SQL_DECIMAL
    4: int,  # SQL_INTEGER
    6: float,  # SQL_FLOAT
    7: float,  # SQL_REAL
    8: float,  # SQL_DOUBLE
    9: datetime.datetime,  # SQL_DATETIME
    12: str,  # SQL_VARCHAR
    91: datetime.date,  # SQL_TYPE_DATE
    93: datetime.datetime,  # SQL_TYPE_TIMESTAMP
}
