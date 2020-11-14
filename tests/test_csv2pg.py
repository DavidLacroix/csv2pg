import os

import psycopg2
import pytest

from csv2pg import copy_to


HOST = "localhost"
PORT = 25432
DBNAME = "test"
USER = "test"
PASSWORD = "test"

os.putenv("PGPASSWORD", PASSWORD)
DSN = "host={host} port={port} dbname={dbname} user={user}".format(
    host=HOST,
    port=PORT,
    dbname=DBNAME,
    user=USER,
)


def test_db_connection():
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT 1")
            res = curs.fetchone()
            assert res[0] == 1


def test_insert():
    tablename = "insert"
    asset = "tests/assets/simple.csv"

    copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is not None


def test_append():
    tablename = "append"
    asset = "tests/assets/simple.csv"

    copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset)
    copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 20


def test_overwrite():
    tablename = "overwrite"
    asset = "tests/assets/simple.csv"

    copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset)
    copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset, overwrite=True)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10


def test_delimiter():
    tablename = "delimiter"
    asset = "tests/assets/delimiter.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter="@",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is not None


def test_quotechar():
    tablename = "quotechar"
    asset = "tests/assets/quotechar.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter="|",
        quotechar="@",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is not None


def test_escapechar():
    tablename = "escapechar"
    asset = "tests/assets/escapechar.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter="\t",
        quotechar='"',
        escapechar="\\",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is not None


def test_doublequote_escape():
    tablename = "doublequote_escape"
    asset = "tests/assets/doublequote_escape.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter="\t",
        quotechar='"',
        doublequote=True,
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is not None


def test_no_header():
    tablename = "no_header"
    asset = "tests/assets/no_header.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter="@",
        header=False,
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is not None


def test_nulls():
    tablename = "nulls"
    asset = "tests/assets/nulls.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter=":",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for col in row:
                    assert col is None


def test_nulls_custom():
    tablename = "nulls_custom"
    asset = "tests/assets/nulls_custom.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter=":",
        null="NULLZZ",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 3
                for i, col in enumerate(row):
                    if i == 1:
                        assert col == ""
                    else:
                        assert col is None


def test_single_column():
    tablename = "single_column"
    asset = "tests/assets/single_column.csv"

    copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset, overwrite=True)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for row in rows:
                assert len(row) == 1
                for col in row:
                    assert col is not None


def test_encoding_iso_8859_1():
    tablename = "encoding_iso_8859_1"
    asset = "tests/assets/encoding_ISO_8859_1.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        encoding="ISO-8859-1",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "ÑÆÛý¼àáäæ"


def test_encoding_GB18030():
    tablename = "encoding_GB18030"
    asset = "tests/assets/encoding_GB18030.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        encoding="GB18030",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "气广"


def test_complex():
    tablename = "complex"
    asset = "tests/assets/complex_LE-LF_header_bom.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        lineterminator="\n",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1000
            for row in rows:
                assert len(row) == 19


def test_rownum():
    tablename = "rownum"
    asset = "tests/assets/simple.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        inject_rownum=True,
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for i, row in enumerate(rows):
                assert len(row) == 4
                assert int(row[0]) == (i + 1)
                for col in row:
                    assert col is not None


def test_rownum_no_header():
    tablename = "rownum_no_header"
    asset = "tests/assets/no_header.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        delimiter="@",
        header=False,
        inject_rownum=True,
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for i, row in enumerate(rows):
                assert len(row) == 4
                assert int(row[0]) == (i + 1)
                for col in row:
                    assert col is not None


def test_rownum_encoding_iso_8859_1():
    tablename = "rownum_encoding_iso_8859_1"
    asset = "tests/assets/encoding_ISO_8859_1.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        inject_rownum=True,
        encoding="ISO-8859-1",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1
            assert rows[0][1] == "ÑÆÛý¼àáäæ"


def test_rownum_encoding_GB18030():
    tablename = "rownum_encoding_GB18030"
    asset = "tests/assets/encoding_GB18030.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        inject_rownum=True,
        encoding="GB18030",
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1
            assert rows[0][1] == "气广"


def test_error_delimiter():
    tablename = "error_delimiter"
    asset = "tests/assets/error_delimiter.csv"

    with pytest.raises(psycopg2.errors.BadCopyFileFormat):
        copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 0


def test_error_unterminated_quote():
    tablename = "error_unterminated_quote"
    asset = "tests/assets/error_unterminated_quote.csv"

    with pytest.raises(psycopg2.errors.BadCopyFileFormat):
        copy_to(HOST, PORT, DBNAME, USER, PASSWORD, tablename, asset)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 0


def test_check_delimiter():
    tablename = "with_delimiter_error"
    asset = "tests/assets/error_delimiter.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        skip_error=True,
    )

    with open(asset + ".err") as f:
        errors = f.readlines()
        assert len(errors) == 3
        assert errors[1].startswith("2")
        assert errors[1].split(",")[1].startswith("MissingFieldsException")
        assert errors[2].startswith("5")
        assert errors[2].split(",")[1].startswith("TooManyFieldsException")

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 8
            assert 2 not in [a for a, b, c in rows]
            assert 5 not in [a for a, b, c in rows]


def test_check_unterminated_quote():
    tablename = "with_quote_error"
    asset = "tests/assets/error_unterminated_quote.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        skip_error=True,
    )

    with open(asset + ".err") as f:
        errors = f.readlines()
        assert len(errors) == 2
        assert errors[1].startswith("3")
        assert errors[1].split(",")[1].startswith("WrongFieldDialectException")

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 9
            assert 3 not in [a for a, b, c in rows]


def test_unlogged():
    tablename = "not_logged"
    asset = "tests/assets/simple.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        unlogged=True,
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute(
                "SELECT relowner FROM pg_class WHERE relpersistence = 'u' AND relname = '{tablename}'".format(
                    tablename=tablename
                )
            )
            rows = curs.fetchone()
            assert rows[0] == 10


def test_filename():
    tablename = "with_filename"
    asset = "tests/assets/simple.csv"

    copy_to(
        HOST,
        PORT,
        DBNAME,
        USER,
        PASSWORD,
        tablename,
        asset,
        overwrite=True,
        inject_filename=True,
    )

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10
            for c in rows:
                assert c[0] == "simple.csv"
