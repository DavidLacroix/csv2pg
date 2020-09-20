import os

import psycopg2
import pytest

from csv2pg.csv2pg import COPY_BUFFER, cli

HOST = "localhost"
PORT = 25432
DBNAME = "test"
USER = "test"

os.putenv("PGPASSWORD", "test")
DSN = "host={host} port={port} dbname={dbname} user={user}".format(
    host=HOST,
    port=PORT,
    dbname=DBNAME,
    user=USER,
)


def call_csv2pg(
    tablename,
    filepath,
    header=True,
    rownum=False,
    delimiter=",",
    quotechar='"',
    doublequote=False,
    escapechar="\\",
    lineterminator="\r\n",
    null="",
    encoding="utf-8",
    overwrite=False,
    buffer=COPY_BUFFER,
):
    cli.callback(
        HOST,
        PORT,
        DBNAME,
        USER,
        False,  # force-password
        False,  # verbose
        header,
        rownum,
        delimiter,
        quotechar,
        doublequote,
        escapechar,
        lineterminator,
        null,
        encoding,
        overwrite,
        buffer,
        tablename,
        filepath,
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

    call_csv2pg(tablename, asset)

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

    call_csv2pg(tablename, asset)
    call_csv2pg(tablename, asset)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 20


def test_overwrite():
    tablename = "overwrite"
    asset = "tests/assets/simple.csv"

    call_csv2pg(tablename, asset)
    call_csv2pg(tablename, asset, overwrite=True)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 10


def test_delimiter():
    tablename = "delimiter"
    asset = "tests/assets/delimiter.csv"

    call_csv2pg(tablename, asset, overwrite=True, delimiter="@")

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

    call_csv2pg(tablename, asset, overwrite=True, delimiter="|", quotechar="(")

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

    call_csv2pg(
        tablename, asset, overwrite=True, delimiter="\t", quotechar='"', escapechar="\\"
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

    call_csv2pg(tablename, asset, overwrite=True, delimiter="@", header=False)

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

    call_csv2pg(tablename, asset, overwrite=True, delimiter=":")

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

    call_csv2pg(tablename, asset, overwrite=True, delimiter=":", null="NULLZZ")

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

    call_csv2pg(tablename, asset, overwrite=True)

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

    call_csv2pg(tablename, asset, overwrite=True, encoding="ISO-8859-1")

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "ÑÆÛý¼àáäæ"


def test_encoding_GB18030():
    tablename = "encoding_GB18030"
    asset = "tests/assets/encoding_GB18030.csv"

    call_csv2pg(tablename, asset, overwrite=True, encoding="GB18030")

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "气广"


def test_complex():
    tablename = "complex"
    asset = "tests/assets/complex_LE-LF_header_bom.csv"

    call_csv2pg(tablename, asset, overwrite=True, lineterminator="\n")

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

    call_csv2pg(tablename, asset, overwrite=True, rownum=True)

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

    call_csv2pg(
        tablename, asset, overwrite=True, delimiter="@", header=False, rownum=True
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

    call_csv2pg(tablename, asset, overwrite=True, rownum=True, encoding="ISO-8859-1")

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

    call_csv2pg(tablename, asset, overwrite=True, rownum=True, encoding="GB18030")

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1
            assert rows[0][1] == "气广"


def test_insert_error():
    tablename = "insert_error"
    asset = "tests/assets/simple_error.csv"

    with pytest.raises(psycopg2.errors.BadCopyFileFormat):
        call_csv2pg(tablename, asset)

    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT * FROM {tablename}".format(tablename=tablename))
            rows = curs.fetchall()
            assert len(rows) == 0
