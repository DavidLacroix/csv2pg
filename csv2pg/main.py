#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import io
import logging
import re

import psycopg2
import psycopg2.extras
from tqdm import tqdm

from csv2pg.exceptions import (
    CsvException,
    MissingFieldsException,
    TooManyFieldsException,
    WrongFieldDialectException,
)
from csv2pg.striter import StringIteratorIO


COPY_BUFFER = 2 ** 13  # default read buffer size for copy_expert
FIELD_VALIDITY_PATTERN = "^([^{quotechar}]+|{quotechar}(?:[^{quotechar}]|{quotechar}{quotechar}|{escapechar}{quotechar})*{quotechar})?$"

logger = logging.getLogger("csv2pg")


def copy_to(
    hostname,
    port,
    dbname,
    username,
    password,
    table,
    filepath,
    connection_options={},
    verbose=False,
    progress=False,
    skip_error=False,
    header=True,
    inject_rownum=False,
    inject_filename=False,
    delimiter=",",
    quotechar='"',
    doublequote=False,
    escapechar="\\",
    lineterminator="\r\n",
    null="",
    encoding="utf-8",
    overwrite=False,
    unlogged=False,
    buffer=COPY_BUFFER,
):
    """
    COPY FROM 'csv' TO 'postgres'
    """
    if verbose:
        logger.setLevel(logging.INFO)

    dialect = csv.Dialect
    dialect.delimiter = str(delimiter)
    dialect.quotechar = str(quotechar)
    dialect.doublequote = str(doublequote)
    dialect.escapechar = str(escapechar)
    dialect.lineterminator = str(lineterminator)
    dialect.quoting = csv.QUOTE_MINIMAL
    dialect.skipinitialspace = True

    logger.info(
        "Reading {fp} as csv with [header={h}, delimiter={d}, quotechar={q}, escapechar={e}, lineterminator={lt}]".format(
            fp=filepath,
            h=header,
            d=repr(dialect.delimiter),
            q=repr(dialect.quotechar),
            e=repr(dialect.escapechar),
            lt=repr(dialect.lineterminator),
        )
    )

    columns = _get_columns(filepath, header, dialect, encoding=encoding)

    options = "{has_options}{options}".format(
        has_options="?" if bool(connection_options) else "",
        options="&".join(f"{k}={v}" for k, v in connection_options.items()),
    )
    pg_uri_template = (
        "postgres://{username}{password}@{hostname}:{port}/{dbname}{options}"
    )
    pg_uri = pg_uri_template.format(
        username=username,
        password=(":" + password if password else ""),
        hostname=hostname,
        port=port,
        dbname=dbname,
        options=options,
    )
    pg_uri_safe = pg_uri_template.format(
        username=username,
        password="***",
        hostname=hostname,
        port=port,
        dbname=dbname,
        options=options,
    )

    db_status, db_server_version = _check_database(pg_uri)
    if not db_status:
        raise ConnectionError("Database connection error {}".format(pg_uri_safe))
    logger.info(
        "Database connection success {} [{}]".format(pg_uri_safe, db_server_version)
    )

    with psycopg2.connect(
        pg_uri, cursor_factory=psycopg2.extras.RealDictCursor
    ) as connection:
        with connection.cursor() as cursor:
            if overwrite:
                _drop_table(cursor, table, verbose=verbose)
            _create_table(
                cursor,
                table,
                columns,
                inject_filename=inject_filename,
                inject_rownum=inject_rownum,
                verbose=verbose,
                unlogged=unlogged,
            )
            connection.commit()
            _copy(
                cursor,
                table,
                filepath,
                header,
                columns,
                dialect,
                buffer_size=buffer,
                encoding=encoding,
                null=null,
                skip_error=skip_error,
                verbose=verbose,
                progress=progress,
                inject_rownum=inject_rownum,
                inject_filename=inject_filename,
            )


def _check_database(uri):
    """
    Check db connection
    """
    status = False
    server_version = None
    try:
        with psycopg2.connect(
            uri, cursor_factory=psycopg2.extras.RealDictCursor
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                server_version = connection.get_parameter_status("server_version")
    except psycopg2.OperationalError:
        pass
    status = True
    return status, server_version


def _get_columns(filepath, header, dialect, encoding="utf-8"):
    """
    Extracting columns from csv file. If --no-header is specified, return generic columns.
    """
    with io.open(filepath, "r", newline="", encoding=encoding) as f:
        reader = csv.reader(f, dialect=dialect)
        try:
            line = next(reader)
        except StopIteration:
            return

    columns = line if header else _default_columns(line)

    return columns


def _default_columns(values):
    """
    Generate generic column array
    """
    columns = [("column_" + str(i)) for i, v in enumerate(values)]
    return columns


def _drop_table(cursor, table, verbose=False):
    sql = "DROP TABLE IF EXISTS {table};".format(table=table)
    cursor.execute(sql)
    _log_cursor_execution(cursor)


def _create_table(
    cursor,
    table,
    columns,
    inject_rownum=False,
    inject_filename=False,
    verbose=False,
    unlogged=False,
):
    columns_sql = ", \n".join(
        '    "{column}" TEXT'.format(column=column) for column in columns
    )
    if inject_rownum:
        columns_sql = "_rownum INTEGER,\n" + columns_sql
    if inject_filename:
        columns_sql = "_filename TEXT,\n" + columns_sql
    unlogged = " UNLOGGED " if unlogged else " "
    sql = "CREATE{unlogged}TABLE IF NOT EXISTS {table} (\n{columns}\n);".format(
        unlogged=unlogged, table=table, columns=columns_sql
    )
    cursor.execute(sql)
    _log_cursor_execution(cursor)


def _log_cursor_execution(cursor):
    logger.info(cursor.statusmessage)
    try:
        logger.info(cursor.connection.notices.pop().rstrip())
    except IndexError:
        logger.info(cursor.query.decode())


def _copy(
    cursor,
    table,
    filepath,
    header,
    expected_columns,
    dialect,
    buffer_size=1024,
    encoding="utf-8",
    null="",
    skip_error=False,
    verbose=False,
    progress=False,
    inject_rownum=False,
    inject_filename=False,
):
    sql = "COPY {table} FROM STDIN WITH CSV DELIMITER {delimiter} NULL {null}{quote}{escape}{header}".format(
        table=table,
        delimiter=psycopg2.extensions.adapt(dialect.delimiter),
        null=psycopg2.extensions.adapt(null),
        quote=" QUOTE {}".format(psycopg2.extensions.adapt(dialect.quotechar))
        if dialect.quotechar
        else "",
        escape=" ESCAPE {}".format(psycopg2.extensions.adapt(dialect.escapechar))
        if dialect.escapechar
        else "",
        header=" HEADER" if header else "",
    )

    line_count = 0
    if progress:
        logger.info("Estimating file size...")
        with open(filepath, "rb") as f:
            for line in f:
                line_count += 1

    logger.info(sql)

    with io.open(filepath, "r", encoding=encoding) as f_in:
        if skip_error:
            err_filepath = filepath + ".err"
            # TODO: only create err file if errors are found
            with io.open(err_filepath, "w", encoding=encoding) as f_err:
                wrapper = StringIteratorIO(
                    _wrap(
                        f_in,
                        f_err,
                        dialect,
                        header,
                        expected_columns,
                        verbose=verbose,
                        progress=progress,
                        progress_total=line_count,
                        inject_rownum=inject_rownum,
                        inject_filename=inject_filename,
                    )
                )
                cursor.copy_expert(sql, wrapper, size=buffer_size)
        else:
            wrapper = StringIteratorIO(
                _wrap(
                    f_in,
                    None,
                    dialect,
                    header,
                    expected_columns,
                    verbose=verbose,
                    progress=progress,
                    progress_total=line_count,
                    inject_rownum=inject_rownum,
                    inject_filename=inject_filename,
                )
            )
            cursor.copy_expert(sql, wrapper, size=buffer_size)

    logger.info("COPY {}".format(cursor.rowcount))


def _wrap(
    f_in,
    f_err,
    dialect,
    header,
    expected_columns,
    verbose=False,
    progress=False,
    progress_total=None,
    inject_rownum=False,
    inject_filename=False,
):
    filename = f_in.name.split("/")[-1]
    field_pattern = re.compile(
        FIELD_VALIDITY_PATTERN.format(
            quotechar=dialect.quotechar, escapechar=dialect.escapechar
        )
    )

    generated_header = []
    writer = csv.writer(f_err, dialect=dialect) if f_err else None
    for i, line in tqdm(enumerate(f_in), disable=not progress, total=progress_total):
        line_number = i if header else i + 1
        reader = csv.reader([line], dialect=dialect)
        for r in reader:
            parsed_line = r
            break

        if f_err:
            # Init error file
            if i == 0:
                if header:
                    generated_header = ["_rownum", "_error"] + parsed_line
                else:
                    generated_header = ["_rownum", "_error"] + expected_columns
                writer.writerow(generated_header)
            # Check line and skip
            try:
                _check_line(expected_columns, parsed_line, field_pattern)
            except CsvException as e:
                err_row = _format_error(
                    filename, parsed_line, line_number, generated_header, e, verbose
                )
                writer.writerow(err_row)
                continue

        # Inject extra fields in line before insertion
        if inject_rownum:
            line = "{value}{delimiter}{line}".format(
                value=line_number, delimiter=dialect.delimiter, line=line
            )
        if inject_filename:
            line = "{value}{delimiter}{line}".format(
                value=filename, delimiter=dialect.delimiter, line=line
            )

        # import pdb;pdb.set_trace()
        yield line


def _check_line(ref, target, pattern):
    if len(ref) > len(target):
        missing = len(ref) - len(target)
        raise MissingFieldsException(f"{missing} missing fields", None)
    if len(ref) < len(target):
        extra = len(target) - len(ref)
        raise TooManyFieldsException(f"{extra} extra fields", None)

    for i, field in enumerate(target):
        result = pattern.findall(field)
        if len(result) != 1:
            raise WrongFieldDialectException(field, i)
        if result[0] != field:
            raise WrongFieldDialectException(field, i)


def _format_error(
    filename, parsed_line, line_number, generated_header, exception, verbose
):
    error = exception.__class__.__name__
    message, field_number = exception.args
    try:
        header_error = generated_header[field_number]
    except (IndexError, TypeError):
        header_error = None

    logger.info(
        f"{error} in file {filename} at line {line_number}:{field_number}:{header_error}: {message}",
    )
    err_row = [line_number, f"{error}:{field_number}:{header_error}"] + parsed_line
    return err_row
