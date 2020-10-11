#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import getpass
import io
import os
import re

import click
import psycopg2
import psycopg2.extras
from tqdm import tqdm

from bin import __version__
from lib.striter import StringIteratorIO

COPY_BUFFER = 2 ** 13  # default read buffer size for copy_expert
FIELD_VALIDITY_PATTERN = "^([^{quotechar}]+|{quotechar}(?:[^{quotechar}]|{quotechar}{quotechar}|{escapechar}{quotechar})*{quotechar})?$"


class CsvException(Exception):
    pass


class TooManyFieldsException(CsvException):
    pass


class MissingFieldsException(CsvException):
    pass


class WrongFieldDialectException(CsvException):
    pass


@click.command()
@click.option(
    "-h",
    "--host",
    "hostname",
    envvar="PGHOST",
    default="localhost",
    show_default=True,
    help="database server host",
)
@click.option(
    "-p",
    "--port",
    "port",
    envvar="PGPORT",
    type=int,
    default=5432,
    show_default=True,
    help="database server port",
)
@click.option(
    "-d",
    "--dbname",
    "dbname",
    envvar="PGDATABASE",
    default=getpass.getuser(),
    show_default=True,
    help="database user name",
)
@click.option(
    "-U",
    "--username",
    "username",
    envvar="PGUSER",
    default=getpass.getuser(),
    show_default=True,
    help="database name to connect to",
)
@click.option(
    "-W", "--password", "password", is_flag=True, help="force password prompt"
)
@click.option("-v", "--verbose", "verbose", is_flag=True, default=False)
@click.option(
    "--progress", "progress", is_flag=True, default=False, help="display progress bar"
)
@click.option(
    "--skip-error",
    "skip_error",
    is_flag=True,
    default=False,
    show_default=True,
    help="detect, ignore and export errors to <filepath>.err",
)
@click.option(
    "--header/--no-header", "header", is_flag=True, default=True, show_default=True
)
@click.option(
    "--rownum/--no-rownum",
    "rownum",
    is_flag=True,
    default=False,
    show_default=True,
    help="include line number in a _rownum column",
)
@click.option(
    "--filename/--no-filename",
    "filename",
    is_flag=True,
    default=False,
    show_default=True,
    help="include filename in a _filename column",
)
@click.option(
    "--delimiter",
    "delimiter",
    default=",",
    show_default=True,
    help="char separating the fields",
)
@click.option(
    "--quotechar",
    "quotechar",
    default='"',
    show_default=True,
    help="char used to quote a field",
)
@click.option(
    "--doublequote",
    "doublequote",
    is_flag=True,
    default=False,
    show_default=True,
    help="When True, escapechar is replaced by doubling the quote char",
)
@click.option(
    "--escapechar",
    "escapechar",
    default="\\",
    show_default=True,
    help="char used to esapce the quote char",
)
@click.option(
    "--lineterminator",
    "lineterminator",
    default="\r\n",
    show_default=True,
    help="line ending sequence",
)
@click.option(
    "--null",
    "null",
    default="",
    show_default=True,
    help="will be treated as NULL by postgres",
)
@click.option("--encoding", "encoding", default="utf-8", show_default=True)
@click.option(
    "--overwrite",
    "overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="destroy table before inserting csv",
)
@click.option(
    "--unlogged",
    "unlogged",
    is_flag=True,
    default=False,
    show_default=True,
    help="insert in an UNLOGGED table (faster)",
)
@click.option(
    "--buffer",
    "buffer",
    type=int,
    default=COPY_BUFFER,
    show_default=True,
    help="size of the read buffer to be used by COPY FROM",
)
@click.argument("table", nargs=1)
@click.argument("filepath", nargs=1, type=click.Path())
@click.version_option(version=__version__)
def cli(
    hostname,
    port,
    dbname,
    username,
    password,
    verbose,
    progress,
    skip_error,
    header,
    rownum,
    filename,
    delimiter,
    quotechar,
    doublequote,
    escapechar,
    lineterminator,
    null,
    encoding,
    overwrite,
    unlogged,
    buffer,
    table,
    filepath,
):
    """
    COPY FROM 'csv' TO 'postgres'
    """
    dialect = csv.Dialect
    dialect.delimiter = str(delimiter)
    dialect.quotechar = str(quotechar)
    dialect.doublequote = str(doublequote)
    dialect.escapechar = str(escapechar)
    dialect.lineterminator = str(lineterminator)
    dialect.quoting = csv.QUOTE_MINIMAL
    dialect.skipinitialspace = True

    if verbose:
        click.echo(
            "Reading {fp} as csv with [header={h}, delimiter={d}, quotechar={q}, escapechar={e}, lineterminator={lt}]".format(
                fp=filepath,
                h=header,
                d=repr(dialect.delimiter),
                q=repr(dialect.quotechar),
                e=repr(dialect.escapechar),
                lt=repr(dialect.lineterminator),
            )
        )

    columns = get_columns(filepath, header, dialect, encoding=encoding)

    pgpassword = os.getenv("PGPASSWORD")
    if password:
        pgpassword = click.prompt("Password", hide_input=True)
    pg_uri = "postgres://{username}{password}@{hostname}:{port}/{dbname}?application_name=csv2pg&connect_timeout={connect_timeout}".format(
        username=username,
        password=(":" + pgpassword if pgpassword else ""),
        hostname=hostname,
        port=port,
        dbname=dbname,
        connect_timeout=5,
    )

    db_status, db_server_version = check_database(pg_uri)
    if not db_status:
        raise click.ClickException("Database connection error {}".format(pg_uri))
    elif verbose:
        click.echo(
            "Database connection success {} [{}]".format(pg_uri, db_server_version)
        )

    with psycopg2.connect(
        pg_uri, cursor_factory=psycopg2.extras.RealDictCursor
    ) as connection:
        with connection.cursor() as cursor:
            if overwrite:
                drop_table(cursor, table, verbose=verbose)
            create_table(
                cursor,
                table,
                columns,
                filename=filename,
                rownum=rownum,
                verbose=verbose,
                unlogged=unlogged,
            )
            connection.commit()
            copy(
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
                rownum=rownum,
                filename=filename,
            )


def check_database(uri):
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


def get_columns(filepath, header, dialect, encoding="utf-8"):
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


def drop_table(cursor, table, verbose=False):
    sql = "DROP TABLE IF EXISTS {table};".format(table=table)
    cursor.execute(sql)
    if verbose:
        click.echo(cursor.statusmessage)
        try:
            click.secho(cursor.connection.notices.pop().rstrip(), fg="white")
        except IndexError:
            click.secho(cursor.query.decode(), fg="white")


def create_table(
    cursor, table, columns, rownum=False, filename=False, verbose=False, unlogged=False
):
    columns_sql = ", \n".join(
        '    "{column}" TEXT'.format(column=column) for column in columns
    )
    if rownum:
        columns_sql = "_rownum INTEGER,\n" + columns_sql
    if filename:
        columns_sql = "_filename TEXT,\n" + columns_sql
    unlogged = " UNLOGGED " if unlogged else " "
    sql = "CREATE{unlogged}TABLE IF NOT EXISTS {table} (\n{columns}\n);".format(
        unlogged=unlogged, table=table, columns=columns_sql
    )
    cursor.execute(sql)
    if verbose:
        click.echo(cursor.statusmessage)
        try:
            click.secho(cursor.connection.notices.pop().rstrip(), fg="white")
        except IndexError:
            click.secho(cursor.query.decode(), fg="white")


def copy(
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
    rownum=False,
    filename=False,
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
        click.secho("Estimating file size...", fg="white")
        with open(filepath, "rb") as f:
            for line in f:
                line_count += 1

    if verbose:
        click.secho(sql, fg="white")

    with io.open(filepath, "r", encoding=encoding) as f_in:
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
                    skip_error=skip_error,
                    verbose=verbose,
                    progress=progress,
                    progress_total=line_count,
                    inject_rownum=rownum,
                    inject_filename=filename,
                )
            )
            cursor.copy_expert(sql, wrapper, size=buffer_size)

    if verbose:
        click.echo("COPY {}".format(cursor.rowcount))


def _wrap(
    f_in,
    f_err,
    dialect,
    header,
    expected_columns,
    skip_error=False,
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
    writer = csv.writer(f_err, dialect=dialect)
    for i, line in tqdm(enumerate(f_in), disable=not progress, total=progress_total):
        line_number = i if header else i + 1
        reader = csv.reader([line], dialect=dialect)
        for r in reader:
            parsed_line = r
            break

        # Init error file
        if i == 0:
            if header:
                generated_header = ["_rownum", "_error"] + parsed_line
            else:
                generated_header = ["_rownum", "_error"] + expected_columns
            writer.writerow(generated_header)

        # Check line and skip
        try:
            if skip_error:
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
    if verbose:
        click.secho(
            f"{error} in file {filename} at line {line_number}:{field_number}:{header_error}: {message}",
            fg="red",
            err=True,
        )
    err_row = [line_number, f"{error}:{field_number}:{header_error}"] + parsed_line
    return err_row


if __name__ == "__main__":
    cli()
