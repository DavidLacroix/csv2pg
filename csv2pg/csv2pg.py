#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import getpass
import io
import os

import click
import psycopg2
import psycopg2.extras

COPY_BUFFER = 2 ** 13  # default read buffer size for copy_expert


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
    "--header/--no-header", "header", is_flag=True, default=True, show_default=True
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
    "--buffer",
    "buffer",
    type=int,
    default=COPY_BUFFER,
    show_default=True,
    help="size of the read buffer to be used by COPY FROM",
)
@click.argument("table", nargs=1)
@click.argument("filepath", nargs=1, type=click.Path())
@click.version_option()
def cli(
    hostname,
    port,
    dbname,
    username,
    password,
    verbose,
    header,
    delimiter,
    quotechar,
    doublequote,
    escapechar,
    lineterminator,
    null,
    encoding,
    overwrite,
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

    pgpassword = os.getenv("PGPASSWORD")
    if password:
        pgpassword = click.prompt("Password", hide_input=True)
    pg_uri = "postgres://{username}{password}@{hostname}:{port}/{dbname}?application_name=csv2pg".format(
        username=username,
        password=(":" + pgpassword if pgpassword else ""),
        hostname=hostname,
        port=port,
        dbname=dbname,
    )

    db_status, db_server_version = check_database(pg_uri)
    if not db_status:
        raise click.ClickException("Database connection error {}".format(pg_uri))
    elif verbose:
        click.echo(
            "Database connection success {} [{}]".format(pg_uri, db_server_version)
        )

    columns = get_columns(filepath, header, dialect, encoding=encoding)

    with psycopg2.connect(
        pg_uri, cursor_factory=psycopg2.extras.RealDictCursor
    ) as connection:
        with connection.cursor() as cursor:
            if overwrite:
                drop_table(cursor, table, verbose=verbose)
            create_table(cursor, table, columns, verbose=verbose)
            connection.commit()
            copy(
                cursor,
                table,
                filepath,
                header,
                dialect,
                buffer_size=buffer,
                encoding=encoding,
                null=null,
                verbose=verbose,
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


def create_table(cursor, table, columns, verbose=False):
    columns_sql = ", \n".join(
        '    "{column}" TEXT'.format(column=column) for column in columns
    )
    sql = "CREATE TABLE IF NOT EXISTS {table} (\n{columns}\n);".format(
        table=table, columns=columns_sql
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
    dialect,
    buffer_size=1024,
    encoding="utf-8",
    null="",
    verbose=False,
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
    with io.open(filepath, "r", encoding=encoding) as f:
        cursor.copy_expert(sql, f, size=buffer_size)

    if verbose:
        click.echo("COPY {}".format(cursor.rowcount))
        click.secho(sql, fg="white")


if __name__ == "__main__":
    cli()
