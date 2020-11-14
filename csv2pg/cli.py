#!/usr/bin/env python
# -*- coding: utf-8 -*-
import getpass
import os

import click

from csv2pg import __version__, copy_to
from csv2pg.main import COPY_BUFFER


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

    default_options = {
        "application_name": "csv2pg",
        "connect_timeout": "10",
    }

    pgpassword = os.getenv("PGPASSWORD")
    if password:
        pgpassword = click.prompt("Password", hide_input=True)

    copy_to(
        hostname,
        port,
        dbname,
        username,
        pgpassword,
        table,
        filepath,
        connection_options=default_options,
        verbose=verbose,
        progress=progress,
        skip_error=skip_error,
        header=header,
        inject_rownum=rownum,
        inject_filename=filename,
        delimiter=delimiter,
        quotechar=quotechar,
        doublequote=doublequote,
        escapechar=escapechar,
        lineterminator=lineterminator,
        null=null,
        encoding=encoding,
        overwrite=overwrite,
        unlogged=unlogged,
        buffer=buffer,
    )


if __name__ == "__main__":
    cli()
