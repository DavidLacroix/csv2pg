# csv2pg
A simple and fast cli application to load a csv into postgres

## Installation
```bash
pip install --user csv2pg
```

## Usage
```
$ csv2pg --help

Usage: csv2pg.py [OPTIONS] TABLE FILEPATH

  COPY FROM 'csv' TO 'postgres'

Options:
  -h, --host TEXT             database server host  [default: localhost]
  -p, --port INTEGER          database server port  [default: 5432]
  -d, --dbname TEXT           database user name  [default: $USER]
  -U, --username TEXT         database name to connect to  [default: $USER]
  -W, --password              force password prompt
  -v, --verbose
  --progress                  display progress bar
  --skip-error                detect, ignore and export errors to
                              <filepath>.err  [default: False]

  --header / --no-header      [default: True]
  --rownum / --no-rownum      include line number in a _rownum column
                              [default: False]

  --filename / --no-filename  include filename in a _filename column
                              [default: False]

  --delimiter TEXT            char separating the fields  [default: ,]
  --quotechar TEXT            char used to quote a field  [default: "]
  --doublequote               When True, escapechar is replaced by doubling
                              the quote char  [default: False]

  --escapechar TEXT           char used to esapce the quote char  [default: \]
  --lineterminator TEXT       line ending sequence  [default:  ]
  --null TEXT                 will be treated as NULL by postgres  [default: ]
  --encoding TEXT             [default: utf-8]
  --overwrite                 destroy table before inserting csv  [default:
                              False]

  --unlogged                  insert in an UNLOGGED table (faster)  [default:
                              False]

  --buffer INTEGER            size of the read buffer to be used by COPY FROM
                              [default: 8192]

  --version                   Show the version and exit.
  --help                      Show this message and exit.
```

Basic usage:
```sh
csv2pg -h localhost -p 5432 -U postgres -d postgres public.data data.csv --verbose
```
Basic usage with postgres environment variables:
```sh
PGHOST=localhost PGPORT=5432 PGDATABASE=postgres PGUSER=postgres PGPASSWORD= csv2pg public.data data.csv --verbose
```
Loading a tab delimited latin-1 encoded file in an unlogged table with `_filename` and `_rownum` columns, skipping errors and displaying progress bar:
```sh
PGPASSWORD= csv2pg -h localhost -p 25432 -d test -U test \
    --delimiter=$'\t' --encoding="iso-8859-1" \
    --overwrite --unlogged \
    --filename --rownum \
    --skip-error --progress \
public.data data.csv
```

## Quick test
Start a postgres database:
```sh
docker run -d --rm \
    -p 25432:5432 \
    --name csv2pg-test \
    -e POSTGRES_DB=test \
    -e POSTGRES_USER=test \
    -e POSTGRES_PASSWORD=test \
postgres
```
Download and import a test asset file:
```sh
wget https://raw.githubusercontent.com/DavidLacroix/csv2pg/master/tests/assets/simple.csv .
csv2pg -h localhost -p 25432 -U test -d test public.data simple.csv --progress
```

### Precaution
* the `--overwrite` option will drop the table before inserting the new records in. 
* the `--rownum` and `--filename` options will slightly increase the insertion time (increase the data to write on disk)
* the `--skip-error` option will slightly increase the insertion time (fields and lines validation)
* `--verbose` and `--progress` used together might spoil the console output
