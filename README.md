# csv2pg
A simple and fast cli application to load a csv into postgres

## Installation
```bash
pip install --user csv2pg
```

## Usage
```bash
csv2pg --help
```
Basic usage:
```bash
csv2pg -h localhost -p 5432 -U postgres -d postgres public.data data.csv --verbose
```
Basic usage with postgres environment variables:
```sh
PGHOST=localhost PGPORT=5432 PGDATABASE=postgres PGUSER=postgres PGPASSWORD= csv2pg public.data data.csv --verbose
```
Loading a tab delimited latin-1 encoded file in an unlogged table with `_filename` and `_rownum` columns, skipping errors and displaying progress bar:
```sh
PGPASSWORD= csv2pg -h localhost -p 25432 -d test -U test --delimiter=$'\t' --encoding="iso-8859-1" --overwrite --unlogged --filename --rownum --skip-error --progress public.data data.csv
```

### Precaution
* the `--overwrite` option will drop the table before inserting the new records in. 
* the `--rownum` and `--filename` options will slightly increase the insertion time (increase the data to write on disk)
* the `--skip-error` option will slightly increase the insertion time (fields and lines validation)
* `--verbose` and `--progress` used together might spoil the console output
