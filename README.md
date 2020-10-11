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
csv2pg expects the same parameter than psql to setup the postgres connection
```bash
# Loading data.csv into public.my_table
csv2pg -h localhost -p 5432 -U postgres -d postgres public.my_table data.csv --verbose
# Using postgres env to set connection variables
PGHOST=localhost PGPORT=5432 PGDATABASE=postgres PGUSER=postgres PGPASSWORD= csv2pg public.my_table data.csv --verbose
# Loading a tab delimited latin-1 encoded file in an unlogged table with _filename and _rownum columns, skipping errors and displaying progress bar
PGPASSWORD= python csv2pg/csv2pg.py -h localhost -p 25432 -d test -U test --delimiter $'\t' --encoding="iso-8859-1" --overwrite --unlogged --filename --rownum --skip-error --progress public.my_table data.csv
```

### Precaution
* the `--overwrite` option will drop the table before inserting the new records in. 
* the `--rownum` and `--filename` options will slightly increase the insertion time (extra columns to write)
* the `--skip-error` option will slightly increase the insertion time (field validation)
* `--verbose` and `--progress` used together might spoil the console output
