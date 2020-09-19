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
PGHOST=mypg.host.com PGPORT=5432 PGDATABASE=mydb PGUSER=myuser csv2pg public.my_table data.csv --verbose
```

### Basic database utilities
csv2pg will automatically create the table using the header of the csv file. Caution, the option `--overwrite` will drop the table before inserting the new records in. 
