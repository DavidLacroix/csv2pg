# csv2pg
A simple and fast cli application to load a csv into postgres

## Installation
```bash
pip install csv2pg
```

## Usage
csv2pg expects the same parameter than psql to setup the postgres connection
```bash
csv2pg -h localhost -p 5432 -U postgres -d postgres public.my_table data.csv --verbose
PGHOST=mypg.host.com PGPORT=5432 PGDATABASE=mydb PGUSER=myuser csv2pg public.my_table data.csv --verbose

csv2pg --help
```

### Dialect discovery
With python csv.Sniffer() class the csv parameters like `delimiter`, `quotechar`, `escapechar`, `lineterminator` are automatically detected. They can be overriden with cli options.

### Basic database utilities
csv2pg will automatically create the table using the header of the csv file. Caution, the option `--overwrite` will drop the table before inserting the new records in. 
