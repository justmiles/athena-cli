# Athena CLI

Run SQL statements against Amazon Athena and return results to stdout

## Usage

```
athena query --help

Flags:
  -d, --database string               Athena database to query (default "default")
  -f, --format string                 format the output as either json, csv, or table (default "csv")
  -h, --help                          help for query
  -o, --output string                 file name to write this content to. If empty, will write to stdout (default "")
      --query-results-bucket string   S3 bucket for Athena query results (default "aws-athena-query-results-<account>-<region>")
      --query-results-prefix string   S3 key prefix for Athena query results
  -s, --sql string                    SQL query to execute. Can be a file or raw query
      --statistics                    print query statistics to stderr
```

## Examples

### Running Queries

```shell
> athena query --sql "SELECT now() as Rightnow"
"Rightnow"
"2020-08-31 19:21:55.721 UTC"
```

#### Execute query from file

```shell
> cat example.sql
WITH dataset AS (
  SELECT
    'engineering' as department,
    ARRAY['Sharon', 'John', 'Bob', 'Sally'] as users
)
SELECT department, names FROM dataset
CROSS JOIN UNNEST(users) as t(names)

> athena query --sql example.sql
"department","names"
"engineering","Sharon"
"engineering","John"
"engineering","Bob"
"engineering","Sally"
```

### Statistics

The `--statistics` flag sends stats to stderr

```shell
> athena query --sql "SELECT now() as RightNow, now() + interval '1' day as Tomorrow" --format table --statistics
Data Scanned: 0
Execution Time: 372

| RIGHTNOW                    | TOMORROW                    |
| --------------------------- | --------------------------- |
| 2020-08-31 19:04:39.301 UTC | 2020-09-01 19:04:39.301 UTC |
```

### Output Formats

The `--format` flag supports formating the outputs as json, csv, or table

#### table output

```shell
> athena query --sql "SELECT now() as RightNow, now() + interval '1' day as Tomorrow" --format table
| RIGHTNOW                    | TOMORROW                    |
| --------------------------- | --------------------------- |
| 2020-08-31 18:57:34.280 UTC | 2020-09-01 18:57:34.280 UTC |
```

```shell
> athena query --format table --sql "$(cat <<EOF
  WITH dataset AS (
    SELECT
      'engineering' as department,
      ARRAY['Sharon', 'John', 'Bob', 'Sally'] as users
  )
  SELECT department, names FROM dataset
  CROSS JOIN UNNEST(users) as t(names)
EOF
)"

| DEPARTMENT  | NAMES  |
| ----------- | ------ |
| engineering | Sharon |
| engineering | John   |
| engineering | Bob    |
| engineering | Sally  |

```

#### json output

```shell
> athena query --sql "SELECT now() as RightNow, now() + interval '1' day as Tomorrow" --format json
[
  {
    "RightNow": "2020-08-31 18:57:43.201 UTC",
    "Tomorrow": "2020-09-01 18:57:43.201 UTC"
  }
]
```

#### csv output

```shell
> athena query --sql "SELECT now() as RightNow, now() + interval '1' day as Tomorrow" --format csv
"RightNow","Tomorrow"
"2020-08-31 18:57:49.606 UTC","2020-09-01 18:57:49.606 UTC"
```

## Roadmap

- [x] Support CSV, JSON, and ASCII table
- [x] Add common partition-by-date feature
- [ ] Add `--workgroup` flag
- [ ] Support most flags as environment variables (workgroup, output location, output format)
- [x] Don't choke if query doesn't return results (MSCK REPAIR TABLE)
- [ ] SIGTERM or CTRL+C should cancel the query
- [ ] Support a `--cached <n minutes>` flag that will just download the results of a previous query if it is older than N minutes