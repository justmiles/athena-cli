# athena-cli

Run SQL statements against Amazon Athena and return results to stdout

## Usage

```
athena-cli query --help

Flags:
  -d, --database string        Athena database to query (default "default")
  -f, --format string          format the output as either json, csv, or table (default "csv")
  -h, --help                   help for query
      --jmespath string        optional JMESPath to further filter or format results. See jmespath.org for more.
      --output-bucket string   S3 bucket for Athena query results (default "aws-athena-query-results-<account>-<region>")
      --output-prefix string   S3 key prefix for Athena query results
  -s, --sql string             SQL query to execute
```

## Roadmap

- [x] Support CSV, JSON, and ASCII table
- [ ] Add common partition-by-date feature
- [ ] Add `--workgroup` flag
- [ ] Support most flags as environment variables (workgroup, output location, output format)
- [ ] Don't choke if query doesn't return results (MSCK REPAIR TABLE)
