To recompute the SHA512 when bumping the commit of a repository (for example avro-c):
```bash
curl -L https://github.com/duckdb/duckdb-avro-c/archive/<commit_hash>.tar.gz | openssl dgst -sha512
```