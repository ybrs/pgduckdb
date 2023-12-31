# pgduckdb
Postgresql wire protocol proxy for duckdb. In other words, this is a network proxy to allow access to your duckdb files over the postgresql wire protocol.

# Why

Duckdb is an embedded database, and in some circumstances you'd want to share/connect to it over network.

Some clients/applications might not have a duckdb option but have a way to connect to a postgres server. (eg: grafana)

# Installation

As usual you can do

```bash
go install github.com/ybrs/pgduckdb@latest
```

# Usage

You can run pgduckdb pointing to your duckdb file

```

```
