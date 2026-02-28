# duckdb-spanner-cxx

A DuckDB extension for querying [Google Cloud Spanner](https://cloud.google.com/spanner) using the [google-cloud-cpp](https://github.com/googleapis/google-cloud-cpp) Spanner client library.

Based on https://github.com/duckdb/extension-template.

## Features

- **`spanner_query(database, sql, ...)`** — Execute arbitrary SQL queries against Spanner (GoogleSQL or PostgreSQL dialect)
- **`spanner_scan(database, table, ...)`** — Read a Spanner table using the Read API with projection pushdown
- **`COPY ... TO 'table' (FORMAT spanner, database '...')`** — Write data to Spanner using mutations
- Helper macros: `spanner_value`, `spanner_typed`, `spanner_params`, `_spanner_type_name`, `interval_to_iso8601`

## Building

### Prerequisites

- CMake 3.15+
- A C++17 compiler
- [vcpkg](https://vcpkg.io/en/getting-started) for dependency management

### Setup vcpkg

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build

```sh
make release
```

The main binaries:
```
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/spanner/spanner.duckdb_extension
```

> **Note:** The first build takes a long time due to google-cloud-cpp dependencies (gRPC, protobuf, abseil, etc.). Subsequent builds use the vcpkg cache.

## Usage

### Authentication

The extension uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`.

For the Spanner emulator, set `SPANNER_EMULATOR_HOST`:

```shell
export SPANNER_EMULATOR_HOST=localhost:9010
```

### Examples

```sql
LOAD spanner;

-- Query with SQL
SELECT * FROM spanner_query(
    'projects/my-project/instances/my-instance/databases/my-db',
    'SELECT * FROM Singers WHERE SingerId = 1'
);

-- Scan a table
SELECT * FROM spanner_scan(
    'projects/my-project/instances/my-instance/databases/my-db',
    'Singers'
);

-- Write data
COPY (SELECT 1 AS SingerId, 'Alice' AS Name)
TO 'Singers' (FORMAT spanner, database 'projects/my-project/instances/my-instance/databases/my-db');
```

### Named Parameters

Both `spanner_query` and `spanner_scan` support:

| Parameter | Type | Description |
|---|---|---|
| `endpoint` | VARCHAR | Custom endpoint (e.g., emulator) |
| `priority` | VARCHAR | Request priority: `LOW`, `MEDIUM`, `HIGH` |
| `exact_staleness_secs` | BIGINT | Exact staleness for reads |
| `max_staleness_secs` | BIGINT | Max staleness (query only) |
| `read_timestamp` | VARCHAR | Read at specific timestamp (RFC 3339) |
| `min_read_timestamp` | VARCHAR | Min read timestamp (query only) |

`spanner_scan` additionally supports:

| Parameter | Type | Description |
|---|---|---|
| `index` | VARCHAR | Secondary index name |
| `use_data_boost` | BOOLEAN | Enable Data Boost (reserved) |

## Running Tests

```sh
make test
```

## License

MIT
