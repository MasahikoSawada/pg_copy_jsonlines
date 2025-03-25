# pg_copy_jsonlines

An experimental implementation that adds [JSON Lines](https://jsonlines.org/) format support for PostgreSQL's COPY TO and COPY FROM commands. JSON Lines is a convenient format for storing structured data that may contain newlines and is designed for streaming large datasets one record at a time.

## Background

PostgreSQL's COPY command traditionally supports three built-in formats: text, CSV, and binary. In 2024, a proposal was made to make the COPY format system extensible, allowing third-party modules to implement custom formats. This proposal is currently under discussion in the PostgreSQL community ([CommitFest #4681](https://commitfest.postgresql.org/patch/4681/)).

⚠️ **Important Note**: This extension requires patches from the above proposal, which have not yet been committed to PostgreSQL. As such, this implementation is experimental and intended to serve as a reference implementation for the proposed COPY format extension API.

# Build and Installation

`pg_copy_jsonlines` can be built in the same way as other extensions:

```bash
$ cd pg_copy_jsonlines
$ make USE_PGXS=1
$ make install USE_PGXS=1
```

```sql
=# CREATE EXTENSION pg_copy_jsonlines;
CREATE EXTENSION
=# \df
                         List of functions
 Schema |   Name    | Result data type | Argument data types | Type
--------+-----------+------------------+---------------------+------
 public | jsonlines | copy_handler     | internal            | func
(1 row)
```

Now you can use `jsonlines` format in both COPY TO and COPY FROM commands.

# Examples

## `COPY TO` with JSON Lines format

```sql
=# CREATE TABLE jl (id int, a text, b jsonb);
CREATE TABLE
=# INSERT INTO jl VALUES (1, 'hello', '{"test" : [1, true, {"num" : 42}]}'::jsonb), (2, 'hello world', 'true'), (999, null, '{"a" : 1}');
INSERT 0 3
=# TABLE jl;
 id  |      a      |                b
-----+-------------+----------------------------------
   1 | hello       | {"test": [1, true, {"num": 42}]}
   2 | hello world | true
 999 |             | {"a": 1}
(3 rows)


=# COPY jl TO '/tmp/test.jsonl' WITH (format 'jsonlines');
COPY 3

=# \! cat /tmp/test.jsonl
{"id":1,"a":"hello","b":{"test": [1, true, {"num": 42}]}}
{"id":2,"a":"hello world","b":true}
{"id":999,"a":null,"b":{"a": 1}}
```

## `COPY FROM` with JSON Lines format

```sql
=# \! cat /tmp/test.jsonl
{"a":"hello","id":1,"b":{"test": [1, true, {"num": 42}]}}
{"id":2,"b":true,"a":"hello world"}
{"id":999,"a":null,"b":{"a": 1}}
=# CREATE TABLE jl_load (id int, a text, b jsonb);
CREATE TABLE
=# COPY jl_load FROM '/tmp/test.jsonl' WITH (format 'jsonlines');
COPY 3
=# TABLE jl_load;
 id  |      a      |                b
-----+-------------+----------------------------------
   1 | hello       | {"test": [1, true, {"num": 42}]}
   2 | hello world | true
 999 |             | {"a": 1}
(3 rows)
```