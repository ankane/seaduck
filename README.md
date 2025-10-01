# SeaDuck

[Apache Iceberg](https://iceberg.apache.org/) for Ruby, powered by libduckdb

[![Build Status](https://github.com/ankane/seaduck/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/seaduck/actions)

## Installation

First, install libduckdb. For Homebrew, use:

```sh
brew install duckdb
```

Then add this line to your application’s Gemfile:

```ruby
gem "seaduck"
```

## Getting Started

Create a client for an Iceberg catalog

```ruby
catalog = SeaDuck::S3TablesCatalog.new(arn: "arn:aws:s3tables:...")
```

Note: SeaDuck requires a default namespace, which is `main` by default. This namespace is created if it does not exist. Pass `default_namespace` to use a different one.

Create a table

```ruby
catalog.sql("CREATE TABLE events (id bigint, name text)")
```

Load data from a file

```ruby
catalog.sql("COPY events FROM 'data.csv'")
```

You can also load data directly from other [data sources](https://duckdb.org/docs/stable/data/data_sources)

```ruby
catalog.attach("blog", "postgres://localhost:5432/blog")
catalog.sql("INSERT INTO events SELECT * FROM blog.ahoy_events")
```

Query the data

```ruby
catalog.sql("SELECT COUNT(*) FROM events").to_a
```

## Namespaces

List namespaces

```ruby
catalog.list_namespaces
```

Create a namespace

```ruby
catalog.create_namespace("main")
```

Check if a namespace exists

```ruby
catalog.namespace_exists?("main")
```

Drop a namespace

```ruby
catalog.drop_namespace("main")
```

## Tables

List tables

```ruby
catalog.list_tables
```

Check if a table exists

```ruby
catalog.table_exists?("events")
```

Drop a table

```ruby
catalog.drop_table("events")
```

## Snapshots

Get snapshots for a table

```ruby
catalog.snapshots("events")
```

Query the data at a specific snapshot version or time

```ruby
catalog.sql("SELECT * FROM events AT (VERSION => ?)", [3])
# or
catalog.sql("SELECT * FROM events AT (TIMESTAMP => ?)", [Date.today - 7])
```

## SQL Safety

Use parameterized queries when possible

```ruby
catalog.sql("SELECT * FROM events WHERE id = ?", [1])
```

For places that do not support parameters, use `quote` or `quote_identifier`

```ruby
quoted_table = catalog.quote_identifier("events")
quoted_file = catalog.quote("path/to/data.csv")
catalog.sql("COPY #{quoted_table} FROM #{quoted_file}")
```

## History

View the [changelog](https://github.com/ankane/seaduck/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/seaduck/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/seaduck/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/seaduck.git
cd seaduck
bundle install

# REST catalog
docker compose up
bundle exec rake test:rest

# S3 Tables catalog
bundle exec rake test:s3tables

# Glue catalog
bundle exec rake test:glue
```
