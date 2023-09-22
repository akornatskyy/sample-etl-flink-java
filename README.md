# sample-etl-flink-java

[![tests](https://github.com/akornatskyy/sample-etl-flink-java/actions/workflows/tests.yaml/badge.svg)](https://github.com/akornatskyy/sample-etl-flink-java/actions/workflows/tests.yaml)

The sample ingests multiline gzipped files of popular books into postgres.

## Prepare

Download and prepare dataset (as a multiline JSON file):

```sh
curl -sL https://github.com/luminati-io/Amazon-popular-books-dataset/raw/main/Amazon_popular_books_dataset.json | \
  jq -c '.[]' > dataset.json
```

Split the input (multiline JSON) file into parts with 400 lines per output file
and compress with gzip:

```sh
cat dataset.json | split -e -l400 -d --additional-suffix .json \
  --filter='gzip > $FILE.gz' - part_
```

## Postgres

There are a number of ways to run postgres, if you prefer to download binary and
run locally without installation, use the following steps:

```sh
bin/initdb --pgdata=data/ -U postgres -E 'UTF-8' \
  --lc-collate='en_US.UTF-8' --lc-ctype='en_US.UTF-8'
bin/postgres -D data/
```

Create *books* database and apply schema from *./misc/schema.sql*.

## Run

Optionally specify *--input-dir* for a directory to scan for input and/or a
connection to postgres (*--db-url*).

```sh
flink run -p 4 target/sample-etl-flink-java-1.0-SNAPSHOT.jar

flink run -p 4 target/sample-etl-flink-java-1.0-SNAPSHOT.jar \
  --input-dir ./ --db-url jdbc:postgresql://localhost:5432/books
```

Running from IntelliJ IDEA requires to edit run configuration to add 
dependencies of *provided* scope to classpath.

## References

- [Amazon Popular Books Dataset](https://github.com/luminati-io/Amazon-popular-books-dataset)