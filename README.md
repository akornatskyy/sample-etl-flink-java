# sample-etl-flink-java

[![tests](https://github.com/akornatskyy/sample-etl-flink-java/actions/workflows/tests.yaml/badge.svg)](https://github.com/akornatskyy/sample-etl-flink-java/actions/workflows/tests.yaml)

The sample ingests multiline gzipped files of popular books into postgres.

## Prerequisites

Ensure JDK 8 or 11 is installed in your system:

```sh
java -version
```

Flink [runs](https://nightlies.apache.org/flink/flink-docs-stable/docs/try-flink/local_installation/)
on UNIX-like environments, for Windows install
[cygwin](https://www.cygwin.com/) (include *mintty* and *netcat* packages)
to emulate linux commands or use WSL (note, *bash for windows* doesn't work).

Ensure the following (file `~/.bash_profile`):

```sh
# ignore windows line endings (skip \r)
export SHELLOPTS
set -o igncr
```

Update the number of task slots that TaskManager offers and add id
(file `conf/flink-conf.yaml`):

```yaml
taskmanager.numberOfTaskSlots: 4
taskmanager.resource-id: local
```

Start cluster and navigate to the web UI at
[http://localhost:8081](http://localhost:8081):

```sh
start-cluster.sh
```

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

## Design

The design aims simplicity, reuse and maintainability, where components
(being that an operator, stream, source or sink) are *self-sufficient* and
*composable*.

This can be achieved with Java 8 functional interfaces, like
`Function<IN, OUT>` and `Consumer<T>`.

### Operator

Operator, also known as a Flink function or transformation:

- Implements `Function<DataStream<IN>, SingleOutputStreamOperator<OUT>>` from
`java.util.function`. Used to add itself into a data stream.
- Implements functional interface that extends `Function` from
`org.apache.flink.api.common.functions`, e.g. `FlatMapFunction<IN, OOUT>`, etc.,
or extends a rich equivalent from `AbstractRichFunction`, e.g.
`RichFlatMapFunction<IN, OUT>`, etc. Used to perform a transformation on stream
value.

Example (see [BookJsonDeserializerOperator.java](./src/main/java/sample/basic/operators/BookJsonDeserializerOperator.java)):

```java
public final class BookJsonDeserializerOperator
    implements
    Function<
        DataStream<String>,
        SingleOutputStreamOperator<Book>>,
    MapFunction<String, Book> {

  // ...

  @Override
  public SingleOutputStreamOperator<Book> apply(DataStream<String> in) {
    return in
        .map(this)
        .name("parse book from a json line");
  }

  @Override
  public Book map(String value) throws JsonProcessingException {
    return MAPPER.readValue(value, Book.class);
  }
}
```

### Source

Source (a data stream source):

- Implements `Function<StreamExecutionEnvironment, DataStreamSource<T>>`
from `java.util.function`. Used to add itself into `StreamExecutionEnvironment`,
the return type `DataStreamSource<T>` is used to chain stream transformations.

Example (see [BookDataStreamSource.java](./src/main/java/sample/basic/sources/BookDataStreamSource.java)):

```java
public final class BookDataStreamSource
    implements Function<StreamExecutionEnvironment, DataStreamSource<String>> {

  // ...

  @Override
  public DataStreamSource<String> apply(StreamExecutionEnvironment env) {
    Collection<Path> paths = scan(inputDir);
    return env.fromSource(
        FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                paths.toArray(new Path[0]))
            .build(),
        WatermarkStrategy.noWatermarks(),
        "read source");
  }
}
```

### Sink

Sink (a final destination of stream transformations):

- Implements `Function<DataStream<T>, DataStreamSink<T>` from
`java.util.function`. Used to add itself into `DataStream<T>`.

Example (see [BookJdbcSink.java](./src/main/java/sample/basic/sinks/BookJdbcSink.java)):

```java
public final class BookJdbcSink
    implements
    Function<DataStream<Book>, DataStreamSink<Book>> {

  // ...

  @Override
  public DataStreamSink<Book> apply(DataStream<Book> in) {
    return in
        .addSink(sink(executionOptions, connectionOptions))
        .name("persist to storage");
  }
}
```

### Stream

Stream (a Flink application, or a streaming dataflow):

- Exposes factory function `getStream(Options options)`. Used to pass
configuration options, e.g. `input-dir`, `db-url`, etc.
- Implements `Consumer<StreamExecutionEnvironment>` from
`java.util.function`. Used to add itself into `StreamExecutionEnvironment`.
- Implements `Function<DataStreamSource<IN>, SingleOutputStreamOperator<T>>`.
Used to compose a streaming flow of operators.

Example (see [BooksIngestionStream.java](./src/main/java/sample/basic/streams/BooksIngestionStream.java)):

```java
public final class BooksIngestionStream
    implements
    Consumer<StreamExecutionEnvironment>,
    Function<
        DataStreamSource<String>,
        SingleOutputStreamOperator<Book>> {

  // ...

  public static BooksIngestionStream getStream(Options options) {
    return new BooksIngestionStream(options);
  }

  @Override
  public void accept(StreamExecutionEnvironment env) {
    new BookDataStreamSource(options.inputDir)
        .andThen(this)
        .andThen(new BookJdbcSink(
            options.jdbc.execution,
            options.jdbc.connection))
        .apply(env);
  }

  @Override
  public SingleOutputStreamOperator<Book> apply(
      DataStreamSource<String> source) {
    return new BookJsonDeserializerOperator()
        //.andThen(...)
        //.andThen(...)
        .apply(source);
  }
}
```

### Options

Options class represents a stream dataflow configuration, which is usually
obtained from the application command line args or similar:

- Use POJO.
- Exposes factory function `fromArgs(String[] args)`. Used to parse
configuration options and set sensible defaults.

Example (see [BooksIngestionStream.java](./src/main/java/sample/basic/streams/BooksIngestionStream.java)):

```java
public final class BooksIngestionStream {

  // ...

  public static class Options {
    public final Path inputDir;

    Options(ParameterTool params) {
      inputDir = new Path(
          Optional.ofNullable(params.get("input-dir")).orElse("./"));
    }

    public static Options fromArgs(String[] args) {
      return new Options(ParameterTool.fromArgs(args));
    }
  }
}
```

### Entry Point

This is an entry point of Java application to initialize and execute Flink job.

Example (see [BasicBooksIngestion.java](./src/main/java/sample/basic/BasicBooksIngestion.java)):

```java
public final class BasicBooksIngestion {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    BooksIngestionStream
        .getStream(BooksIngestionStream.Options.fromArgs(args))
        .accept(env);

    env.execute("Sample Books Basic ETL Job");
  }
}
```

## References

- [Amazon Popular Books Dataset](https://github.com/luminati-io/Amazon-popular-books-dataset)
