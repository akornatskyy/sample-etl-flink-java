package sample.basic.streams;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sample.basic.domain.Book;
import sample.basic.operators.BookJsonDeserializerOperator;
import sample.basic.sinks.BookJdbcSink;
import sample.basic.sources.BookDataStreamSource;

public final class BooksIngestionStream
    implements
    Consumer<StreamExecutionEnvironment>,
    Function<
        DataStreamSource<String>,
        SingleOutputStreamOperator<Book>> {

  private final BooksIngestionStream.Options options;

  BooksIngestionStream(Options options) {
    this.options = options;
  }

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
    // NOTE: to even input distribution across deserializer (due to files of
    // different size, and/or different number of records; aiming a better
    // spread across available task slots), consider to rebalance source
    // (e.g. `source.rebalance()`).
    return new BookJsonDeserializerOperator()
        .apply(source);
  }

  public static class Options {
    public final Path inputDir;
    public final Options.Jdbc jdbc;

    Options(ParameterTool params) {
      inputDir = new Path(
          Optional.ofNullable(params.get("input-dir")).orElse("./"));
      jdbc = new Options.Jdbc(params);
    }

    public static Options fromArgs(String[] args) {
      return new Options(ParameterTool.fromArgs(args));
    }

    public static class Jdbc {
      public final JdbcConnectionOptions connection;
      public final JdbcExecutionOptions execution;

      Jdbc(ParameterTool params) {
        connection = new JdbcConnectionOptions
            .JdbcConnectionOptionsBuilder()
            .withUrl(
                Optional.ofNullable(params.get("db-url")).orElse(
                    "jdbc:postgresql://localhost:5432/books?user=postgres"))
            .withDriverName("org.postgresql.Driver")
            .build();
        execution = JdbcExecutionOptions.builder()
            .withBatchSize(100)
            .withBatchIntervalMs(200)
            .withMaxRetries(5)
            .build();
      }
    }
  }
}
