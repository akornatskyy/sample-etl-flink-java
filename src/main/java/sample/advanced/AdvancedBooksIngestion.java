package sample.advanced;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.advanced.domain.IngestionSource;
import sample.advanced.operators.AddIngestionLogEntryJdbcOperator;
import sample.advanced.operators.BookJsonDeserializerOperator;
import sample.advanced.operators.ReadTextLineOperator;
import sample.advanced.sinks.BookIngestionJdbcSink;
import sample.advanced.sources.BookDataStreamSource;
import sample.basic.domain.Book;

public final class AdvancedBooksIngestion
    implements
    Consumer<StreamExecutionEnvironment>,
    Function<
        DataStreamSource<Path>,
        SingleOutputStreamOperator<IngestionSource<Book>>> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final Options options;

  public AdvancedBooksIngestion(ParameterTool params) {
    this.options = new Options(params);
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    new AdvancedBooksIngestion(ParameterTool.fromArgs(args))
        .accept(env);

    LOGGER.info("execute");
    JobExecutionResult result = env.execute("Sample Books Advanced ETL Job");
    LOGGER.info("done, {} ms", result.getNetRuntime());
  }

  @Override
  public void accept(StreamExecutionEnvironment env) {
    new BookDataStreamSource(options.inputDir)
        .andThen(this)
        .andThen(new BookIngestionJdbcSink(
            options.jdbc.execution,
            options.jdbc.connection))
        .apply(env);
  }

  @Override
  public SingleOutputStreamOperator<IngestionSource<Book>> apply(
      DataStreamSource<Path> source) {
    // NOTE: to even input distribution across deserializer (due to files of
    // different size, and/or different number of records; aiming a better
    // spread across available task slots), consider to rebalance source.
    return new AddIngestionLogEntryJdbcOperator(options.jdbc.connection)
        .andThen(new ReadTextLineOperator())
        // .andThen(DataStream::rebalance)
        .andThen(new BookJsonDeserializerOperator())
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
