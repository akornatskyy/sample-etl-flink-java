package sample.advanced;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.advanced.domain.IngestionSource;
import sample.advanced.operators.AddIngestionLogEntryJdbcRichMapFunction;
import sample.advanced.operators.BookJsonDeserializerMapFunction;
import sample.advanced.operators.TextLineReaderFlatMapFunction;
import sample.advanced.sinks.BookIngestionJdbcSink;
import sample.basic.domain.Book;
import sample.shared.fs.FileExtensionFilter;
import sample.shared.fs.PathScanner;

public final class AdvancedBooksIngestion {
  private static final Logger LOGGER = LogManager.getLogger();

  private final Options options;

  public AdvancedBooksIngestion(ParameterTool params) {
    this.options = new Options(params);
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    addBooksIngestion(env, ParameterTool.fromArgs(args));

    LOGGER.info("execute");
    JobExecutionResult result = env.execute("Sample Books Advanced ETL Job");
    LOGGER.info("done, {} ms", result.getNetRuntime());
  }

  public static void addBooksIngestion(
      StreamExecutionEnvironment env, ParameterTool params) throws IOException {
    LOGGER.info("params: {}", params.toMap());
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    AdvancedBooksIngestion ingestion = new AdvancedBooksIngestion(params);
    ingestion.addBooksIngestion(env);
  }

  public void addBooksIngestion(
      SingleOutputStreamOperator<Path> source,
      SinkFunction<IngestionSource<Book>> sink) {
    source
        .map(new AddIngestionLogEntryJdbcRichMapFunction(
            options.jdbc.connection))
        .setParallelism(1)
        .name("add ingestion log entry")
        .flatMap(new TextLineReaderFlatMapFunction())
        .name("read text lines")
        .map(new BookJsonDeserializerMapFunction())
        .name("parse book from a json line")
        .addSink(sink)
        .name("persist to storage");
  }

  public void addBooksIngestion(
      StreamExecutionEnvironment env) throws IOException {
    addBooksIngestion(
        createDataStreamSource(env),
        createSinkFunction());
  }

  public DataStreamSource<Path> createDataStreamSource(
      StreamExecutionEnvironment env) throws IOException {
    PathScanner pathScanner = new PathScanner(
        new FileExtensionFilter(".json.gz"));
    Collection<Path> paths = pathScanner.scan(options.inputDir);
    LOGGER.info("paths found: {}", paths.size());
    return (DataStreamSource<Path>) env.fromCollection(paths)
        .name("read source paths");
  }

  public SinkFunction<IngestionSource<Book>> createSinkFunction() {
    return BookIngestionJdbcSink.sink(
        options.jdbc.execution,
        options.jdbc.connection);
  }

  public static class Options {
    public final Path inputDir;
    public final Jdbc jdbc;

    Options(ParameterTool params) {
      inputDir = new Path(
          Optional.ofNullable(params.get("input-dir")).orElse("./"));
      jdbc = new Jdbc(params);
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

