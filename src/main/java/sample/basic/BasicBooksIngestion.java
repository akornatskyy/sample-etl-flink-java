package sample.basic;

import java.io.IOException;
import java.util.Optional;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.basic.domain.Book;
import sample.basic.operators.BookDeserializer;
import sample.shared.fs.FileExtensionFilter;
import sample.shared.fs.PathScanner;
import sample.basic.sinks.BookJdbcSink;

public final class BasicBooksIngestion {
  private static final Logger LOGGER = LogManager.getLogger();

  private final BasicBooksIngestion.Params params;

  public BasicBooksIngestion(ParameterTool params) {
    this.params = new Params(params);
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    addBooksIngestion(env, ParameterTool.fromArgs(args));

    LOGGER.info("execute");
    JobExecutionResult result = env.execute("Sample Books Basic ETL Job");
    LOGGER.info("done, {} ms", result.getNetRuntime());
  }

  public static void addBooksIngestion(
      StreamExecutionEnvironment env, ParameterTool params) throws IOException {
    LOGGER.info("params: {}", params.toMap());
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    BasicBooksIngestion ingestion = new BasicBooksIngestion(params);
    ingestion.addBooksIngestion(env);
  }

  public static void addBooksIngestion(
      SingleOutputStreamOperator<String> source,
      SinkFunction<Book> sink) {
    source.rebalance()
        .map(new BookDeserializer())
        .name("parse book from a json line")
        .addSink(sink)
        .name("persist to storage");
  }

  void addBooksIngestion(
      StreamExecutionEnvironment env) throws IOException {
    addBooksIngestion(
        createDataStreamSource(env),
        createSinkFunction());
  }

  DataStreamSource<String> createDataStreamSource(
      StreamExecutionEnvironment env) throws IOException {
    PathScanner pathScanner = new PathScanner(
        new FileExtensionFilter(".json.gz"));
    Path[] paths = pathScanner
        .scan(new Path(params.inputDir))
        .toArray(new Path[0]);
    LOGGER.info("paths found: {}", paths.length);
    return env.fromSource(
        FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                paths)
            .build(),
        WatermarkStrategy.noWatermarks(),
        "read source");
  }

  SinkFunction<Book> createSinkFunction() {
    return BookJdbcSink.sink(
        JdbcExecutionOptions.builder()
            .withBatchSize(100)
            .withBatchIntervalMs(200)
            .withMaxRetries(5)
            .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(params.dbUrl)
            .withDriverName("org.postgresql.Driver")
            .withUsername("postgres")
            .build());
  }

  static class Params {
    public final String inputDir;
    public final String dbUrl;

    Params(ParameterTool params) {
      inputDir = Optional.ofNullable(params.get("input-dir"))
          .orElse("./");
      dbUrl = Optional.ofNullable(params.get("db-url"))
          .orElse("jdbc:postgresql://localhost:5432/books");
    }
  }
}
