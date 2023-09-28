package sample;

import java.util.Optional;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.domain.Book;
import sample.operators.BookDeserializer;
import sample.sinks.BookJdbcSink;
import sample.sources.BookFileSource;

public class DataStreamJob {
  private static final Logger LOGGER = LogManager.getLogger();

  private final Source<String, ?, ?> source;
  private final SinkFunction<Book> sink;

  public DataStreamJob(Source<String, ?, ?> source, SinkFunction<Book> sink) {
    this.source = source;
    this.sink = sink;
  }

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    LOGGER.info("started, params: {}", params.toMap());

    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    DataStreamJob job = new DataStreamJob(
        BookFileSource.forTextLineInputFormat(
            Optional.ofNullable(params.get("input-dir")).orElse("./"),
            ".json.gz"),
        BookJdbcSink.sink(
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(
                    Optional.ofNullable(params.get("db-url"))
                        .orElse("jdbc:postgresql://localhost:5432/books"))
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .build()
        )
    );

    LOGGER.info("execute");
    JobExecutionResult result = job.execute(env);
    LOGGER.info("done, {} ms", result.getNetRuntime());
  }

  public JobExecutionResult execute(
      StreamExecutionEnvironment env) throws Exception {
    env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "read source")
        .rebalance()
        .map(new BookDeserializer())
        .name("parse book from a json line")
        .addSink(sink)
        .name("persist to storage");
    return env.execute("Sample Books ETL Job");
  }
}
