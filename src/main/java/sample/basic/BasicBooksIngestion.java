package sample.basic;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.basic.streams.BooksIngestionStream;

public final class BasicBooksIngestion {
  private static final Logger LOGGER = LogManager.getLogger();

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    BooksIngestionStream
        .getStream(BooksIngestionStream.Options.fromArgs(args))
        .accept(env);

    LOGGER.info("execute");
    JobExecutionResult result = env.execute("Sample Books Basic ETL Job");
    LOGGER.info("done, {} ms", result.getNetRuntime());
  }
}
