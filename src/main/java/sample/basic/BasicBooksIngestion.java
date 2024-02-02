package sample.basic;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sample.basic.streams.BooksIngestionStream;

public final class BasicBooksIngestion {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    if (params.getBoolean("disable-operator-chaining", false)) {
      env.disableOperatorChaining();
    }

    BooksIngestionStream
        .getStream(BooksIngestionStream.Options.fromArgs(args))
        .accept(env);

    env.execute("Sample Books Basic ETL Job");
  }
}
