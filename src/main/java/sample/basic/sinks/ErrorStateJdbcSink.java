package sample.basic.sinks;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sample.basic.domain.ErrorState;

public final class ErrorStateJdbcSink
    implements
    Function<SideOutputDataStream<ErrorState>, DataStreamSink<ErrorState>> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final JdbcExecutionOptions executionOptions;
  private final JdbcConnectionOptions connectionOptions;

  public ErrorStateJdbcSink(
      JdbcExecutionOptions executionOptions,
      JdbcConnectionOptions connectionOptions) {
    this.executionOptions = executionOptions;
    this.connectionOptions = connectionOptions;
  }

  @Override
  public DataStreamSink<ErrorState> apply(SideOutputDataStream<ErrorState> in) {
    return in
        .addSink(sink(executionOptions, connectionOptions))
        .name("persist error state");
  }

  static SinkFunction<ErrorState> sink(
      JdbcExecutionOptions executionOptions,
      JdbcConnectionOptions connectionOptions) {
    return JdbcSink.sink(
        "INSERT INTO error_state (" +
        " domain, reference, violations" +
        ") VALUES (?, ?, ?::jsonb)",
        ErrorStateJdbcSink::accept,
        executionOptions,
        connectionOptions
    );
  }

  static void accept(
      PreparedStatement s, ErrorState e) throws SQLException {
    s.setString(1, e.domain);
    s.setString(2, e.reference);
    s.setObject(3, asJsonString(e.violations));
  }

  static <T> String asJsonString(T input) {
    try {
      return MAPPER.writeValueAsString(input);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
