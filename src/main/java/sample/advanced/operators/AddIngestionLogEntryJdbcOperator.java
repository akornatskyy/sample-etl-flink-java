package sample.advanced.operators;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Function;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import retry.ExpBackoff;
import retry.RetryOptions;
import retry.RetryThrowingSupplier;
import sample.advanced.domain.IngestionSource;
import sample.shared.jdbc.PreparedStatementCommand;

public final class AddIngestionLogEntryJdbcOperator
    extends RichMapFunction<Path, IngestionSource<Path>>
    implements Function<
    DataStream<Path>,
    SingleOutputStreamOperator<IngestionSource<Path>>> {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final RetryOptions RETRY_OPTIONS = RetryOptions.builder()
      .max(5)
      .backoff(
          ExpBackoff.builder()
              .initial(Duration.ofSeconds(1))
              .multiplier(1.5)
              .factor(0.25)
              .build())
      .build();

  private final JdbcConnectionOptions options;

  private transient PreparedStatementCommand preparedStatementCommand;

  public AddIngestionLogEntryJdbcOperator(JdbcConnectionOptions options) {
    this.options = options;
  }

  @Override
  public SingleOutputStreamOperator<IngestionSource<Path>> apply(
      DataStream<Path> in) {
    return in
        .map(this)
        .setParallelism(1)
        .name("add ingestion log entry");
  }

  @Override
  public IngestionSource<Path> map(Path path) throws Exception {
    return RetryThrowingSupplier.get(
        () -> {
          PreparedStatement preparedStatement = getPreparedStatement();
          preparedStatement.setString(1, path.getPath());
          try (ResultSet rs = preparedStatement.executeQuery()) {
            Preconditions.checkArgument(rs.next());
            return new IngestionSource<>(rs.getInt(1), path);
          }
        },
        (r, ex) -> {
          boolean retry = ex instanceof SQLException;
          if (retry) {
            LOGGER.warn(ex.getMessage());
            if (preparedStatementCommand != null &&
                !preparedStatementCommand.isValid()) {
              preparedStatementCommand.close();
            }
          }

          return retry;
        },
        RETRY_OPTIONS);
  }

  @Override
  public void close() {
    if (preparedStatementCommand != null) {
      preparedStatementCommand.close();
      preparedStatementCommand = null;
    }
  }

  private PreparedStatement getPreparedStatement() throws SQLException {
    if (preparedStatementCommand == null) {
      preparedStatementCommand = new PreparedStatementCommand(
          "INSERT INTO ingestion_log_entry (path, status_id)" +
          " VALUES (?, 20) RETURNING id",
          options);
    }

    return preparedStatementCommand.getPreparedStatement();
  }
}
