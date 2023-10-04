package sample.advanced.operators;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;
import sample.advanced.domain.IngestionSource;
import sample.shared.jdbc.PreparedStatementCommand;

import javax.annotation.Nonnull;

public final class AddIngestionLogEntryJdbcRichMapFunction
    extends RichMapFunction<Path, IngestionSource<Path>> {

  private final JdbcConnectionOptions options;

  private transient PreparedStatementCommand preparedStatementCommand;

  public AddIngestionLogEntryJdbcRichMapFunction(
      @Nonnull JdbcConnectionOptions options) {
    this.options = options;
  }

  @Override
  public IngestionSource<Path> map(Path path) throws Exception {
    PreparedStatement preparedStatement = getPreparedStatement();
    preparedStatement.setString(1, path.getPath());
    try (ResultSet rs = preparedStatement.executeQuery()) {
      Preconditions.checkArgument(rs.next());
      return new IngestionSource<>(rs.getInt(1), path);
    }
  }

  @Override
  public void close() {
    if (preparedStatementCommand != null) {
      preparedStatementCommand.close();
      preparedStatementCommand = null;
    }
  }

  private PreparedStatement getPreparedStatement()
      throws SQLException, ClassNotFoundException {
    if (preparedStatementCommand == null) {
      preparedStatementCommand = new PreparedStatementCommand(
          "INSERT INTO ingestion_log_entry (path, status_id)" +
          " VALUES (?, 20) RETURNING id",
          options);
    }

    return preparedStatementCommand.getPreparedStatement();
  }
}
