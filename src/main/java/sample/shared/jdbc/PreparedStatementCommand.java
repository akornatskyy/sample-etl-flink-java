package sample.shared.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class PreparedStatementCommand implements AutoCloseable {
  private final String sql;
  private final JdbcConnectionOptions options;

  private Class<?> loadedDriverClass;
  private PreparedStatement statement;

  public PreparedStatementCommand(
      @Nonnull String sql,
      @Nonnull JdbcConnectionOptions options) {
    this.sql = sql;
    this.options = options;
  }

  public @Nonnull PreparedStatement getPreparedStatement()
      throws SQLException, ClassNotFoundException {
    if (statement != null) {
      return statement;
    }

    String driverName = options.getDriverName();
    if (driverName != null && loadedDriverClass == null) {
      loadedDriverClass = Class.forName(driverName);
    }

    Connection connection = DriverManager.getConnection(
        options.getDbURL(),
        options.getUsername().orElse(null),
        options.getPassword().orElse(null));
    statement = connection.prepareStatement(sql);
    return statement;
  }

  @Override
  public void close() {
    if (statement != null) {
      try {
        IOUtils.closeAllQuietly(statement, statement.getConnection());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      statement = null;
    }
  }
}
