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

  private static Class<?> LOADED_DRIVER_CLASS;

  private final String sql;
  private final JdbcConnectionOptions options;

  private PreparedStatement statement;

  public PreparedStatementCommand(
      @Nonnull String sql,
      @Nonnull JdbcConnectionOptions options) {
    this.sql = sql;
    this.options = options;

    String driverName = options.getDriverName();
    if (driverName != null && LOADED_DRIVER_CLASS == null) {
      synchronized (JdbcConnectionOptions.class) {
        if (LOADED_DRIVER_CLASS == null) {
          try {
            LOADED_DRIVER_CLASS = Class.forName(driverName);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  public PreparedStatement getPreparedStatement() throws SQLException {
    if (statement != null) {
      return statement;
    }

    Connection connection = DriverManager.getConnection(
        options.getDbURL(),
        options.getUsername().orElse(null),
        options.getPassword().orElse(null));
    statement = connection.prepareStatement(sql);
    return statement;
  }

  public boolean isValid() {
    try {
      return statement != null && statement.getConnection().isValid(1000);
    } catch (SQLException ex) {
      throw new IllegalStateException("Failed to reconnect", ex);
    }
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
