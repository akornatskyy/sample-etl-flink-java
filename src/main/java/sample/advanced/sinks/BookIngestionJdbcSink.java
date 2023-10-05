package sample.advanced.sinks;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sample.advanced.domain.IngestionSource;
import sample.basic.domain.Book;
import sample.basic.sinks.BookJdbcSink;

public final class BookIngestionJdbcSink
    implements
    Function<
        SingleOutputStreamOperator<IngestionSource<Book>>,
        DataStreamSink<IngestionSource<Book>>> {

  private final JdbcExecutionOptions executionOptions;
  private final JdbcConnectionOptions connectionOptions;

  public BookIngestionJdbcSink(
      JdbcExecutionOptions executionOptions,
      JdbcConnectionOptions connectionOptions) {
    this.executionOptions = executionOptions;
    this.connectionOptions = connectionOptions;
  }

  @Override
  public DataStreamSink<IngestionSource<Book>> apply(SingleOutputStreamOperator<IngestionSource<Book>> in) {
    return in
        .addSink(sink(executionOptions, connectionOptions))
        .name("persist to storage");
  }

  static SinkFunction<IngestionSource<Book>> sink(
      JdbcExecutionOptions executionOptions,
      JdbcConnectionOptions connectionOptions) {
    return JdbcSink.sink(
        "INSERT INTO book_ingestion (" +
        " asin, isbn, answered_questions, availability, brand," +
        " currency, date_first_available, delivery, description," +
        " discount, domain, features, final_price, formats, image_url," +
        " images_count, initial_price, item_weight, manufacturer," +
        " model_number, plus_content, product_dimensions, rating," +
        " reviews_count, root_bs_rank, seller_id, seller_name, timestamp," +
        " title, url, video, video_count, categories, best_sellers_rank," +
        " ingestion_log_entry_id" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?," +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)",
        BookIngestionJdbcSink::accept,
        executionOptions,
        connectionOptions
    );
  }

  static void accept(
      PreparedStatement s, IngestionSource<Book> is) throws SQLException {
    BookJdbcSink.accept(s, is.input);
    s.setInt(35, is.id);
  }
}
