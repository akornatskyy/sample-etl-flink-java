package sample.basic.sinks;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sample.basic.domain.Book;

public final class BookJdbcSink {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  public static SinkFunction<Book> sink(
      JdbcExecutionOptions executionOptions,
      JdbcConnectionOptions connectionOptions) {
    return JdbcSink.sink(
        "INSERT INTO book (" +
        " asin, isbn, answered_questions, availability, brand," +
        " currency, date_first_available, delivery, description," +
        " discount, domain, features, final_price, formats, image_url," +
        " images_count, initial_price, item_weight, manufacturer," +
        " model_number, plus_content, product_dimensions, rating," +
        " reviews_count, root_bs_rank, seller_id, seller_name, timestamp," +
        " title, url, video, video_count, categories, best_sellers_rank" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?," +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)" +
        "ON CONFLICT (asin)" +
        "DO UPDATE SET " +
        " isbn = excluded.isbn," +
        " answered_questions = excluded.answered_questions," +
        " availability = excluded.availability," +
        " brand = excluded.brand," +
        " currency = excluded.currency," +
        " date_first_available = excluded.date_first_available," +
        " delivery = excluded.delivery," +
        " description = excluded.description," +
        " discount = excluded.discount," +
        " domain = excluded.domain," +
        " features = excluded.features," +
        " final_price = excluded.final_price," +
        " formats = excluded.formats::jsonb," +
        " image_url = excluded.image_url," +
        " images_count = excluded.images_count," +
        " initial_price = excluded.initial_price," +
        " item_weight = excluded.item_weight," +
        " manufacturer = excluded.manufacturer," +
        " model_number = excluded.model_number," +
        " plus_content = excluded.plus_content," +
        " product_dimensions = excluded.product_dimensions," +
        " rating = excluded.rating," +
        " reviews_count = excluded.reviews_count," +
        " root_bs_rank = excluded.root_bs_rank," +
        " seller_id = excluded.seller_id," +
        " seller_name = excluded.seller_name," +
        " timestamp = excluded.timestamp," +
        " title = excluded.title," +
        " url = excluded.url," +
        " video = excluded.video," +
        " video_count = excluded.video_count," +
        " categories = excluded.categories," +
        " best_sellers_rank = excluded.best_sellers_rank::jsonb",
        BookJdbcSink::accept,
        executionOptions,
        connectionOptions
    );
  }

  static void accept(PreparedStatement s, Book b) throws SQLException {
    s.setString(1, b.asin);
    s.setString(2, b.isbn);
    s.setInt(3, b.answeredQuestions);
    s.setString(4, b.availability);
    s.setString(5, b.brand);
    s.setString(6, b.currency);
    s.setDate(7, b.dateFirstAvailable != null
                 ? Date.valueOf(b.dateFirstAvailable) : null);
    Connection connection = s.getConnection();
    s.setArray(8, connection.createArrayOf("varchar", b.delivery.toArray()));
    s.setString(9, b.description);
    s.setBigDecimal(10, b.discount);
    s.setString(11, b.domain);
    s.setArray(12, connection.createArrayOf("varchar", b.features.toArray()));
    s.setBigDecimal(13, b.finalPrice);
    s.setObject(14, asJsonString(b.formats));
    s.setString(15, b.imageUrl);
    s.setInt(16, b.imagesCount);
    s.setBigDecimal(17, b.initialPrice);
    s.setString(18, b.itemWeight);
    s.setString(19, b.manufacturer);
    s.setString(20, b.modelNumber);
    s.setBoolean(21, b.plusContent);
    s.setString(22, b.productDimensions);
    s.setString(23, b.rating);
    s.setInt(24, b.reviewsCount);
    s.setInt(25, b.rootBsRank);
    s.setString(26, b.sellerId);
    s.setString(27, b.sellerName);
    s.setTimestamp(28, Timestamp.from(b.timestamp));
    s.setString(29, b.title);
    s.setString(30, b.url);
    s.setBoolean(31, b.video);
    s.setInt(32, b.videoCount);
    s.setArray(33, connection.createArrayOf("varchar", b.categories.toArray()));
    s.setObject(34, asJsonString(b.bestSellersRank));
  }

  static <T> String asJsonString(T input) {
    try {
      return MAPPER.writeValueAsString(input);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
