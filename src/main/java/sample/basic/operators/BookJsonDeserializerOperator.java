package sample.basic.operators;

import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import sample.basic.domain.Book;

public final class BookJsonDeserializerOperator
    implements
    Function<
        DataStream<String>,
        SingleOutputStreamOperator<Book>>,
    MapFunction<String, Book> {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .registerModule(new JavaTimeModule());

  @Override
  public SingleOutputStreamOperator<Book> apply(DataStream<String> in) {
    return in
        .map(this)
        .name("parse book from a json line");
  }

  @Override
  public Book map(String value) throws JsonProcessingException {
    return MAPPER.readValue(value, Book.class);
  }
}
