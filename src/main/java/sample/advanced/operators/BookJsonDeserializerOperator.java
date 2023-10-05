package sample.advanced.operators;

import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import sample.advanced.domain.IngestionSource;
import sample.basic.domain.Book;

public final class BookJsonDeserializerOperator
    implements
    Function<
        DataStream<IngestionSource<String>>,
        SingleOutputStreamOperator<IngestionSource<Book>>>,
    MapFunction<IngestionSource<String>, IngestionSource<Book>> {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .registerModule(new JavaTimeModule());

  @Override
  public SingleOutputStreamOperator<IngestionSource<Book>> apply(
      DataStream<IngestionSource<String>> in) {
    return in
        .map(this)
        .name("parse book from a json line");
  }

  @Override
  public IngestionSource<Book> map(
      IngestionSource<String> source) throws Exception {
    return new IngestionSource<>(
        source.id,
        MAPPER.readValue(source.input, Book.class));
  }
}
