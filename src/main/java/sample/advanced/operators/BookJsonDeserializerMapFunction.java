package sample.advanced.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import sample.advanced.domain.IngestionSource;
import sample.basic.domain.Book;

public final class BookJsonDeserializerMapFunction
    implements MapFunction<IngestionSource<String>, IngestionSource<Book>> {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .registerModule(new JavaTimeModule());

  @Override
  public IngestionSource<Book> map(
      IngestionSource<String> source) throws JsonProcessingException {
    return new IngestionSource<>(
        source.id,
        MAPPER.readValue(source.input, Book.class));
  }
}
