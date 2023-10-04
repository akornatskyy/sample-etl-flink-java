package sample.advanced.operators;

import java.io.BufferedReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import sample.advanced.domain.IngestionSource;
import sample.shared.fs.TextLineFormat;

public final class TextLineReaderFlatMapFunction
    implements FlatMapFunction<IngestionSource<Path>, IngestionSource<String>> {

  private static final TextLineFormat FORMAT = new TextLineFormat();

  @Override
  public void flatMap(
      IngestionSource<Path> ingestionSource,
      Collector<IngestionSource<String>> out) throws Exception {
    try (BufferedReader reader = FORMAT.createReader(ingestionSource.input)) {
      Integer id = ingestionSource.id;
      String line;
      while ((line = reader.readLine()) != null) {
        out.collect(new IngestionSource<>(id, line));
      }
    }
  }
}
