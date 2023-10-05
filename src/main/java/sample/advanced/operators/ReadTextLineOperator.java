package sample.advanced.operators;

import java.io.BufferedReader;
import java.util.function.Function;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import sample.advanced.domain.IngestionSource;
import sample.shared.fs.TextLineFormat;

public final class ReadTextLineOperator
    implements
    Function<
        SingleOutputStreamOperator<IngestionSource<Path>>,
        SingleOutputStreamOperator<IngestionSource<String>>>,
    FlatMapFunction<IngestionSource<Path>, IngestionSource<String>> {

  private static final TextLineFormat FORMAT = new TextLineFormat();

  @Override
  public SingleOutputStreamOperator<IngestionSource<String>> apply(
      SingleOutputStreamOperator<IngestionSource<Path>> in) {
    return in
        .flatMap(this)
        .name("read text lines");
  }

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
