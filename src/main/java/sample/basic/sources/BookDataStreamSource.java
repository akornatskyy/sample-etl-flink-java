package sample.basic.sources;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.shared.fs.FileExtensionFilter;
import sample.shared.fs.PathScanner;

public final class BookDataStreamSource
    implements Function<StreamExecutionEnvironment, DataStreamSource<String>> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final Path inputDir;

  public BookDataStreamSource(Path inputDir) {
    this.inputDir = inputDir;
  }

  @Override
  public DataStreamSource<String> apply(StreamExecutionEnvironment env) {
    Collection<Path> paths = scan(inputDir);
    LOGGER.info("paths found: {}", paths.size());
    return env.fromSource(
        FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                paths.toArray(new Path[0]))
            .build(),
        WatermarkStrategy.noWatermarks(),
        "read source");
  }

  static Collection<Path> scan(Path... paths) {
    PathScanner pathScanner = new PathScanner(
        new FileExtensionFilter(".json.gz"));
    try {
      return pathScanner.scan(paths);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
