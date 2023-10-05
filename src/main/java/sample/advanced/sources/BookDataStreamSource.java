package sample.advanced.sources;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.shared.fs.FileExtensionFilter;
import sample.shared.fs.PathScanner;

public final class BookDataStreamSource
    implements Function<StreamExecutionEnvironment, DataStreamSource<Path>> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final Path inputDir;

  public BookDataStreamSource(Path inputDir) {
    this.inputDir = inputDir;
  }

  @Override
  public DataStreamSource<Path> apply(StreamExecutionEnvironment env) {
    Collection<Path> paths = scan(inputDir);
    LOGGER.info("paths found: {}", paths.size());
    return (DataStreamSource<Path>) env.fromCollection(paths)
        .name("read source paths");
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
