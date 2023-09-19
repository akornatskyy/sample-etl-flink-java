package sample.sources;

import java.io.IOException;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.shared.fs.PathScanner;

public final class BookFileSource {
  private static final Logger LOGGER = LogManager.getLogger();

  public static FileSource<String> forTextLineInputFormat(
      String rootPath,
      String ...extensions) throws IOException {
    LOGGER.info("root: {}, extensions: {}", rootPath, extensions);
    Path[] paths = new PathScanner(extensions)
        .scan(new Path(rootPath))
        .toArray(Path[]::new);
    LOGGER.info("paths found: {}", paths.length);
    return FileSource.forRecordStreamFormat(
            new TextLineInputFormat(),
            paths)
        .build();
  }
}
