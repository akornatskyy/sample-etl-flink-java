package sample.basic.sources;

import java.io.IOException;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sample.basic.shared.fs.FileExtensionFilter;
import sample.basic.shared.fs.PathScanner;

public final class BookFileSource {
  private static final Logger LOGGER = LogManager.getLogger();

  public static FileSource<String> forTextLineInputFormat(
      String rootPath,
      String... extensions) throws IOException {
    LOGGER.info("root: {}, extensions: {}", rootPath, extensions);

    Path[] paths = new PathScanner(new FileExtensionFilter(extensions))
        .scan(new Path(rootPath))
        .toArray(new Path[0]);
    LOGGER.info("paths found: {}", paths.length);

    return FileSource.forRecordStreamFormat(
            new TextLineInputFormat(),
            paths)
        .build();
  }
}
