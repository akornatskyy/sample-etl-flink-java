package sample.shared.fs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public final class PathScanner {
  private final Set<String> extensions;

  public PathScanner(String ...extensions) {
    this.extensions = new HashSet<>();
    this.extensions.addAll(Arrays.asList(extensions));
  }

  public Stream<Path> scan(Path rootPath) throws IOException {
    FileSystem fs = rootPath.getFileSystem();
    FileStatus[] statuses = fs.listStatus(rootPath);
    return Arrays.stream(statuses)
        .flatMap(f -> {
          if (f.isDir()) {
            try {
              return scan(f.getPath());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          return Stream.of(f.getPath());
        })
        .filter(path -> {
          String fullPath = path.getPath();
          return extensions.stream().anyMatch(fullPath::endsWith);
        });
  }
}
