package sample.shared.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nonnull;

public final class PathScanner {
  private final Predicate<Path> fileFilter;

  public PathScanner(@Nonnull Predicate<Path> fileFilter) {
    this.fileFilter = fileFilter;
  }

  public Collection<Path> scan(@Nonnull Path... paths) throws IOException {
    List<Path> result = new ArrayList<>();
    for (Path p : paths) {
      FileSystem fs = p.getFileSystem();
      scan(fs.getFileStatus(p), fs, result);
    }

    return result;
  }

  private void scan(FileStatus fileStatus, FileSystem fs, List<Path> result)
      throws IOException {
    Path p = fileStatus.getPath();
    if (fileStatus.isDir()) {
      for (FileStatus f : fs.listStatus(p)) {
        scan(f, fs, result);
      }

      return;
    }

    if (fileFilter.test(p)) {
      result.add(p);
    }
  }
}
