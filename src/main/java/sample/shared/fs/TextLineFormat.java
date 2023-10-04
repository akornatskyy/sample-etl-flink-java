package sample.shared.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;

public final class TextLineFormat {
  public static final String DEFAULT_CHARSET_NAME = "UTF-8";

  private final String charsetName;

  public TextLineFormat() {
    this(DEFAULT_CHARSET_NAME);
  }

  public TextLineFormat(String charsetName) {
    this.charsetName = charsetName;
  }

  public BufferedReader createReader(Path p) throws IOException {
    final InflaterInputStreamFactory<?> deCompressor =
        StandardDeCompressors.getDecompressorForFileName(p.getPath());
    final FSDataInputStream stream = p.getFileSystem().open(p);
    return Utils.doWithCleanupOnException(stream, () ->
        new BufferedReader(
            new InputStreamReader(
                deCompressor == null ? stream : deCompressor.create(stream),
                charsetName)));
  }
}
