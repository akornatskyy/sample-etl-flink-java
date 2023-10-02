package sample.shared.operators;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IOUtils;

public final class TextLineInput implements FlatMapFunction<Path, String> {
  public static final String DEFAULT_CHARSET_NAME = "UTF-8";

  private final String charsetName;

  public TextLineInput() {
    this(DEFAULT_CHARSET_NAME);
  }

  public TextLineInput(String charsetName) {
    this.charsetName = charsetName;
  }

  @Override
  public void flatMap(Path p, Collector<String> out) throws Exception {
    final InflaterInputStreamFactory<?> deCompressor =
        StandardDeCompressors.getDecompressorForFileName(p.getPath());
    final FSDataInputStream stream = p.getFileSystem().open(p);
    try {
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  deCompressor == null ? stream : deCompressor.create(stream),
                  charsetName));
      String line;
      while ((line = reader.readLine()) != null) {
        out.collect(line);
      }
    } finally {
      IOUtils.closeQuietly(stream);
    }
  }
}
