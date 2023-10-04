package sample.advanced.domain;

public final class IngestionSource<T> {
  public final Integer id;
  public final T input;

  public IngestionSource(Integer id, T input) {
    this.id = id;
    this.input = input;
  }
}
