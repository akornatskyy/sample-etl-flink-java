package sample.basic.domain;

import java.util.ArrayList;
import java.util.List;

public final class BookValidator {
  private static final int ISBN_MAX_LENGTH = 10;

  private static final String REQUIRED = "Required field cannot be left blank";
  private static final String MAX_LENGTH = "Exceeds maximum length of %d.";

  public static List<DomainViolation> validate(Book book) {
    List<DomainViolation> violations = new ArrayList<>();

    // if (book.isbn == null || book.isbn.isEmpty()) {
    //      violations.add(new DomainViolation(
    //          "isbn", REQUIRED));
    // }

    if (book.isbn != null && book.isbn.length() > ISBN_MAX_LENGTH) {
      violations.add(new DomainViolation(
          "isbn", String.format(MAX_LENGTH, ISBN_MAX_LENGTH)));
    }

    if (book.currency == null || book.currency.isEmpty()) {
      violations.add(new DomainViolation(
          "currency", REQUIRED));
    }

    return violations;
  }
}
