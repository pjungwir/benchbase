package com.oltpbenchmark.benchmarks.temporal;

import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Postgresql daterange value.
 *
 * <p>This is to work around no rangetype support in the jdbc driver.
 *
 * <p>N.B. We don't bother with closed/open and just assume everything follows [x,y) (except for
 * unbounded).
 */
public class DateRange {

  public static DateRange EMPTY_RANGE = new DateRange();

  private static final Pattern RE_DATE_RANGE =
      Pattern.compile("^(empty|[\\[(](\\d{4}-\\d{2}-\\d{2})?,(\\d{4}-\\d{2}-\\d{2})?\\))$");

  public final boolean isEmpty;
  public final LocalDate from;
  public final LocalDate til;

  private DateRange() {
    this.isEmpty = true;
    this.from = null;
    this.til = null;
  }

  public DateRange(LocalDate from, LocalDate til) {
    this.isEmpty = false;
    this.from = from;
    this.til = til;
  }

  public static DateRange parse(String s) {
    Matcher m = RE_DATE_RANGE.matcher(s);
    if (!m.matches()) throw new RuntimeException("Invalid DateRange: " + s);

    if (m.group(1).equals("empty")) return EMPTY_RANGE;

    LocalDate from = null;
    LocalDate til = null;

    if (m.group(2) != null && !m.group(2).equals("")) from = LocalDate.parse(m.group(2));

    if (m.group(3) != null && !m.group(3).equals("")) til = LocalDate.parse(m.group(3));

    return new DateRange(from, til);
  }
}
