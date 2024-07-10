package com.oltpbenchmark.benchmarks.temporal;

import java.time.LocalDate;

/** Keeps track of this position's progress, so we can add new things accordingly. */
public final class Position {
  public int positionId;
  public int employeeId;
  public LocalDate lastPromoted;
  public String duty;
  public int rank;

  public Position(int positionId, int employeeId, String duty, LocalDate assigned) {
    this.positionId = positionId;
    this.employeeId = employeeId;
    this.duty = duty;
    this.rank = 1;
    this.lastPromoted = assigned;
  }

  public void promote(LocalDate asof) {
    this.rank = this.rank + 1;
    this.lastPromoted = asof;
  }
}
