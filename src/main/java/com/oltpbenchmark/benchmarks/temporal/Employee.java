package com.oltpbenchmark.benchmarks.temporal;

import java.time.LocalDate;

/** Keeps track of this employee's progress, so we can add new things accordingly. */
public final class Employee {
  public int employeeId;
  public int salary;
  public LocalDate hired;
  public LocalDate lastRaised;

  public Employee(int employeeId, int salary, LocalDate hired) {
    this.employeeId = employeeId;
    this.salary = salary;
    this.hired = hired;
    this.lastRaised = hired;
  }

  public void raise(LocalDate asof) {
    this.salary = Math.round(this.salary * 1.02f);
    this.lastRaised = asof;
  }
}
