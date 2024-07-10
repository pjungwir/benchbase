package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.benchmarks.temporal.procedures.*;
import java.time.LocalDate;

public final class TemporalModel {

  public final LocalDate today;
  public final int startingEmployees;
  public final int startingPositions;
  private final Employee[] employees;
  private final Position[] positions;

  public TemporalModel(double scaleFactor) {
    this.today = LocalDate.now();
    this.startingEmployees = (int) Math.round(TemporalConstants.NUM_EMPLOYEES * scaleFactor);
    this.startingPositions = (int) Math.round(TemporalConstants.NUM_POSITIONS * scaleFactor);
    this.employees = new Employee[startingEmployees];
    this.positions = new Position[startingPositions];
  }

  public synchronized Employee getEmployee(int id) {
    return this.employees[id - 1];
  }

  public synchronized Position getPosition(int id) {
    return this.positions[id - 1];
  }

  public synchronized void insertEmployee(int employeeId, int salary, LocalDate hired) {
    this.employees[employeeId - 1] = new Employee(employeeId, salary, hired);
  }

  public synchronized void insertPosition(
      int positionId, int employeeId, String duty, LocalDate assigned) {
    this.positions[positionId - 1] = new Position(positionId, employeeId, duty, assigned);
  }
}
