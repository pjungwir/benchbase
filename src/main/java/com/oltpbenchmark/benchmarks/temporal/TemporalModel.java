package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.benchmarks.temporal.procedures.*;
import com.oltpbenchmark.util.RandomDistribution;
import java.time.LocalDate;
import java.util.Random;

public final class TemporalModel {

  public final double scaleFactor;
  public final LocalDate today;
  public final int startingEmployees;
  public final int startingPositions;
  private final Employee[] employees;
  private final Position[] positions;
  private int fkViolations = 0;

  public TemporalModel(double scaleFactor) {
    this.scaleFactor = scaleFactor;
    this.today = LocalDate.now();
    this.startingEmployees = (int) Math.round(TemporalConstants.NUM_EMPLOYEES * scaleFactor);
    this.startingPositions = (int) Math.round(TemporalConstants.NUM_POSITIONS * scaleFactor);
    this.employees = new Employee[startingEmployees];
    this.positions = new Position[startingPositions];
  }

  public int gaussianEmployeeId(Random rng) {
    // We have to make a new Gaussian each time,
    // because rng should be a thread-local variable:
    return new RandomDistribution.Gaussian(
            rng, 1, Math.round(TemporalConstants.NUM_EMPLOYEES * scaleFactor))
        .nextInt();
  }

  public int gaussianPositionId(Random rng) {
    // We have to make a new Gaussian each time,
    // because rng should be a thread-local variable:
    return new RandomDistribution.Gaussian(
            rng, 1, Math.round(TemporalConstants.NUM_POSITIONS * scaleFactor))
        .nextInt();
  }

  /*
   * Returns an int +/- halfRange from today,
   * with normal distribution.
   */
  public int gaussianDays(Random rng, int halfRange) {
    return new RandomDistribution.Gaussian(rng, -halfRange, halfRange).nextInt();
  }

  public int zipfianRank(Random rng, int max) {
    return new RandomDistribution.Zipf(rng, 1, max, 2).nextInt();
  }

  public int randomEmployeeId(Random rng) {
    return rng.nextInt(startingEmployees) + 1;
  }

  public int randomPositionId(Random rng) {
    return rng.nextInt(startingPositions) + 1;
  }

  public Employee chooseEmployee(Random rng) {
    return employees[rng.nextInt(startingEmployees)];
  }

  public Position choosePosition(Random rng) {
    return positions[rng.nextInt(startingPositions)];
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
      int positionId, int employeeId, String duty, LocalDate assigned, LocalDate relieved) {
    this.positions[positionId - 1] = new Position(positionId, employeeId, duty, assigned, relieved);
  }

  public synchronized int failFk() {
    fkViolations += 1;
    return fkViolations;
  }
}
