package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.temporal.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemporalWorker extends Worker<TemporalBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(TemporalWorker.class);

  private final TemporalModel model;
  private final TemporalConfiguration config;

  public TemporalWorker(TemporalBenchmark benchmarkModule, int id) {
    super(benchmarkModule, id);

    model = getBenchmark().model;
    config = getBenchmark().config;

    // TODO: Use a fixed seed like TPCH?
  }

  private class RandomEmployee {
    public final int id;
    public final LocalDate s;
    public final LocalDate e;
    public final int raise;

    private RandomEmployee(int id, LocalDate s, LocalDate e, int raise) {
      this.id = id;
      this.s = s;
      this.e = e;
      this.raise = raise;
    }
  }

  private RandomEmployee makeRandomEmployee(boolean gaussianEmployee, double maxYears) {
    int id = model.gaussianEmployeeId(rng());
    // Start the range +/- some years centered on today.
    // This should give us mostly "success" (i.e. the foreign key is valid),
    // but sometimes a failure.
    // We report failures as errors so we can see whether we have a realistic mix.
    // You can tune this with custom config elements:
    // <maxYears{Update,Delete}EmployeeRange>
    // and <maxYears{Insert,Update}PositionRange>.
    // I imagine the FK should valid 99% of the time.
    LocalDate s;
    LocalDate e;

    if (TemporalConstants.CHECK_FK_GAUSSIAN_RANGE) {
      s = model.today.plusDays(model.gaussianDays(rng(), (int) Math.round(maxYears * 365)));
      // Pick a range from 1 day to 2 years:
      e = s.plusDays(1 + rng().nextInt(365 * 2));
    } else {
      s = model.today.plusDays(-rng().nextInt(365 * TemporalConstants.MAX_EMPLOYEE_TENURE));
      e = s.plusDays(1 + rng().nextInt(365 * 5));
    }

    int raise = 1000 * (1 + rng().nextInt(20));

    return new RandomEmployee(id, s, e, raise);
  }

  private class RandomPosition {
    public final int id;
    public final LocalDate s;
    public final LocalDate e;
    public final Integer employeeId;
    public final String duty;
    public final int rank;

    private RandomPosition(
        int id, LocalDate s, LocalDate e, Integer employeeId, String duty, int rank) {
      this.id = id;
      this.s = s;
      this.e = e;
      this.employeeId = employeeId;
      this.duty = duty;
      this.rank = rank;
    }
  }

  private RandomPosition makeRandomPosition(double maxYears) {
    int id = model.gaussianPositionId(rng());
    // Start the range +/- some years centered on today.
    // This should give us mostly "success" (i.e. the foreign key is valid),
    // but sometimes a failure.
    // We report failures as errors so we can see whether we have a realistic mix.
    // You can tune this with custom config elements:
    // <maxYears{Update,Delete}EmployeeRange>
    // and <maxYears{Insert,Update}PositionRange>.
    // I imagine the FK should valid 99% of the time.
    LocalDate s;
    LocalDate e;
    Integer employeeId = null;
    String duty;
    int rank;

    if (TemporalConstants.CHECK_FK_GAUSSIAN_RANGE) {
      s = model.today.plusDays(model.gaussianDays(rng(), (int) Math.round(maxYears * 365)));
      // Pick a range from 1 day to 2 years:
      e = s.plusDays(1 + rng().nextInt(365 * 2));
    } else {
      s = model.today.plusDays(-rng().nextInt(365 * TemporalConstants.MAX_EMPLOYEE_TENURE));
      e = s.plusDays(1 + rng().nextInt(365 * 5));
    }

    // Occasionally reassign the position to another employee:
    boolean reassign = rng().nextInt(100) < 20; // 20%
    if (reassign) {
      employeeId = model.gaussianEmployeeId(rng());
    }

    duty = TemporalConstants.POSITION_NAMES[rng().nextInt(TemporalConstants.POSITION_NAMES.length)];

    rank = model.zipfianRank(rng(), 6);

    return new RandomPosition(id, s, e, employeeId, duty, rank);
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
      throws UserAbortException, SQLException {

    try {
      // These first three are for a synthetic benchmark
      // comparing foreign key implementations,
      // so we can run just the different trigger queries
      // and not include all the other stuff.
      // They foreign key should be invalid 1% of the time.

      if (nextTrans.getProcedureClass().equals(CheckForeignKeyRangeAgg.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        int ok = getProcedure(CheckForeignKeyRangeAgg.class).run(conn, emp.id, emp.s, emp.e);
        if (ok < 1) return TransactionStatus.ERROR;

      } else if (nextTrans.getProcedureClass().equals(CheckForeignKeyLag.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        int ok = getProcedure(CheckForeignKeyLag.class).run(conn, emp.id, emp.s, emp.e);
        if (ok < 1) return TransactionStatus.ERROR;

      } else if (nextTrans.getProcedureClass().equals(CheckForeignKeyExists.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        int ok = getProcedure(CheckForeignKeyExists.class).run(conn, emp.id, emp.s, emp.e);
        if (ok < 1) return TransactionStatus.ERROR;

        // These next three are the same, but we'll choose parameters
        // that should make the foreign key fail 90% of the time.

      } else if (nextTrans
          .getProcedureClass()
          .equals(CheckForeignKeyRangeAggProbablyInvalid.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        int ok =
            getProcedure(CheckForeignKeyRangeAggProbablyInvalid.class)
                .run(conn, emp.id, emp.s, emp.e);
        if (ok < 1) return TransactionStatus.ERROR;

      } else if (nextTrans.getProcedureClass().equals(CheckForeignKeyLagProbablyInvalid.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        int ok =
            getProcedure(CheckForeignKeyLagProbablyInvalid.class).run(conn, emp.id, emp.s, emp.e);
        if (ok < 1) return TransactionStatus.ERROR;

      } else if (nextTrans.getProcedureClass().equals(CheckForeignKeyExistsProbablyInvalid.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        int ok =
            getProcedure(CheckForeignKeyExistsProbablyInvalid.class)
                .run(conn, emp.id, emp.s, emp.e);
        if (ok < 1) return TransactionStatus.ERROR;

      } else if (nextTrans.getProcedureClass().equals(Noop.class)) {
        // Do nothing, so we can make a 33/33/33/1 workload instead of 33/33/34.
        getProcedure(Noop.class).run(conn);

        // These next four are for real queries
        // that excercise the foreign key constraint triggers:
        // referenced update/delete and referencing update/insert:

      } else if (nextTrans.getProcedureClass().equals(InsertPosition.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsInsertPositionRange());
        String duty =
            TemporalConstants.POSITION_NAMES[
                rng().nextInt(TemporalConstants.POSITION_NAMES.length)];
        int rank = 1;

        getProcedure(InsertPosition.class).run(conn, emp.id, duty, emp.s, emp.e, rank);

      } else if (nextTrans.getProcedureClass().equals(UpdatePosition.class)) {
        RandomPosition p = makeRandomPosition(config.getMaxYearsUpdatePositionRange());

        getProcedure(UpdatePosition.class).run(conn, p.id, p.employeeId, p.duty, p.rank, p.s);

      } else if (nextTrans.getProcedureClass().equals(UpdateEmployee.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE, config.getMaxYearsUpdateEmployeeRange());
        getProcedure(UpdateEmployee.class).run(conn, emp.id, emp.raise, emp.s);

      } else if (nextTrans.getProcedureClass().equals(DeleteEmployee.class)) {
        RandomEmployee emp =
            makeRandomEmployee(
                TemporalConstants.CHECK_FK_GAUSSIAN_RANGE,
                TemporalConstants.MAX_EMPLOYEE_TENURE / 2);
        getProcedure(DeleteEmployee.class)
            .run(conn, emp.id, emp.e.plusDays((long) (365 * config.getShiftYearsDeleteEmployee())));

        // The rest are for read-only queries,
        // mostly various join types:
        // inner join, left outer join, semijoin, antijoin:

      } else if (nextTrans.getProcedureClass().equals(SelectOneEmployee.class)) {
        Employee emp = model.chooseEmployee(rng());
        // 1 + to avoid 0:
        LocalDate asof =
            emp.hired.plusDays(rng().nextLong(1 + emp.hired.until(model.today, ChronoUnit.DAYS)));
        getProcedure(SelectOneEmployee.class).run(conn, emp.employeeId, asof);

      } else if (nextTrans.getProcedureClass().equals(SelectAllEmployees.class)) {
        LocalDate asof =
            model.today.minusDays(rng().nextLong(365 * TemporalConstants.MAX_EMPLOYEE_TENURE));
        getProcedure(SelectAllEmployees.class).run(conn, asof);

      } else if (nextTrans.getProcedureClass().equals(SelectOneEmployeePositions.class)) {
        Employee emp = model.chooseEmployee(rng());
        // 1 + to avoid 0:
        LocalDate asof =
            emp.hired.plusDays(rng().nextLong(1 + emp.hired.until(model.today, ChronoUnit.DAYS)));
        getProcedure(SelectOneEmployeePositions.class).run(conn, emp.employeeId, asof);

      } else if (nextTrans.getProcedureClass().equals(SelectEmployeesWithPosition.class)) {
        LocalDate asof =
            model.today.minusDays(rng().nextLong(365 * TemporalConstants.MAX_EMPLOYEE_TENURE));
        getProcedure(SelectEmployeesWithPosition.class).run(conn, asof);

      } else if (nextTrans.getProcedureClass().equals(SelectEmployeesWithoutPosition.class)) {
        LocalDate asof =
            model.today.minusDays(rng().nextLong(365 * TemporalConstants.MAX_EMPLOYEE_TENURE));
        getProcedure(SelectEmployeesWithoutPosition.class).run(conn, asof);

      } else if (nextTrans
          .getProcedureClass()
          .equals(SelectOneEmployeeWithOptionalPositions.class)) {
        Employee emp = model.chooseEmployee(rng());
        // 1 + to avoid 0:
        LocalDate asof =
            emp.hired.plusDays(rng().nextLong(1 + emp.hired.until(model.today, ChronoUnit.DAYS)));
        getProcedure(SelectOneEmployeeWithOptionalPositions.class).run(conn, emp.employeeId, asof);

      } else if (nextTrans
          .getProcedureClass()
          .equals(SelectAllEmployeesWithOptionalPositions.class)) {
        LocalDate asof =
            model.today.minusDays(rng().nextLong(365 * TemporalConstants.MAX_EMPLOYEE_TENURE));
        getProcedure(SelectAllEmployeesWithOptionalPositions.class).run(conn, asof);
      }

    } catch (SQLException e) {
      // If it was a foreign key error, that's fine.
      // (We want to test that path too.)
      // 23503 is the standard code for a foreign key violation.
      // Just keep track in case it's a lot.
      // It's too bad we don't get a chance to log the final count once all the workers are done.
      // But we can track it as an ERROR to see a histogram at the end.
      if (e.getSQLState().equals("23503")) {
        int failures = model.failFk();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("fk failures: %d", failures));
        }
        return TransactionStatus.ERROR;
      } else {
        throw e;
      }
    }
    return TransactionStatus.SUCCESS;
  }
}
