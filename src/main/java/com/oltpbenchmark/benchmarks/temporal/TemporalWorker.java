package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.temporal.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemporalWorker extends Worker<TemporalBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(TemporalWorker.class);

  private final TemporalModel model;

  public TemporalWorker(TemporalBenchmark benchmarkModule, int id) {
    super(benchmarkModule, id);

    model = getBenchmark().model;

    // TODO: Use a fixed seed like TPCH?
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
      throws UserAbortException, SQLException {

    boolean expectFkViolation = false;

    try {
      if (nextTrans.getProcedureClass().equals(InsertPosition.class)) {
        Employee emp = model.chooseEmployee(rng());
        synchronized (emp) {
          if (emp.fired) expectFkViolation = true;

          String duty =
              TemporalConstants.POSITION_NAMES[
                  rng().nextInt(TemporalConstants.POSITION_NAMES.length)];
          int rank = 1;
          LocalDate s = emp.hired;
          LocalDate e = null;

          int positionId =
              getProcedure(InsertPosition.class).run(conn, emp.employeeId, duty, s, e, rank);
          // TODO: For now we'll only update the rows from the initial load,
          // to cut down on contention in our model.
          // But it'd be nice to update new things too.
          // getBenchmark().model.insertPosition(employeeId, positionId, duty, s, e, rank);
        }

      } else if (nextTrans.getProcedureClass().equals(UpdatePosition.class)) {
        Position p = model.choosePosition(rng());
        synchronized (p) {
          Employee emp;
          // 50% of the time, assign to another employee to test the FK:
          boolean reassign = rng().nextInt(100) > 50;
          p.rank += 1;

          emp = reassign ? model.chooseEmployee(rng()) : model.getEmployee(p.employeeId);
          synchronized (emp) {
            if (emp.fired) expectFkViolation = true;

            if (reassign) {
              p.employeeId = emp.employeeId;
              // Better start after they were hired:
              p.lastPromoted = emp.hired;
            }
            p.lastPromoted = p.lastPromoted.plusDays(1 + rng().nextInt(365 * 3));
            getProcedure(UpdatePosition.class)
                .run(conn, p.positionId, p.employeeId, p.duty, p.rank, p.lastPromoted);
          }
        }

      } else if (nextTrans.getProcedureClass().equals(UpdateEmployee.class)) {
        while (true) {
          Employee emp = model.chooseEmployee(rng());
          synchronized (emp) {
            if (emp.fired) continue;

            emp.raise(emp.lastRaised.plusDays(365 * (1 + rng().nextInt(3))));
            getProcedure(UpdateEmployee.class)
                .run(conn, emp.employeeId, emp.salary, emp.lastRaised);
          }
          break;
        }

      } else if (nextTrans.getProcedureClass().equals(DeleteEmployee.class)) {
        // The FK is ON DELETE CASCADE, so expectFkViolation is always false here.
        while (true) {
          Employee emp = model.chooseEmployee(rng());
          synchronized (emp) {
            if (emp.fired) continue;

            emp.fired = true;
            // Advance lastRaised so other operations will hit an FK violation:
            emp.lastRaised = emp.lastRaised.plusDays(1 + rng().nextInt(365 * 3));
            getProcedure(DeleteEmployee.class).run(conn, emp.employeeId, emp.lastRaised);
          }
          break;
        }
      }
    } catch (SQLException e) {
      // If it was a foreign key error, that's fine.
      // (We want to test that path too.)
      // 23503 is the standard code for a foreign key violation.
      // Just keep track in case it's a lot.
      // It's too bad we don't get a chance to log the final count once all the workers are done.
      if (expectFkViolation && e.getSQLState().equals("23503")) {
        int failures = model.failFk();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("fk failures: %d", failures));
        }
      } else {
        throw e;
      }
    }
    return (TransactionStatus.SUCCESS);
  }
}
