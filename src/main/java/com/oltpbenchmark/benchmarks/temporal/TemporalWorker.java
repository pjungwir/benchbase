package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.temporal.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;

public final class TemporalWorker extends Worker<TemporalBenchmark> {
  private final TemporalModel model;

  public TemporalWorker(TemporalBenchmark benchmarkModule, int id) {
    super(benchmarkModule, id);

    model = getBenchmark().model;

    // TODO: Use a fixed seed like TPCH?
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
      throws UserAbortException, SQLException {
    if (nextTrans.getProcedureClass().equals(InsertPosition.class)) {
      int employeeId = model.randomEmployeeId(rng());
      String duty =
          TemporalConstants.POSITION_NAMES[rng().nextInt(TemporalConstants.POSITION_NAMES.length)];
      int rank = 1;
      LocalDate s = model.getEmployee(employeeId).hired;
      LocalDate e = null;

      int positionId = getProcedure(InsertPosition.class).run(conn, employeeId, duty, s, e, rank);
      // TODO: For now we'll only update the rows from the initial load,
      // to cut down on contention in our model.
      // But it'd be nice to update new things too.
      // getBenchmark().model.insertPosition(employeeId, positionId, duty, s, e, rank);

    } else if (nextTrans.getProcedureClass().equals(UpdatePosition.class)) {
      Position p = model.choosePosition(rng());
      synchronized (p) {
        p.rank += 1;
        // 50% of the time, assign to another employee to test the FK:
        if (rng().nextInt(100) > 50) {
          p.employeeId = model.randomEmployeeId(rng());
          // Better start from today or we can choose an employee who wasn't hired yet:
          LocalDate today = LocalDate.now();
          if (p.lastPromoted.isBefore(today)) p.lastPromoted = today;
        }
        p.lastPromoted = p.lastPromoted.plusDays(1 + rng().nextInt(365 * 3));
        getProcedure(UpdatePosition.class)
            .run(conn, p.positionId, p.employeeId, p.duty, p.rank, p.lastPromoted);
      }

    } else if (nextTrans.getProcedureClass().equals(UpdateEmployee.class)) {
      Employee emp = model.chooseEmployee(rng());
      synchronized (emp) {
        emp.raise(emp.lastRaised.plusDays(365 * (1 + rng().nextInt(3))));
        getProcedure(UpdateEmployee.class).run(conn, emp.employeeId, emp.salary, emp.lastRaised);
      }
    }
    return (TransactionStatus.SUCCESS);
  }
}
