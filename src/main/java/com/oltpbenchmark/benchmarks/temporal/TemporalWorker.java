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
      int employeeId = rng().nextInt(model.startingEmployees) + 1;
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

    }
    return (TransactionStatus.SUCCESS);
  }
}
