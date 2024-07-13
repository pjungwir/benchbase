package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class UpdatePosition extends Procedure {
  public final SQLStmt updatePosition =
      new SQLStmt(
          "UPDATE positions FOR PORTION OF valid_at FROM ? TO ? "
              + "SET name = concat(?, ' ', to_char(?, 'RN')), "
              + "    employee_id = ? "
              + "WHERE id = ?");

  public boolean run(
      Connection conn, int positionId, int employeeId, String duty, int rank, LocalDate promotedAt)
      throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, updatePosition)) {
      stmt.setDate(1, Date.valueOf(promotedAt));
      stmt.setDate(2, null);
      stmt.setString(3, duty);
      stmt.setInt(4, rank);
      stmt.setInt(5, employeeId);
      stmt.setInt(6, positionId);
      return stmt.execute();
    }
  }
}
