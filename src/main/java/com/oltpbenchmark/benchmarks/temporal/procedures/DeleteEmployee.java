package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class DeleteEmployee extends Procedure {
  public final SQLStmt deleteEmployee =
      new SQLStmt("DELETE FROM employees FOR PORTION OF valid_at FROM ? TO ? " + "WHERE id = ?");

  public boolean run(Connection conn, int employeeId, LocalDate firedAt) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, deleteEmployee)) {
      stmt.setDate(1, Date.valueOf(firedAt));
      stmt.setDate(2, null);
      stmt.setInt(3, employeeId);
      return stmt.execute();
    }
  }
}
