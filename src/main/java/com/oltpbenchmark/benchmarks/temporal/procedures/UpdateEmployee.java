package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class UpdateEmployee extends Procedure {
  public final SQLStmt sql =
      new SQLStmt(
          "UPDATE employees FOR PORTION OF valid_at FROM ? TO ? "
              + "SET salary = ? "
              + "WHERE id = ?");

  public boolean run(Connection conn, int employeeId, int salary, LocalDate raisedAt)
      throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, sql)) {
      stmt.setDate(1, Date.valueOf(raisedAt));
      stmt.setDate(2, null);
      stmt.setInt(3, salary);
      stmt.setInt(4, employeeId);
      return stmt.execute();
    }
  }
}
