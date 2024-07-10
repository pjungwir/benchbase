package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class InsertPosition extends Procedure {
  public final SQLStmt insertPosition =
      new SQLStmt(
          "INSERT INTO positions (employee_id, valid_at, name) "
              + "VALUES (?, daterange(?, ?), concat(?, ' ', to_char(?, 'RN'))) RETURNING id");

  public int run(Connection conn, int employeeId, String duty, LocalDate s, LocalDate e, int rank)
      throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, insertPosition)) {
      stmt.setInt(1, employeeId);
      stmt.setDate(2, s == null ? null : Date.valueOf(s));
      stmt.setDate(3, e == null ? null : Date.valueOf(e));
      stmt.setString(4, duty);
      stmt.setInt(5, rank);
      stmt.execute();
      // TODO: return the id?
      return -1;
    }
  }
}
