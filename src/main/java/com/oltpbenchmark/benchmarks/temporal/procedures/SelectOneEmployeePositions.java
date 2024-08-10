package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

/** Selects the positions for one employee using an inner join. */
public class SelectOneEmployeePositions extends Procedure {
  public final SQLStmt selectOneEmployeePositions =
      new SQLStmt(
          ""
              + "SELECT p.id, p.valid_at * e.valid_at AS valid_at, p.name, p.employee_id "
              + "FROM employees e "
              + "JOIN positions p ON e.id = p.employee_id AND e.valid_at && p.valid_at "
              + "WHERE e.id = ? AND e.valid_at @> ?::date");

  public void run(Connection conn, int employeeId, LocalDate asof) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, selectOneEmployeePositions)) {
      stmt.setInt(1, employeeId);
      stmt.setDate(2, Date.valueOf(asof));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          int id = rs.getInt(1);
          assert id > 0;
        }
      }
    }
  }
}
