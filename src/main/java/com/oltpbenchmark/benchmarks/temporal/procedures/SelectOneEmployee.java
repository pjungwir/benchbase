package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class SelectOneEmployee extends Procedure {
  public final SQLStmt selectOneEmployee =
      new SQLStmt("SELECT * FROM employees WHERE id = ? AND valid_at @> ?::date");

  public void run(Connection conn, int employeeId, LocalDate asof) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, selectOneEmployee)) {
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
