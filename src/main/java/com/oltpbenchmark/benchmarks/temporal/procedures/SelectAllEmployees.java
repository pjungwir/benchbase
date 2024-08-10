package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class SelectAllEmployees extends Procedure {
  public final SQLStmt selectAllEmployees =
      new SQLStmt("SELECT * FROM employees WHERE valid_at @> ?::date");

  public void run(Connection conn, LocalDate asof) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, selectAllEmployees)) {
      stmt.setDate(1, Date.valueOf(asof));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          int id = rs.getInt(1);
          assert id > 0;
        }
      }
    }
  }
}
