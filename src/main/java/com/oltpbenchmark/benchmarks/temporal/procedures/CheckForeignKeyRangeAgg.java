package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class CheckForeignKeyRangeAgg extends Procedure {
  public final SQLStmt checkForeignKeyRangeAgg =
      new SQLStmt(
          "SELECT 1 FROM ( "
              + "  SELECT valid_at AS r "
              + "  FROM ONLY public.employees x WHERE id OPERATOR(pg_catalog.=) ? "
              + "  AND valid_at::pg_catalog.anyrange OPERATOR(pg_catalog.&&) daterange(?, ?)::pg_catalog.daterange "
              + "  FOR KEY SHARE OF x "
              + ") x1 "
              + "HAVING   daterange(?, ?)::pg_catalog.daterange OPERATOR(pg_catalog.<@) pg_catalog.range_agg(x1.r)");

  public int run(Connection conn, int employeeId, LocalDate validFrom, LocalDate validTil)
      throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, checkForeignKeyRangeAgg)) {
      stmt.setInt(1, employeeId);
      stmt.setDate(2, Date.valueOf(validFrom));
      stmt.setDate(3, Date.valueOf(validTil));
      stmt.setDate(4, Date.valueOf(validFrom));
      stmt.setDate(5, Date.valueOf(validTil));
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
        else return 0;
      }
    }
  }
}
