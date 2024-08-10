package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class CheckForeignKeyExists extends Procedure {
  public final SQLStmt checkForeignKeyExists =
      new SQLStmt(
          "SELECT 1 WHERE EXISTS ( "
              + "  SELECT  1 "
              + "  FROM ONLY public.employees x "
              + "  WHERE id OPERATOR(pg_catalog.=) ? "
              + "  AND valid_at::pg_catalog.anyrange OPERATOR(pg_catalog.&&) daterange(?, ?) "
              + "  AND COALESCE(LOWER(valid_at), '-Infinity') <= COALESCE(LOWER(daterange(?, ?)), '-Infinity') "
              + "  AND COALESCE(LOWER(daterange(?, ?)), '-Infinity') < COALESCE(UPPER(valid_at), ' Infinity') "
              + "  FOR KEY SHARE OF x"
              + ") "
              + "AND EXISTS ( "
              + "  SELECT  1 "
              + "  FROM ONLY public.employees x "
              + "  WHERE id OPERATOR(pg_catalog.=) ? "
              + "  AND valid_at::pg_catalog.anyrange OPERATOR(pg_catalog.&&) daterange(?, ?) "
              + "  AND COALESCE(LOWER(valid_at), '-Infinity') < COALESCE(UPPER(daterange(?, ?)), ' Infinity') "
              + "  AND COALESCE(UPPER(daterange(?, ?)), 'Infinity') <= COALESCE(UPPER(valid_at), ' Infinity') "
              + "  FOR KEY SHARE OF x"
              + ") "
              + "AND NOT EXISTS ( "
              + "  SELECT  1 "
              + "  FROM ONLY public.employees AS pk1 "
              + "  WHERE id OPERATOR(pg_catalog.=) ? "
              + "  AND valid_at::pg_catalog.anyrange OPERATOR(pg_catalog.&&) daterange(?, ?) "
              + "  AND COALESCE(LOWER(daterange(?, ?)), '-Infinity') < COALESCE(UPPER(valid_at), ' Infinity') "
              + "  AND COALESCE(UPPER(valid_at), 'Infinity') < COALESCE(UPPER(daterange(?, ?)), ' Infinity') "
              + "  AND NOT EXISTS ( "
              + "    SELECT  1 "
              + "    FROM ONLY public.employees AS pk2 "
              + "    WHERE pk1.id OPERATOR(pg_catalog.=) pk2.id "
              + "    AND COALESCE(LOWER(pk2.valid_at), '-Infinity') <= COALESCE(UPPER(pk1.valid_at), ' Infinity') "
              + "    AND COALESCE(UPPER(pk1.valid_at), 'Infinity') < COALESCE(UPPER(pk2.valid_at), ' Infinity') "
              + "    FOR KEY SHARE OF pk2"
              + "  ) "
              + "  FOR KEY SHARE OF pk1"
              + ")");

  public int run(Connection conn, int employeeId, LocalDate validFrom, LocalDate validTil)
      throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, checkForeignKeyExists)) {
      stmt.setInt(1, employeeId);
      stmt.setDate(2, Date.valueOf(validFrom));
      stmt.setDate(3, Date.valueOf(validTil));
      stmt.setDate(4, Date.valueOf(validFrom));
      stmt.setDate(5, Date.valueOf(validTil));
      stmt.setDate(6, Date.valueOf(validFrom));
      stmt.setDate(7, Date.valueOf(validTil));

      stmt.setInt(8, employeeId);
      stmt.setDate(9, Date.valueOf(validFrom));
      stmt.setDate(10, Date.valueOf(validTil));
      stmt.setDate(11, Date.valueOf(validFrom));
      stmt.setDate(12, Date.valueOf(validTil));
      stmt.setDate(13, Date.valueOf(validFrom));
      stmt.setDate(14, Date.valueOf(validTil));

      stmt.setInt(15, employeeId);
      stmt.setDate(16, Date.valueOf(validFrom));
      stmt.setDate(17, Date.valueOf(validTil));
      stmt.setDate(18, Date.valueOf(validFrom));
      stmt.setDate(19, Date.valueOf(validTil));
      stmt.setDate(20, Date.valueOf(validFrom));
      stmt.setDate(21, Date.valueOf(validTil));

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
        else return 0;
      }
    }
  }
}
