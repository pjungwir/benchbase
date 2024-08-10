package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class CheckForeignKeyLag extends Procedure {
  public final SQLStmt checkForeignKeyLag =
      new SQLStmt(
          "SELECT 1 FROM ( "
              + "  SELECT  uk.uk_start_value, uk.uk_end_value, "
              + "          NULLIF(LAG(uk.uk_end_value) OVER (ORDER BY uk.uk_start_value), "
              + "          uk.uk_start_value) AS x "
              + "  FROM   ( "
              + "    SELECT  COALESCE(LOWER(x.valid_at), '-Infinity') AS uk_start_value, "
              + "            COALESCE(UPPER(x.valid_at), 'Infinity') AS uk_end_value "
              + "    FROM    ONLY public.employees AS x "
              + "    WHERE id OPERATOR(pg_catalog.=) ? "
              + "    AND valid_at::pg_catalog.anyrange OPERATOR(pg_catalog.&&) daterange(?, ?) "
              + "    FOR KEY SHARE OF x "
              + "  ) AS uk "
              + ") AS uk "
              + "WHERE uk.uk_start_value < COALESCE(UPPER(daterange(?, ?)), 'Infinity') "
              + "AND   uk.uk_end_value >= COALESCE(LOWER(daterange(?, ?)), '-Infinity') "
              + "HAVING MIN(uk.uk_start_value) <= COALESCE(LOWER(daterange(?, ?)), '-Infinity') "
              + "AND    MAX(uk.uk_end_value) >= COALESCE(UPPER(daterange(?, ?)), 'Infinity') "
              + "AND    array_agg(uk.x) FILTER (WHERE uk.x IS NOT NULL) IS NULL");

  public int run(Connection conn, int employeeId, LocalDate validFrom, LocalDate validTil)
      throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, checkForeignKeyLag)) {
      stmt.setInt(1, employeeId);
      stmt.setDate(2, Date.valueOf(validFrom));
      stmt.setDate(3, Date.valueOf(validTil));
      stmt.setDate(4, Date.valueOf(validFrom));
      stmt.setDate(5, Date.valueOf(validTil));
      stmt.setDate(6, Date.valueOf(validFrom));
      stmt.setDate(7, Date.valueOf(validTil));
      stmt.setDate(8, Date.valueOf(validFrom));
      stmt.setDate(9, Date.valueOf(validTil));
      stmt.setDate(10, Date.valueOf(validFrom));
      stmt.setDate(11, Date.valueOf(validTil));
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) return rs.getInt(1);
        else return 0;
      }
    }
  }
}
