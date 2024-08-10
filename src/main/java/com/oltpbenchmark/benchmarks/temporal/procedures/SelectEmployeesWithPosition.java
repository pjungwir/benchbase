package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.temporal.DateRange;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * Finds employees at t who have at least one position.
 *
 * <p>This lets us benchmark semijoins.
 */
public class SelectEmployeesWithPosition extends Procedure {
  public final SQLStmt selectEmployeesWithPosition =
      new SQLStmt(
          ""
              + "SELECT  e.id, UNNEST(multirange(e.valid_at) * j.valid_at)::text AS valid_at, "
              + "        e.name, e.salary "
              + "FROM employees e "
              + "JOIN ("
              + "  SELECT  p.employee_id, range_agg(p.valid_at) AS valid_at "
              + "  FROM    positions p "
              + "  GROUP BY p.employee_id "
              + ") AS j "
              + "ON e.id = j.employee_id AND e.valid_at && j.valid_at "
              + "WHERE e.valid_at @> ?::date");

  public void run(Connection conn, LocalDate asof) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, selectEmployeesWithPosition)) {
      stmt.setDate(1, Date.valueOf(asof));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          int id = rs.getInt(1);
          DateRange validAt = DateRange.parse(rs.getString(2));
          assert validAt.from != null;
          assert asof.isEqual(validAt.from) || asof.isAfter(validAt.from);
          assert validAt.til == null || asof.isBefore(validAt.til);
        }
      }
    }
  }
}
