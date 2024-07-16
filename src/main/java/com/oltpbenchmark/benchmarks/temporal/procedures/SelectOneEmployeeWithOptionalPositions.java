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

/** Finds an employee with their positions as an outer join. */
public class SelectOneEmployeeWithOptionalPositions extends Procedure {
  public final SQLStmt sql =
      new SQLStmt(
          ""
              + "SELECT  e.id, e.valid_at, e.name, e.salary, "
              + "        j2.id AS position_id, j2.name AS position_name, j2.employee_id, "
              + "        COALESCE(j2.valid_at, e.valid_at) AS position_valid_at "
              + "FROM employees e "
              + "LEFT JOIN ( "
              + "  SELECT  p.employee_id, range_agg(p.valid_at) AS valid_at, array_agg(p) AS ps "
              + "  FROM    positions p "
              + "  GROUP BY p.employee_id "
              + ") AS j "
              + "ON e.id = j.employee_id AND e.valid_at && j.valid_at "
              + "LEFT JOIN LATERAL ( "
              + "  SELECT  u.employee_id, u.id, u.name, e.valid_at * u.valid_at AS valid_at "
              + "  FROM    UNNEST(j.ps) AS u "
              + "  WHERE NOT isempty(e.valid_at * u.valid_at) "
              + "  UNION ALL "
              + "  SELECT  NULL, NULL, NULL, u.valid_at "
              + "  FROM    UNNEST(multirange(e.valid_at) - j.valid_at) AS u(valid_at) "
              + "  WHERE NOT isempty(u.valid_at) "
              + ") AS j2 "
              + "ON j.employee_id IS NOT NULL "
              + "WHERE e.id = ? AND e.valid_at @> ?::date");

  public void run(Connection conn, int employeeId, LocalDate asof) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, sql)) {
      stmt.setInt(1, employeeId);
      stmt.setDate(2, Date.valueOf(asof));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          int id = rs.getInt(1);
          DateRange employeeValidAt = DateRange.parse(rs.getString(2));
          DateRange positionValidAt = DateRange.parse(rs.getString(8));
          assert employeeValidAt.from != null;
          assert asof.isEqual(employeeValidAt.from) || asof.isAfter(employeeValidAt.from);
          // Can't assert that p.valid_at @> asof,
          // because the record gets sliced up.
          // But we can say that p.valid_at <@ e.valid_at.
          assert employeeValidAt.contains(positionValidAt);
        }
      }
    }
  }
}
