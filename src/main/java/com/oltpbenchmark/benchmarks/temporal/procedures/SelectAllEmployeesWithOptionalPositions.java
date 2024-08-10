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

/** Gets all employees with their positions as an outer join. */
public class SelectAllEmployeesWithOptionalPositions extends Procedure {
  public final SQLStmt selectAllEmployeesWithOptionalPositions =
      new SQLStmt(
          ""
              + "SELECT  e.*, "
              + "        p.*, "
              + "        UNNEST(multirange(e.valid_at) * multirange(p.valid_at)) AS valid_at "
              + "FROM employees e "
              + "JOIN positions p "
              + "ON e.id = p.employee_id AND e.valid_at && p.valid_at "
              + "WHERE e.valid_at @> ?::date "
              + "UNION ALL "
              + "SELECT  e.*, (NULL::positions).*, "
              + "        UNNEST( "
              + "          CASE WHEN j.valid_at IS NULL "
              + "          THEN multirange(e.valid_at) "
              + "          ELSE multirange(e.valid_at) - j.valid_at END "
              + "        ) "
              + "FROM    employees e "
              + "LEFT JOIN ( "
              + "  SELECT p.employee_id, range_agg(p.valid_at) AS valid_at "
              + "  FROM   positions p "
              + "  GROUP BY p.employee_id "
              + ") AS j "
              + "ON      e.id = j.employee_id AND e.valid_at && j.valid_at "
              + "WHERE e.valid_at @> ?::date");

  public void run(Connection conn, LocalDate asof) throws SQLException {
    try (PreparedStatement stmt =
        this.getPreparedStatement(conn, selectAllEmployeesWithOptionalPositions)) {
      stmt.setDate(1, Date.valueOf(asof));
      stmt.setDate(2, Date.valueOf(asof));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          int id = rs.getInt(1);
          DateRange employeeValidAt = DateRange.parse(rs.getString(2));
          DateRange positionValidAt = DateRange.parse(rs.getString(9));
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
