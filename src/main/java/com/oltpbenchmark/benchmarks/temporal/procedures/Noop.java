package com.oltpbenchmark.benchmarks.temporal.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
 * A dummy query so we can do 33/33/33/1 and not 33/33/34
 */
public class Noop extends Procedure {
  public final SQLStmt noops = new SQLStmt("SELECT 1");

  public void run(Connection conn) throws SQLException {
    try (PreparedStatement stmt = this.getPreparedStatement(conn, noops)) {
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          rs.getInt(1);
        }
      }
    }
  }
}
