package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemporalLoader extends Loader<TemporalBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(TemporalLoader.class);

  private final TemporalModel model;

  public TemporalLoader(TemporalBenchmark benchmark) {
    super(benchmark);

    model = this.benchmark.model;

    if (LOG.isDebugEnabled()) {
      LOG.debug("# of EMPLOYEES: {}", model.startingEmployees);
      LOG.debug("# of POSITIONS: {}", model.startingPositions);
    }
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    List<LoaderThread> threads = new ArrayList<>();

    final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
    // load EMPLOYEES
    final int employeesPerThread = Math.max(model.startingEmployees / numLoaders, 1);
    final int numEmployeeThreads =
        (int) Math.ceil((double) model.startingEmployees / employeesPerThread);
    // load POSITIONS
    final int positionsPerThread = Math.max(model.startingPositions / numLoaders, 1);
    final int numPositionThreads =
        (int) Math.ceil((double) model.startingPositions / positionsPerThread);

    final CountDownLatch employeeLatch = new CountDownLatch(numEmployeeThreads);
    final CountDownLatch positionLatch = new CountDownLatch(numPositionThreads);

    // EMPLOYEES
    for (int i = 0; i < numEmployeeThreads; i++) {
      final int lo = i * employeesPerThread + 1;
      final int hi = Math.min(model.startingEmployees, (i + 1) * employeesPerThread);

      threads.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              loadEmployees(conn, lo, hi);
            }

            @Override
            public void afterLoad() {
              employeeLatch.countDown();
            }
          });
    }

    // POSITIONS
    for (int i = 0; i < numPositionThreads; i++) {
      final int lo = i * positionsPerThread + 1;
      final int hi = Math.min(model.startingPositions, (i + 1) * positionsPerThread);

      threads.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              loadPositions(conn, lo, hi);
            }

            @Override
            public void beforeLoad() {
              try {
                employeeLatch.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public void afterLoad() {
              positionLatch.countDown();
            }
          });
    }

    // Set the GENERATED sequences past the ids we loaded:
    // TODO: To support non-Postgres we'll need to let the db generate ids above,
    // and slot the Employee objects into the right place.
    // Can we still use batches?
    threads.add(
        new LoaderThread(this.benchmark) {
          @Override
          public void load(Connection conn) throws SQLException {
            // (no sqli possible from integers)
            String sql =
                String.format(
                    "ALTER TABLE employees ALTER COLUMN id "
                        + "SET GENERATED ALWAYS RESTART WITH %d",
                    model.startingEmployees + 1);
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
              stmt.execute();
            }
            sql =
                String.format(
                    "ALTER TABLE positions ALTER COLUMN id "
                        + "SET GENERATED ALWAYS RESTART WITH %d",
                    model.startingPositions + 1);
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
              stmt.execute();
            }
          }

          @Override
          public void beforeLoad() {
            try {
              positionLatch.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });

    return threads;
  }

  /**
   * @throws SQLException
   */
  protected void loadEmployees(Connection conn, int lo, int hi) throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(TemporalConstants.TABLENAME_EMPLOYEES);

    String sql =
        "INSERT INTO employees (id, valid_at, name, salary) "
            + "VALUES (?, daterange(?, null), ?, ?)";

    int total = 0;

    try (PreparedStatement employeeInsert = conn.prepareStatement(sql)) {
      int batchSize = 0;

      for (int i = lo; i <= hi; i++) {
        String name =
            TemporalConstants.EMPLOYEE_NAMES[
                this.rng().nextInt(TemporalConstants.EMPLOYEE_NAMES.length)];
        int salary = 1000 * (20 + this.rng().nextInt(180));
        int tenure = this.rng().nextInt(TemporalConstants.MAX_EMPLOYEE_TENURE);
        LocalDate t = this.model.today.minusDays(365 * tenure);

        employeeInsert.setInt(1, i);
        employeeInsert.setDate(2, Date.valueOf(t));
        employeeInsert.setString(3, name);
        employeeInsert.setInt(4, salary);
        employeeInsert.addBatch();

        this.model.insertEmployee(i, salary, t);

        batchSize++;
        total++;
        if ((batchSize % workConf.getBatchSize()) == 0) {
          employeeInsert.executeBatch();

          employeeInsert.clearBatch();
          batchSize = 0;
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Employees %d / %d", total, this.model.startingEmployees));
          }
        }
      }
      if (batchSize > 0) {
        employeeInsert.executeBatch();
        employeeInsert.clearBatch();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Employees Loaded [%d]", total));
    }
  }

  /**
   * @throws SQLException
   */
  protected void loadPositions(Connection conn, int lo, int hi) throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(TemporalConstants.TABLENAME_POSITIONS);

    String sql =
        "INSERT INTO positions (id, valid_at, employee_id, name) "
            + "VALUES (?, daterange(?, ?), ?, ?)";

    int total = 0;

    try (PreparedStatement positionInsert = conn.prepareStatement(sql)) {
      int batchSize = 0;

      for (int i = lo; i <= hi; i++) {
        String duty =
            TemporalConstants.POSITION_NAMES[
                this.rng().nextInt(TemporalConstants.POSITION_NAMES.length)];
        int employeeId = this.model.gaussianEmployeeId(rng());
        Employee emp = this.model.getEmployee(employeeId);
        // Start within 5 years of their hire date:
        LocalDate startDate = emp.hired.plusDays(rng().nextInt(365 * 5));
        // Hold the position for x years:
        int years =
            rng()
                    .nextInt(
                        TemporalConstants.MAX_POSITION_DURATION
                            - TemporalConstants.MIN_POSITION_DURATION)
                + TemporalConstants.MIN_POSITION_DURATION;
        LocalDate endDate = startDate.plusDays(365 * years);

        positionInsert.setInt(1, i);
        positionInsert.setDate(2, Date.valueOf(startDate));
        positionInsert.setDate(3, Date.valueOf(endDate));
        positionInsert.setInt(4, employeeId);
        positionInsert.setString(5, duty);
        positionInsert.addBatch();

        this.model.insertPosition(i, employeeId, duty, startDate, endDate);

        batchSize++;
        total++;

        if ((batchSize % workConf.getBatchSize()) == 0) {
          positionInsert.executeBatch();

          positionInsert.clearBatch();
          batchSize = 0;
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Positions %d / %d", total, this.model.startingPositions));
          }
        }
      }
      if (batchSize > 0) {
        positionInsert.executeBatch();
        positionInsert.clearBatch();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Positions Loaded [%d]", total));
    }
  }
}
