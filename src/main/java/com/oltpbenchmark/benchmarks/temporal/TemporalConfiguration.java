package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.WorkloadConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;

public class TemporalConfiguration {

  private final XMLConfiguration xmlConfig;

  public TemporalConfiguration(WorkloadConfiguration workConf) {
    this.xmlConfig = workConf.getXmlConfig();
  }

  /*
   * These four parameters let you tune the start/end times
   * used for each operation that invokes a foreign key trigger.
   * The defaults should give roughly 99% valid operations
   * and 1% invalid. That seems like a typical distribution
   * if your foreign keys are just guardrails for developer error.
   * But if you do a lot of CASCADEs, perhaps you want something
   * less lopsided.
   */

  public double getMaxYearsUpdateEmployeeRange() {
    return xmlConfig.getDouble("maxYearsUpdateEmployeeRange", 0.03);
  }

  /*
   * For deleting employees, growing/shrinking the range
   * (which is always centered around today)
   * is not very helpful,
   * since the distribution of positions' start dates is centered on today also.
   * So instead we use a fixed range of +/- half of MAX_EMPLOYEE_TENURE
   * (which is 2 sigma based on Benchbase's Gaussian implementation),
   * and you can slide the mean right with positive numbers or left with negative.
   */
  public double getShiftYearsDeleteEmployee() {
    return xmlConfig.getDouble("shiftYearsDeleteEmployee", 15);
  }

  public double getMaxYearsInsertPositionRange() {
    return xmlConfig.getDouble("maxYearsInsertPositionRange", 0.03);
  }

  public double getMaxYearsUpdatePositionRange() {
    return xmlConfig.getDouble("maxYearsUpdatePositionRange", 16.00);
  }
}
