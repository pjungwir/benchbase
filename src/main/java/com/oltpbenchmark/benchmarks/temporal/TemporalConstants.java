package com.oltpbenchmark.benchmarks.temporal;

public abstract class TemporalConstants {

  public static final String TABLENAME_EMPLOYEES = "employees";
  public static final String TABLENAME_POSITIONS = "positions";

  public static final String[] EMPLOYEE_NAMES = {"Joe", "Fred", "Sue", "Carol"};

  public static final String[] POSITION_NAMES = {"Janitor", "Dishwasher", "Peon", "Gopher"};

  /** Number of employees */
  public static final int NUM_EMPLOYEES = 1000;

  /** Number of positions */
  public static final int NUM_POSITIONS = 2000;

  /** Max number of years to pre-load employee tenure */
  public static final int MAX_EMPLOYEE_TENURE = 20;

  /** Max number of years til a promotion */
  public static final int MAX_PROMOTION_DURATION = 3;

  /** Minimum years in each position */
  public static final int MIN_POSITION_DURATION = 2;

  /** Maximum years in each position */
  public static final int MAX_POSITION_DURATION = 10;

  /** Probability [0,1] of not having a position */
  public static final double PROBABILITY_UNASSIGNED = 0.1;

  /** +/- years from today for CheckForeignKey* procedures */
  public static final boolean CHECK_FK_GAUSSIAN_RANGE = false;

  public static final double MAX_YEARS_CHECK_FK_RANGE = 0.03; /* 1% failure rate */
}
