package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.temporal.procedures.*;
import java.util.List;

public class TestTemporalBenchmark extends AbstractTestBenchmarkModule<TemporalBenchmark> {

  public static final List<Class<? extends Procedure>> PROCEDURE_CLASSES =
      List.of(
          InsertPosition.class,
          UpdatePosition.class,
          UpdateEmployee.class,
          DeleteEmployee.class,
          SelectOneEmployee.class,
          SelectAllEmployees.class,
          SelectOneEmployeePositions.class,
          SelectEmployeesWithPosition.class,
          SelectEmployeesWithoutPosition.class,
          SelectOneEmployeeWithOptionalPositions.class);

  @Override
  public List<Class<? extends Procedure>> procedures() {
    return TestTemporalBenchmark.PROCEDURE_CLASSES;
  }

  @Override
  public Class<TemporalBenchmark> benchmarkClass() {
    return TemporalBenchmark.class;
  }
}
