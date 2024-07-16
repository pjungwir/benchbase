package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;
import java.util.List;

public class TestTemporalLoader extends AbstractTestLoader<TemporalBenchmark> {

  @Override
  public List<Class<? extends Procedure>> procedures() {
    return TestTemporalBenchmark.PROCEDURE_CLASSES;
  }

  @Override
  public Class<TemporalBenchmark> benchmarkClass() {
    return TemporalBenchmark.class;
  }
}
