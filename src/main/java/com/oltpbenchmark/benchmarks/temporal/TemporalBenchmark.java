package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.temporal.procedures.InsertPosition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemporalBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(TemporalBenchmark.class);

  public final TemporalConfiguration config;
  public final TemporalModel model;

  public TemporalBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
    this.config = new TemporalConfiguration(workConf);
    this.model = new TemporalModel(workConf.getScaleFactor());
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return InsertPosition.class.getPackage();
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    int numTerminals = workConf.getTerminals();
    LOG.info(String.format("Creating %d workers for Temporal", numTerminals));
    for (int i = 0; i < numTerminals; i++) {
      workers.add(new TemporalWorker(this, i));
    }

    return workers;
  }

  @Override
  protected Loader<TemporalBenchmark> makeLoaderImpl() {
    return new TemporalLoader(this);
  }
}
