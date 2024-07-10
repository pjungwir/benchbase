package com.oltpbenchmark.benchmarks.temporal;

import com.oltpbenchmark.WorkloadConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;

public class TemporalConfiguration {

  private final XMLConfiguration xmlConfig;

  public TemporalConfiguration(WorkloadConfiguration workConf) {
    this.xmlConfig = workConf.getXmlConfig();
  }
}
