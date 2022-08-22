package io.substrait.hiveql;

import org.apache.calcite.rel.RelNode;

public interface HiveQLClient {

  RelNode plan(String sql);

  void close();
}
