package io.substrait.hiveql;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

public class HiveQLPlanDriverTest {

  @Test
  public void testInit() throws Throwable {
    String hiveLibDir = "/mnt/data/jvm/bigdata/substrait-java/hiveql/lib";
    File hiveJarsFile = new File(hiveLibDir);
    List<String> resourceUrls = Arrays.stream(hiveJarsFile.listFiles()).map(File::getAbsolutePath)
        .collect(Collectors.toList());
    resourceUrls.add(hiveLibDir);
//    resourceUrls.add("/mnt/data/jvm/bigdata/substrait-java/hiveql/build/classes/java/main");
    String scratchdir = new File("tmp").getAbsolutePath();
    FileUtils.deleteDirectory(new File(scratchdir));
    System.setProperty("java.io.tmpdir", scratchdir);
    HiveQLClient hiveQLClient = null;
    try {
      hiveQLClient = HiveQLUtils.newHiveQLClient(
          Map.of("hive.metastore.uris", "thrift://127.0.0.1:9083", "hive.exec.local.scratchdir",
              scratchdir), resourceUrls);
//      RelNode plan = hiveQLClient.plan(
//          "SELECT p_partkey, p_size\n"
//              + "FROM part p\n"
//              + "WHERE p_size <\n"
//              + "      (SELECT sum(l_orderkey)\n"
//              + "       FROM lineitem l\n"
//              + "       WHERE l.l_partkey = p.p_partkey)\n");
      RelNode plan = hiveQLClient.plan(
          "select lower(l_comment) from lineitem where length(l_comment)>0 limit 10");
      System.out.println(plan.toString());
    } finally {
      if (hiveQLClient != null) {
        hiveQLClient.close();
      }
    }
  }
}
