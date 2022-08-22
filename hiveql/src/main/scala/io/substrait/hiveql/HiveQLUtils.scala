package io.substrait.hiveql

import java.io.File
import scala.collection.JavaConverters._

object HiveQLUtils {

  val logger = org.slf4j.LoggerFactory.getLogger(HiveQLUtils.getClass)

  private def jdbcPrefixes = Seq(
    "com.mysql.jdbc", "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc")

  def newHiveQLClient(configurations: java.util.Map[String, String], resourceUrls: java.util.List[String]): HiveQLClient = {
    val isolatedLoader = {
      val jars =
        resourceUrls.asScala
          .map {
            case path => new File(path).toURI.toURL
          }

      logger.info(
        s"Initializing HiveQLClient using path: ${jars.mkString("\n")}")

      new IsolatedClientLoader(
        execJars = jars,
        config = configurations.asScala.toMap,
        isolationOn = true, //Above JDK9, we need use URLClassLoader to init Hive Objects,see https://issues.apache.org/jira/browse/HIVE-21584 .
        sharedPrefixes = jdbcPrefixes)
    }
    isolatedLoader.createClient()
  }
}
