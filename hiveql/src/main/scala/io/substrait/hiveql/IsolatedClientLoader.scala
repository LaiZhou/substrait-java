package io.substrait.hiveql

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.hive.ql.parse.HiveCalcitePlanner

import java.lang.reflect.InvocationTargetException
import java.net.{URL, URLClassLoader}
import scala.util.Try

private[hiveql] class IsolatedClientLoader(val execJars: Seq[URL] = Seq.empty,
                                           val config: Map[String, String] = Map.empty,
                                           val isolationOn: Boolean = true,
                                           val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
                                           val sharedPrefixes: Seq[String] = Seq.empty,
                                           val barrierPrefixes: Seq[String] = Seq.empty) {

  private[hiveql] val logger = org.slf4j.LoggerFactory.getLogger(classOf[IsolatedClientLoader])

  /** All jars used by the hive specific classloader. */
  protected def allJars = execJars.toArray

  /** True if `name` refers to an (io.substrait.hiveql) class that must see specific version of Hive. */
  protected def isBarrierClass(name: String): Boolean =
    name.startsWith(classOf[HiveQLClientImpl].getName) ||
      name.startsWith(classOf[HiveCalcitePlanner].getName) ||
      barrierPrefixes.exists(name.startsWith)


  protected def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  protected def isSharedClass(name: String): Boolean = {
    val isHadoopClass =
      name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hive.")

    name.startsWith("org.slf4j") ||
      name.startsWith("org.apache.log4j") || // log4j1.x
      name.startsWith("org.apache.logging.log4j") || // log4j2
      name.startsWith("io.substrait") ||
      isHadoopClass ||
      name.startsWith("scala.") ||
      (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) ||
      name.startsWith("java.") ||
      name.startsWith("javax.sql.") ||
      sharedPrefixes.exists(name.startsWith)
  }

  /**
   * The classloader that is used to load an isolated version of Hive.
   * This classloader is a special URLClassLoader that exposes the addURL method.
   * So, when we add jar, we can add this new jar directly through the addURL method
   * instead of stacking a new URLClassLoader on top of it.
   */
  private[hiveql] val classLoader: MutableURLClassLoader = {
    val isolatedClassLoader =
      if (isolationOn) {
        if (allJars.isEmpty) {
          // See HiveUtils; this is the Java 9+ + builtin mode scenario
          baseClassLoader
        } else {
          val javaVersion = java.lang.Float.parseFloat(SystemUtils.JAVA_SPECIFICATION_VERSION);
          val rootClassLoader: ClassLoader =
            if (javaVersion > 1.8) {
              // In Java 9, the boot classloader can see few JDK classes. The intended parent
              // classloader for delegation is now the platform classloader.
              // See http://java9.wtf/class-loading/
              val platformCL =
              classOf[ClassLoader].getMethod("getPlatformClassLoader").
                invoke(null).asInstanceOf[ClassLoader]
              // Check to make sure that the root classloader does not know about Hive.
              assert(Try(platformCL.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)
              platformCL
            } else {
              // The boot classloader is represented by null (the instance itself isn't accessible)
              // and before Java 9 can see all JDK classes
              null
            }
          new URLClassLoader(allJars, rootClassLoader) {
            override def loadClass(name: String, resolve: Boolean): Class[_] = {
              val loaded = findLoadedClass(name)
              if (loaded == null) doLoadClass(name, resolve) else loaded
            }

            def doLoadClass(name: String, resolve: Boolean): Class[_] = {
              val classFileName = name.replaceAll("\\.", "/") + ".class"
              if (isBarrierClass(name)) {
                // For barrier classes, we construct a new copy of the class.
                logger.debug(s"start custom defining: $name - ...")
                try {
                  val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
                  logger.debug(s"success custom defining: $name - ...")
                  defineClass(name, bytes, 0, bytes.length)
                } catch {
                  case e: NullPointerException =>
                    throw e
                }
              } else if (!isSharedClass(name)) {
                logger.debug(s"hive class: $name - ${getResource(classToPath(name))}")
                super.loadClass(name, resolve)
              } else {
                // For shared classes, we delegate to baseClassLoader, but fall back in case the
                // class is not found.
                logger.debug(s"shared class: $name")
                try {
                  baseClassLoader.loadClass(name)
                } catch {
                  case _: ClassNotFoundException =>
                    super.loadClass(name, resolve)
                }
              }
            }
          }
        }
      } else {
        baseClassLoader
      }
    // Right now, we create a URLClassLoader that gives preference to isolatedClassLoader
    // over its own URLs when it loads classes and resources.
    // We may want to use ChildFirstURLClassLoader based on
    // the configuration of spark.executor.userClassPathFirst, which gives preference
    // to its own URLs over the parent class loader (see Executor's createClassLoader method).
    new NonClosableMutableURLClassLoader(isolatedClassLoader)
  }

  private[hiveql] def addJar(path: URL): Unit = synchronized {
    classLoader.addURL(path)
  }

  private[hiveql] def createClient(): HiveQLClient = synchronized {
    if (!isolationOn) {
      return new HiveQLClientImpl(config,
        baseClassLoader, this)
    }
    // Pre-reflective instantiation setup.
    logger.debug("Initializing the logger to avoid disaster...")
    val origLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)
    try {
      classLoader
        .loadClass(classOf[HiveQLClientImpl].getName)
        .getConstructors.head
        .newInstance(config, classLoader, this)
        .asInstanceOf[HiveQLClient]
    } catch {
      case e: InvocationTargetException =>
        e.getCause match {
          case cnf: NoClassDefFoundError =>
            //            throw QueryExecutionErrors.loadHiveClientCausesNoClassDefFoundError(
            //              cnf, execJars, HiveUtils.HIVE_METASTORE_JARS.key, e)
            throw e
          case _ =>
            throw e
        }
    } finally {
      Thread.currentThread.setContextClassLoader(origLoader)
    }
  }
}
