package io.substrait.hiveql

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveCalcitePlanner, ParseException, ParseUtils}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.{Context, QueryState}

private[hiveql] class HiveQLClientImpl(config: Map[String, String],
                                       initClassLoader: ClassLoader,
                                       val clientLoader: IsolatedClientLoader) extends HiveQLClient {

  val logger = org.slf4j.LoggerFactory.getLogger(classOf[HiveQLClientImpl])

  // Create an internal session state for this HiveClientImpl.
  val state: SessionState = {
    val original = Thread.currentThread().getContextClassLoader
    if (clientLoader.isolationOn) {
      // Switch to the initClassLoader.
      Thread.currentThread().setContextClassLoader(initClassLoader)
      try {
        newState()
      } finally {
        Thread.currentThread().setContextClassLoader(original)
      }
    } else {
      newState()
    }
  }

  private def newState(): SessionState = {
    val hiveConf = new HiveConf(classOf[SessionState]);
    hiveConf.setClassLoader(initClassLoader)
    config.foreach { case (k, v) => hiveConf.set(k, v) }
    // If this is true, SessionState.start will create a file to log hive job which will not be deleted on exit.
    if (hiveConf.getBoolean("hive.session.history.enabled", false)) {
      logger.warn("Detected HiveConf hive.session.history.enabled is true and will be reset to" + " false to disable useless hive logic")
      hiveConf.setBoolean("hive.session.history.enabled", false)
    }
    // If this is tez engine, SessionState.start might bring extra logic to initialize tez stuff.
    if (hiveConf.get("hive.execution.engine") eq "tez") {
      logger.warn("Detected HiveConf hive.execution.engine is 'tez' and will be reset to 'mr'" + " to disable useless hive logic")
      hiveConf.set("hive.execution.engine", "mr")
    }
    hiveConf.set("hive.query.reexecution.enabled", "false")
    //Avoid jdk11+ HiveMetaStoreClient URI casting exception when the value is default 'RANDOM'
    hiveConf.set("hive.metastore.uri.selection", "SEQUENTIAL")
    hiveConf.set("hive.query.results.cache.enabled", "false")

    val state = new SessionState(hiveConf)
    // Hive 2.3 will set UDFClassLoader to hiveConf when initializing SessionState
    // since HIVE-11878, and ADDJarsCommand will add jars to clientLoader.classLoader.
    // For this reason we cannot load the jars added by ADDJarsCommand because of class loader
    // got changed. We reset it to clientLoader.ClassLoader here.
    state.getConf.setClassLoader(initClassLoader)
    state
  }

  override def plan(command: String): RelNode = {
    val original = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(initClassLoader)
    try {
      SessionState.start(state)
      val conf = SessionState.getSessionConf

      val queryState = new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build
      val queryId = queryState.getQueryId
      logger.info("Compiling command(queryId=" + queryId + "): " + command)
      val queryTxnMgr = SessionState.get.initTxnMgr(conf)
      queryState.setTxnManager(queryTxnMgr)

      var ctx = new Context(conf)
      ctx.setHiveTxnManager(queryTxnMgr)
      ctx.setCmd(command)
      ctx.setHDFSCleanup(true)

      var tree: ASTNode = null
      var parseError = false
      try {
        tree = ParseUtils.parse(command, ctx)
        // clear CurrentFunctionsInUse set, to capture new set of functions
        // that SemanticAnalyzer finds are in use
        SessionState.get.getCurrentFunctionsInUse.clear()
        // Flush the metastore cache.  This assures that we don't pick up objects from a previous
        // query running in this same thread.  This has to be done after we get our semantic
        // analyzer (this is when the connection to the metastore is made) but before we analyze,
        // because at that point we need access to the objects.
        Hive.get(conf).getMSC.flushCache()
        // Do semantic analysis and  logic plan generation
        val sem = new HiveCalcitePlanner(queryState)
        sem.initCtx(ctx)
        sem.init(true)
        val op = sem.genOperator(tree)
        val plan = sem.genLogicalPlan(tree)
        logger.info(RelOptUtil.toString(plan))
        plan
      } catch {
        case e: ParseException =>
          parseError = true
          logger.error("parseError:", e)
          null
      } finally {
        if (ctx != null) {
          ctx.clear()
          ctx = null
        }
        Thread.currentThread().setContextClassLoader(original)
      }
    }
  }

  override def close(): Unit = {
    if (state != null) {
      state.close()
    }
  }
}
