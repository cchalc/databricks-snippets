// Databricks notebook source
// Generate List of tags to simulate input 
val tags = 0 to 0
val tagList:List[Int] =tags.toList
val tagListStr:List[String]  = tagList.map(_.toString)

// Convert list of tags to sequence for parallelization
val jobArguments = tagListStr.toSeq

// Set max concurrency for parallelism
val jobConcurrency = 25

// Notebook to run in parallel
val notebookToRun = "/Concurrency Examples/2. Notebook to Parallelize"

// Verify Spark Scheduler Mode is FAIR
spark.conf.get("spark.scheduler.mode")

// COMMAND ----------

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration


// look up required context for parallel run calls
val context = dbutils.notebook.getContext()

// create threadpool for parallel runs
implicit val executionContext = ExecutionContext.fromExecutorService(
  Executors.newFixedThreadPool(jobConcurrency))


try {
  val futures = jobArguments.zipWithIndex.map { case (args, i) =>
    Future({
      // ensure thread knows about databricks context
      dbutils.notebook.setContext(context)

      // define up to maxJobs separate scheduler pools
      sc.setLocalProperty("spark.scheduler.pool", "pool" + i.toString)

      // start the job in the scheduler pool
      dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, Map("tagName" -> args))
    })}

  // wait for all the jobs to finish processing
  Await.result(Future.sequence(futures), atMost = Duration.Inf)
} finally {
  // ensure to clean up the threadpool
  executionContext.shutdownNow()
}

// COMMAND ----------


