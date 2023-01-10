package io.sqlbenchmarks.sqlbench

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, FileSystems}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source

class Conf(args: Array[String]) extends ScallopConf(args) {
  val inputPath = opt[String](required = true)
  val queryPath = opt[String](required = true)
  val query = opt[Int](required = false)
  val numQueries = opt[Int](required = false)
  val keepAlive = opt[Boolean](required = false)
  verify()
}

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val w = new BufferedWriter(new FileWriter(new File(s"results-${System.currentTimeMillis()}.csv")))

    val spark: SparkSession = SparkSession.builder
      .appName("SQLBench Benchmarks")
      .getOrCreate()

    // register tables
    val start = System.currentTimeMillis()
    val dir = FileSystems.getDefault.getPath(conf.inputPath())
    Files.list(dir).iterator().asScala.foreach { path =>
      val filename = path.getFileName.toString
      if (filename.endsWith(".parquet")) {
        val table = filename.substring(0, filename.length - 8)
        println(s"Registering table $table at $path")
        val df = spark.read.parquet(path.toString)
        df.createTempView(table)
      }
    }
    val duration = System.currentTimeMillis() - start
    w.write(s"Register Tables,$duration\n")
    w.flush()

    if (conf.query.isSupplied) {
      execute(spark, conf.queryPath(), conf.query(), w)
    } else {
      for (query <- 1 to conf.numQueries()) {
          try {
            execute(spark, conf.queryPath(), query, w)
          } catch {
            case e: Exception =>
              // don't stop on errors
              println(s"Query $query FAILED:")
              e.printStackTrace()
          }
      }
    }

    w.close()

    if (conf.keepAlive()) {
      println("Sleeping to keep driver alive ... ")
      Thread.sleep(Long.MaxValue)
    }
  }

  private def execute(spark: SparkSession, path: String, query: Int, w: BufferedWriter) {
    val sqlFile = s"$path/q$query.sql"
    println(s"Executing query $query from $sqlFile")

    val source = Source.fromFile(sqlFile)
    val sql = source.getLines.mkString("\n")
    source.close()

    val queries = sql.split(';').filterNot(_.trim.isEmpty)
    var total_duration: Long = 0

    for ((sql, i) <- queries.zipWithIndex) {
      println(sql)

      val start = System.currentTimeMillis()
      val resultDf = spark.sql(sql.replace("create view", "create temp view"))
      val results = resultDf.collect()
      val duration = System.currentTimeMillis() - start
      total_duration += duration
      println(s"Query $query took $duration ms")

      var prefix = s"q$query"
      if (queries.length > 1) {
        prefix += "_part_"+ (i+1)
      }

      val optimizedLogicalPlan = resultDf.queryExecution.optimizedPlan
      writeFile(prefix, "logical_plan.txt", optimizedLogicalPlan.toString())
      writeFile(prefix, "logical_plan.qpml", Qpml.fromLogicalPlan(optimizedLogicalPlan))
      val physicalPlan = resultDf.queryExecution.executedPlan
      writeFile(prefix, "physical_plan.txt", physicalPlan.toString())

      val csvFilename = s"$prefix.csv"

      // write results to CSV format
      val resultWriter = new BufferedWriter(new FileWriter(csvFilename))
      resultWriter.write(physicalPlan.schema.fieldNames.mkString(",") + "\n")
      results.foreach(row => resultWriter.write(row.mkString(",") + "\n"))
      resultWriter.close()

      // could also save directly from dataframe but this would execute the query again
      //resultDf.coalesce(1).write.csv(csvFilename)
    }

    w.write(s"q$query,$total_duration\n")
    w.flush()
  }

  def writeFile(prefix: String, suffix: String, text: String): Unit = {
    val filename = prefix + "_"+ suffix
    println(s"Writing $filename")
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

}
