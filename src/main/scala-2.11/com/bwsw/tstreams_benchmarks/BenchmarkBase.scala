package com.bwsw.tstreams_benchmarks

import java.io.File
import java.nio.file.{Files, Paths}

/**
  * This trait contains basic methods for benchmark tests
  */
trait BenchmarkBase {
  val totalStatisticsFileName = "Total_statistics.json"
  val transactionsTimingFileName = "Transactions_timing.json"

  /**
    * Checks command line arguments (paths to config file and result directory)
    *
    * @param args contains path to config file and path to result directory
    */
  def checkParams(args: Array[String]): Unit = {
    // Check that params were passed
    if (args.length < 2) {
      throw new IllegalArgumentException("Config file and result directory are required")
    }

    // Check config file
    val configFilePath = Paths.get(args(0))
    if (!Files.exists(configFilePath) || Files.isDirectory(configFilePath)) {
      throw new IllegalArgumentException("Config file doesn't exist")
    }

    // Check result directory
    val resultDirectoryPath = Paths.get(args(1))
    if (Files.exists(resultDirectoryPath) && !Files.isDirectory(resultDirectoryPath)) {
      throw new IllegalArgumentException("Name of result directory is already used by an existing file")
    }
    if (!Files.exists(resultDirectoryPath)) {
      val resultDirectory = new File(args(1) + "/")
      if (!resultDirectory.mkdirs()) {
        throw new RuntimeException("Failed to create result directory")
      }
    }
  }

  /**
    * Calculates statistics and saves it to result files*
    *
    * @param resultDirectoryPath - path to the directory where to save statistics
    * @param result - result string
    * @param totalStatistics - true for total statistics, false for raw timing data
    */
  def saveStatistics(resultDirectoryPath: String, result: String, totalStatistics: Boolean = false): Unit = {
    val fileName = resultDirectoryPath + "/" + (if (totalStatistics) totalStatisticsFileName else transactionsTimingFileName)
    val file = new File(fileName)
    val pw = new java.io.PrintWriter(file)
    pw.write(result)
    pw.close()
  }
}
