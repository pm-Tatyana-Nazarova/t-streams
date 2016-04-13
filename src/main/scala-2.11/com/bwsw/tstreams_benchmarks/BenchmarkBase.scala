package com.bwsw.tstreams_benchmarks

import java.io.File
import java.nio.file.{Files, Paths}

/**
  * This trait contains basic methods for benchmark tests
  */
trait BenchmarkBase {

  /**
    * Checks command line arguments (paths to config file and result directory)
    *
    * @param args contains path to config file and path to result directory
    */
  def checkParams(args: Array[String]): Unit = {
    // Check that params were passed
    if (args.size < 2) {
      System.err.println("Config file and result directory are required")
      System.exit(1)
    }

    // Check config file
    val configFilePath = Paths.get(args(0))
    if (!Files.exists(configFilePath) || Files.isDirectory(configFilePath)) {
      System.err.println("Config file doesn't exist")
      System.exit(1)
    }

    // Check result directory
    val resultDirectoryPath = Paths.get(args(1))
    if (Files.exists(resultDirectoryPath) && !Files.isDirectory(resultDirectoryPath)) {
      System.err.println("Name of result directory is already used by an existing file")
      System.exit(1)
    }
    if (!Files.exists(resultDirectoryPath)) {
      val resultDirectory = new File(args(1))
      if (!resultDirectory.mkdirs()) {
        System.err.println("Failed to create result directory")
        System.exit(1)
      }
    }
  }
}
