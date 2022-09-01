/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

object ErrorHandler {
  @transient
  private[this] val LOG = Logger.getLogger(this.getClass)

  // get filesystem from path allows support fs schema rather always using default one in hdfs-site.xml
  // FileSystem.get(new Configuration()) is a bad way to go
  def getFileSystem(path: String): (Path, FileSystem) = {
    val p = new Path(path)
    (p, p.getFileSystem(new Configuration()))
  }

  /**
    * clean all the failed data for error path before reload.
    *
    * @param path path to clean
    */
  def clear(path: String): Unit = {
    try {
      val (p, fileSystem) = getFileSystem(path)
      val filesStatus = fileSystem.listStatus(p)
      for (file <- filesStatus) {
        if (!file.getPath.getName.startsWith("reload.")) {
          fileSystem.delete(file.getPath, true)
        }
      }
    } catch {
      case e: Throwable => {
        LOG.error(s"$path cannot be clean, but this error does not affect the import result, " +
                    s"you can only focus on the reload files.",
                  e)
      }
    }
  }

  /**
    * save the failed execute statement.
    *
    * @param buffer buffer saved failed ngql
    * @param path path to write these buffer ngql
    */
  def save(buffer: ArrayBuffer[String], path: String): Unit = {
    LOG.info(s"create reload path $path")
    val (p, fileSystem) = getFileSystem(path)
    val errors     = fileSystem.create(p)

    try {
      for (error <- buffer) {
        errors.writeBytes(error)
        errors.writeBytes("\n")
      }
    } finally {
      errors.close()
    }
  }

  /**
    * check if path exists
    *
    * @param path error path
    *@return true if path exists
    */
  def existError(path: String): Boolean = {
    val (_, fileSystem) = getFileSystem(path)
    fileSystem.exists(new Path(path))
  }
}
