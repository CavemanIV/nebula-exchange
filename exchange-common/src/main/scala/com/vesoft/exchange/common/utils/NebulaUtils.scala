/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.exchange.common.utils

import java.nio.charset.Charset
import java.nio.ByteBuffer
import java.nio.ByteOrder

import com.google.common.primitives.UnsignedLong
import com.vesoft.exchange.common.MetaProvider
import com.vesoft.exchange.common.VidType
import com.vesoft.exchange.common.config.{SchemaConfigEntry, Type}
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.log4j.Logger

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NebulaUtils {
  val DEFAULT_EMPTY_VALUE: String = "_NEBULA_EMPTY"

  private[this] val LOG = Logger.getLogger(this.getClass)

  def getDataSourceFieldType(sourceConfig: SchemaConfigEntry,
                             space: String,
                             metaProvider: MetaProvider): Map[String, Int] = {
    val nebulaFields = sourceConfig.nebulaFields
    val sourceFields = sourceConfig.fields
    val label        = sourceConfig.name

    var nebulaSchemaMap: Map[String, Integer] = null
    val dataType: Type.Value                  = metaProvider.getLabelType(space, label)
    if (dataType == null) {
      throw new IllegalArgumentException(s"label $label does not exist.")
    }
    if (dataType == Type.VERTEX) {
      nebulaSchemaMap = metaProvider.getTagSchema(space, label)
    } else {
      nebulaSchemaMap = metaProvider.getEdgeSchema(space, label)
    }

    val sourceSchemaMap: mutable.Map[String, Int] = mutable.HashMap[String, Int]()
    for (i <- nebulaFields.indices) {
      val nebulaField = nebulaFields.get(i)
      if (!nebulaSchemaMap.contains(nebulaField)) {
        throw new IllegalArgumentException(
          s"property name $nebulaField is not defined in NebulaGraph")
      }
      sourceSchemaMap.put(sourceFields.get(i), nebulaSchemaMap(nebulaField))
    }
    sourceSchemaMap.toMap
  }

  def isNumic(str: String): Boolean = {
    val newStr: String = if (str.startsWith("-")) {
      str.substring(1)
    } else { str }

    for (char <- newStr.toCharArray) {
      if (!Character.isDigit(char)) return false
    }
    true
  }

  def escapeUtil(str: String): String = {
    var s = str
    if (s.contains("\\")) {
      s = s.replaceAll("\\\\", "\\\\\\\\")
    }
    if (s.contains("\t")) {
      s = s.replaceAll("\t", "\\\\t")
    }
    if (s.contains("\n")) {
      s = s.replaceAll("\n", "\\\\n")
    }
    if (s.contains("\"")) {
      s = s.replaceAll("\"", "\\\\\"")
    }
    if (s.contains("\'")) {
      s = s.replaceAll("\'", "\\\\'")
    }
    if (s.contains("\r")) {
      s = s.replaceAll("\r", "\\\\r")
    }
    if (s.contains("\b")) {
      s = s.replaceAll("\b", "\\\\b")
    }
    s
  }

  def getPartitionId(id: String, partitionSize: Int, vidType: VidType.Value): Int = {
    val hashValue: Long = if (vidType == VidType.STRING) {
      // todo charset must be the same with Nebula Space
      val byteId = id.getBytes(Charset.forName("UTF-8"))
      if (byteId.length == 8) {
        //byte array to long, need to take care of endianess
        ByteBuffer.wrap(byteId).order(ByteOrder.nativeOrder).getLong
      } else {
        MurmurHash2.hash64(byteId, byteId.length, 0xc70f6907)
      }
    } else {
      id.toLong
    }
    val unsignedValue = UnsignedLong.fromLongBits(hashValue)
    val partSize      = UnsignedLong.fromLongBits(partitionSize)
    unsignedValue.mod(partSize).intValue + 1
  }

  def escapePropName(nebulaFields: List[String]): List[String] = {
    val propNames: ListBuffer[String] = new ListBuffer[String]
    for (key <- nebulaFields) {
      val sb = new StringBuilder()
      sb.append("`")
      sb.append(key)
      sb.append("`")
      propNames.append(sb.toString())
    }
    propNames.toList
  }
}
