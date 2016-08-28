package com.lianjia.profiling.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.codec.binary.Base64

/**
  * @author fenglei@lianjia.com on 2016-07
  */

object ZipUtil {
  def deflate(bytes: Array[Byte]): String = {
    val arrOutputStream = new ByteArrayOutputStream()
    val zipOutputStream = new GZIPOutputStream(arrOutputStream)
    zipOutputStream.write(bytes)
    zipOutputStream.close()
    Base64.encodeBase64String(arrOutputStream.toByteArray)
  }

  def inflate(base64: String): String = {
    val bytes = Base64.decodeBase64(base64)
    val zipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val buf: Array[Byte] = new Array[Byte](2048)
    var num = -1
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    while ( { num = zipInputStream.read(buf, 0, buf.length); num != -1 }) {
      byteArrayOutputStream.write(buf, 0, num)
    }
    val inflated = byteArrayOutputStream.toString
    zipInputStream.close()
    byteArrayOutputStream.close()
    inflated
  }
}
