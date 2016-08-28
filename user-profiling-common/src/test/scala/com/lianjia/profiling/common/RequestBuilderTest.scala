package com.lianjia.profiling.common

object RequestBuilderTest extends App {
  println(RequestBuilder.toPlainJava(1))
  println(RequestBuilder.toPlainJava("x"))
  val x = RequestBuilder.toPlainJava(Map(1 -> 1))
  println(x)
  val xx = 1.asInstanceOf[Integer]
  println(xx)
}
