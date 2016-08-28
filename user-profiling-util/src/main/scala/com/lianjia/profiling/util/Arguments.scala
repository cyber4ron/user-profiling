package com.lianjia.profiling.util

import org.kohsuke.args4j.CmdLineParser

/**
  * @author fenglei@lianjia.com on 2016-04
  */

trait Arguments {
  def parseArgs(args: Array[String]) = {
    val parser = new CmdLineParser(this)
    parser.parseArgument(args: _*)
    this
  }
}
