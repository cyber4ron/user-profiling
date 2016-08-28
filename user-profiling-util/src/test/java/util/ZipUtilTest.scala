package util

import com.lianjia.profiling.util.ZipUtil

/**
  * @author fenglei@lianjia.com on 2016-07
  */

object ZipUtilTest extends App {
  val x = ZipUtil.inflate("H4sIAAAAAAAAAA3HOw6AIAwA0Lt0pklbykduIxQTVwEX4931be+BOaCwBgnKPnkictDvCQXWuMDBWqf9oWbdIhlarDtql4y1kqLo1iQQczoyvB/bKG6jTgAAAA==")
  println(x)
}
