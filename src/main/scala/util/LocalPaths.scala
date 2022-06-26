package util

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Random


/**
 * Random file directory utils
 */
class LocalPaths(basePaths: Array[String], name: String) {
  private val rand: Random = new Random

  for (tmpDir <- basePaths) {
    new File(tmpDir + "/" + name).mkdirs
  }

  private def getNextBasePath: String = basePaths(rand.nextInt(basePaths.length))

  def getTempPath(prefix: String): String = {
    var res: String = null
    do {
      val t: Int = rand.nextInt(Integer.MAX_VALUE)
      res = getNextBasePath + "/" + prefix + "-" + t
    } while (Files.exists(Paths.get(res)))
    res
  }
}
