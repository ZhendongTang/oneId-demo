package util

import com.google.common.hash.{HashFunction, Hashing}

import java.nio.charset.Charset

/**
 * hash工具类
 */
object HashUtil {

  private val mur128Func: HashFunction = Hashing.murmur3_128()

  private val md5Func: HashFunction = Hashing.md5()

  /**
   * string to hash long
   * @param in 需要hash的值
   * @return 返回long
   */
  def hashToLong(in: String): Long = mur128Func.hashString(in, Charset.defaultCharset()).padToLong()

  /**
   * string to hash long (符合PACC计算要求)
   * @param in 需要hash的值
   * @return 返回符合PACC计算要求的long
   */
  def hashToPACCLong(in: String): Long = {

    /**
     * Binary 111111111111111111111111111111111111111111111111111110000000000
     */
    (Math.abs(hashToLong(in)) & 0x7FFFFFFFFFFFFC00L)>>10

  }

  /**
   * long md5
   * @param in 需要转换的值
   * @return md5
   */
  def long2Md5(in:Long):String = md5Func.hashLong(in).toString

  /**
   * string To md5
   * @param in 需要转换的值
   * @return md5
   */
  def str2Md5(in:String):String = md5Func.hashString(in,Charset.defaultCharset()).toString

}