package util

import com.google.common.collect.HashBiMap
import scala.reflect.ClassTag

object CompressionUtils {

  /**
   * Unzip the data
   *
   * @param map         Data mapping
   * @param startOffset Start of storage
   * @param endOffset   End of storage
   * @param value       Compressed value
   * @return
   */
  def getValue[T: ClassTag]
  (map: HashBiMap[Int, T], startOffset: Int, endOffset: Int)
  (value: Long)
  (implicit t: ClassTag[T]): T = {
    map.get(BitUtil.getBitIntValue(startOffset, endOffset, value))
  }

  /**
   * Compressed data
   *
   * @param map         Data mapping
   * @param startOffset Start of storage
   * @param endOffset   End of storage
   * @param newValue    Compressed value
   * @param oldValue    The source value that needs to be set
   * @return
   */
  def setValue[T: ClassTag](map: HashBiMap[Int, T], startOffset: Int, endOffset: Int)
                           (newValue: T, oldValue: Long)
  (implicit t: ClassTag[T]): Long = {
    BitUtil.setBitValue(startOffset, endOffset, map.inverse().get(newValue), oldValue)
  }


}
