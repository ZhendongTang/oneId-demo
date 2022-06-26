package util

import org.slf4j.LoggerFactory

import java.util
import java.util.Comparator
import scala.collection.mutable

class PairExternalSorter(basePaths: Array[String]) {

  private val logger = LoggerFactory.getLogger(getClass)


  val localPath = new LocalPaths(basePaths, "pair-ext-sort")

  val KEY_SHIFT = 20
  val BUFFER_SIZE: Int = 1 << KEY_SHIFT
  val INDEX_MASK: Long = BUFFER_SIZE - 1

  val packedKeysBuffer = new Array[(Long,Int)](BUFFER_SIZE) // use 64MB
  val valuesBuffer = new Array[Long](BUFFER_SIZE) // use 64MB
  var usedBufferSize = 0


  /**
   * Sort a given iterator. It consumes the given iterator and return a new one.
   *
   * @param it iterator to be sorted
   * @return sorted long iterator
   */
  def sort(it: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {
    mergeAll(localSort(it))
  }

  /**
   * Given a series of iterators, it merges all of them into one single sorted iterator.
   *
   * @param queue a series of iterators
   * @return merged and sorted iterator
   */
  private def mergeAll(queue: mutable.Queue[Iterator[(Long, Long)]]): Iterator[(Long, Long)] = {
    logger.info("Merging " + queue.size + " iterators.")
    SorterUtil.mergeAll(queue,mergeTwo)
  }

  /**
   * Given two sorted iterator, it returns a new merged and sorted iterator.
   *
   * @param in1 an iterator
   * @param in2 another iterator
   * @return merged and sorted iterator
   */
  private def mergeTwo(in1: Iterator[(Long, Long)], in2: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {

    val outPath = localPath.getTempPath("mergetwo")
    val out = new DirectWriter(outPath)
    var u: (Long, Long) = (0, 0)
    var v: (Long, Long) = (0, 0)
    u = in1.next
    v = in2.next

    def write(pair: (Long, Long)) {
      out.write(pair._1)
      out.write(pair._2)
    }

    var remain = true

    while (remain) {
      if (u._1 < v._1) {
        write(u)
        if (in1.hasNext) u = in1.next
        else {
          write(v)
          remain = false
        }
      }
      else {
        write(v)
        if (in2.hasNext) v = in2.next
        else {
          write(u)
          remain = false
        }
      }
    }

    in1.foreach(write)
    in2.foreach(write)


    out.close()

    getDirectReaderIterator(outPath)

  }

  /**
   * This operation reads values from a given iterator, writes to the buffer, and sorts it.
   * If the input exceeds the size of the buffer, this operation spill the sorted
   * buffer out and continues until it reads all long values.
   *
   * @param it an iterator
   * @return a series of sorted iterators which are parts of the input iterator
   */
  def localSort(it: Iterator[(Long, Long)]): mutable.Queue[Iterator[(Long, Long)]] = {

    val queue = new mutable.Queue[Iterator[(Long, Long)]]()

    while (it.hasNext) {
      val usedBufferSize = acquireNextBuffer(it)
      util.Arrays.sort(packedKeysBuffer, 0, usedBufferSize,new Comparator[(Long,Int)]{
        override def compare(o1: (Long, Int), o2: (Long, Int)): Int = {
         o1._1.compareTo(o2._1)
        }
      })

      if (it.hasNext) {
        val tPath = localPath.getTempPath("localsort")

        val out = new DirectWriter(tPath)

        for (i <- 0 until usedBufferSize) {
          val (key, idx) = packedKeysBuffer(i)
          out.write(key)
          out.write(valuesBuffer(idx))
        }
        out.close()

        queue.enqueue(getDirectReaderIterator(tPath))
      }
      else {
        queue.enqueue(getBufferIterator(usedBufferSize))
      }
    }

    queue
  }

  /**
   * Fill the buffer with next values.
   *
   * @param it an iterator
   * @return the number of values read
   */
  def acquireNextBuffer(it: Iterator[(Long, Long)]): Int = {
    var i = 0

    while (it.hasNext && i < BUFFER_SIZE) {
      val (k, v) = it.next()
      packedKeysBuffer(i)=(k,i)
      valuesBuffer(i) = v
      i += 1
    }

    i
  }

  /**
   * Iterator for the buffer.
   *
   * @param limit to read
   * @return an iterator from the buffer
   */
  def getBufferIterator(limit: Int): Iterator[(Long, Long)] = new Iterator[(Long, Long)] {
    assert(limit <= BUFFER_SIZE)
    private var i: Int = 0

    override def hasNext: Boolean = i < limit

    override def next(): (Long, Long) = {
      val (key, idx) = packedKeysBuffer(i)
      val next = (key, valuesBuffer(idx))
      i += 1
      next
    }
  }

  /**
   * Iterator for a sorted file.
   * !IMPORTANT this operation delete the input file after it reads all values.
   *
   * @param path of a sorted file
   * @return an iterator for the sorted file
   */
  def getDirectReaderIterator(path: String): Iterator[(Long, Long)] = {
    SorterUtil.getDirectReaderIterator(path, dr=>(dr.readLong(),dr.readLong()))
  }


}
