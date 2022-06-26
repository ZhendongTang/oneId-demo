package util

import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable

class LongExternalSorter(basePaths: Array[String]) {

  private val logger = LoggerFactory.getLogger(getClass)

  val localPath = new LocalPaths(basePaths, "long-ext-sort")

  val KEY_SHIFT = 23
  val BUFFER_SIZE: Int = 1 << KEY_SHIFT

  val buffer = new Array[Long](BUFFER_SIZE) // use 64MB
  var usedBufferSize = 0

  /**
   * Sort a given long iterator. It consumes the given iterator and return a new one.
   *
   * @param it long iterator to be sorted
   * @return sorted long iterator
   */
  def sort(it: Iterator[Long]): Iterator[Long] = {
    mergeAll(localSort(it))
  }

  /**
   * Given a series of long iterators, it merges all of them into one single sorted iterator.
   *
   * @param queue a series of long iterators
   * @return merged and sorted iterator
   */
  private def mergeAll(queue: mutable.Queue[Iterator[Long]]): Iterator[Long] = {
    SorterUtil.mergeAll(queue,mergeTwo)
  }

  /**
   * Given two sorted iterator, it returns a new merged and sorted iterator.
   *
   * @param in1 an iterator
   * @param in2 another iterator
   * @return merged and sorted iterator
   */
  private def mergeTwo(in1: Iterator[Long], in2: Iterator[Long]): Iterator[Long] = {

    val outPath = localPath.getTempPath("mergetwo")
    val out = new DirectWriter(outPath)
    var prev = Long.MinValue
    var u: Long = 0
    var v: Long = 0
    u = in1.next
    v = in2.next

    var remain = true

    def write(x: Long): Unit = {
      if (prev != x) {
        out.write(x)
        prev = x
      }
    }

    while (remain) {
      if (u < v) {
        write(u)
        if (in1.hasNext) u = in1.next
        else {
          write(v)
          remain = false
        }
      }
      else if (u > v) {
        write(v)
        if (in2.hasNext) v = in2.next
        else {
          write(u)
          remain = false
        }
      }
      else {
        if (in1.hasNext) u = in1.next
        else {
          write(v)
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
   * This operation reads long values from a given iterator, writes to the buffer, and sorts it.
   * If the input exceeds the size of the buffer, this operation spill the sorted
   * buffer out and continues until it reads all long values.
   *
   * @param it a long iterator
   * @return a series of sorted iterators which are parts of the input iterator
   */
  def localSort(it: Iterator[Long]): mutable.Queue[Iterator[Long]] = {

    val queue = new mutable.Queue[Iterator[Long]]()

    while (it.hasNext) {
      val usedBufferSize = acquireNextBuffer(it)
      util.Arrays.sort(buffer, 0, usedBufferSize)

      if (it.hasNext) {
        val tPath = localPath.getTempPath("localsort")

        val out = new DirectWriter(tPath)

        var prev = Long.MinValue
        for (i <- 0 until usedBufferSize) {
          val next = buffer(i)
          if (prev != next) {
            out.write(next)
            prev = next
          }
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
   * Fill the buffer with next long values.
   *
   * @param it long iterator
   * @return the number of values read
   */
  def acquireNextBuffer(it: Iterator[Long]): Int = {
    var i = 0

    while (it.hasNext && i < BUFFER_SIZE) {
      val v = it.next()
      buffer(i) = v
      i += 1
    }

    i
  }

  /**
   * Iterator for the buffer.
   *
   * @param limit to read
   * @return long iterator from the buffer
   */
  def getBufferIterator(limit: Int): Iterator[Long] = new Iterator[Long] {
    assert(limit <= BUFFER_SIZE)
    private var i: Int = 0
    private var prev: Long = Long.MinValue

    override def hasNext: Boolean = {
      while (i < limit && prev == buffer(i)) i += 1
      i < limit
    }

    override def next(): Long = {
      if (hasNext) {
        val next = buffer(i)
        prev = next
        i += 1
        next
      }
      else Iterator.empty.next()
    }
  }

  /**
   * Iterator for a sorted file.
   * !IMPORTANT this operation delete the input file after it reads all values.
   *
   * @param path of a sorted file
   * @return long iterator for the sorted file
   */
  def getDirectReaderIterator(path: String): Iterator[Long] = {
    SorterUtil.getDirectReaderIterator(path, dr => dr.readLong())
  }


}
