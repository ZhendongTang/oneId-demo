package util

import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

object SorterUtil{

  def getDirectReaderIterator[T:ClassTag] (path: String,read:DirectReader => T): Iterator[T] = {

    new Iterator[T] {
      var dr: DirectReader = _
      var state: Short = 0 //0: not initialized, 1:dr is open, 2: dr is closed.
      @tailrec
      override def hasNext: Boolean = state match {
        case 0 =>
          dr = new DirectReader(path)
          state = 1
          hasNext
        case 1 =>
          val nxt = dr.hasNext
          if (!nxt) {
            dr.close()
            Files.delete(Paths.get(path))
            state = 2
          }
          nxt
        case 2 => false
      }

      override def next(): T= if (hasNext) read(dr) else Iterator.empty.next()
    }

  }

  def mergeAll[T:ClassTag](queue: mutable.Queue[Iterator[T]],mergeTwo: (Iterator[T],Iterator[T])=>Iterator[T] )
  :Iterator[T]={
    if(queue.isEmpty)Iterator.empty
    else {
      while (queue.size>1)queue.enqueue(mergeTwo(queue.dequeue(),queue.dequeue()))
      queue.dequeue()
    }
  }

}
