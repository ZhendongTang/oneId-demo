package util

import org.apache.spark.Partitioner


class PACCPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  import PartImplicitWrapper._
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.asInstanceOf[Long].mod(numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case p: PACCPartitioner =>
      p.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
