package service.PACC

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import org.apache.spark.rdd.RDD

/**
 * 并查集
 * 单机连通域算法
 */
object UnionFind {
  /**
   * 单机连通域算法
   * @param edges 边的集合
   * @return 边的RDD
   */
  def apply(edges: RDD[(Long,Long)]):RDD[(Long,Long)]={
    val it = edges.collect.iterator
    edges.sparkContext.parallelize(UnionFind(it).toStream)
  }
  /**
   * @param edges 边的集合
   * @return 边的集合 同一个连通域的点都指向最小的点
   */
  def apply(edges:Iterator[(Long,Long)]): Iterator[(Long,Long)]={
    val fa = new Long2LongOpenHashMap()
    fa.defaultReturnValue(-1)
    def findFather(x:Long):Long={
      val p = fa.get(x)
      if(p != -1){
        val father = findFather(p)
        fa.put(x,father)
        father
      }
      else x
    }
    def union(x:Long,y:Long)={
      val fx = findFather(x)
      val fy = findFather(y)
      if(fx<fy)fa.put(fy,fx)
      else if(fx>fy)fa.put(fx,fy)
    }
    edges.foreach{ case (u,v)=>
      val fu = findFather(u)
      val fv = findFather(v)
      if(fu != fv) union(fu,fv)
    }
    new Iterator[(Long,Long)] {
      private val it = fa.entrySet.iterator()
      override def hasNext: Boolean = it.hasNext
      override def next(): (Long, Long) = {
        val cur = it.next().getKey.toLong
        cur->findFather(cur)
      }
    }
  }
}
