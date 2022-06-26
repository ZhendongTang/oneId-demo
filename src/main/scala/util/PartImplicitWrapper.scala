package util

object PartImplicitWrapper {

  /**
   * Binary 1111111111
   */
  val COPYID_MASK = 0x3FFL

  /**
   * Binary 111111111111111111111111111111111111111111111111111110000000000
   */
  val NODEID_MASK = 0x7FFFFFFFFFFFFC00L

  implicit class CopyOps(n: Long){

    def nodeId: Long = (n & NODEID_MASK) >> 10

    def copyId: Long = n & COPYID_MASK

    def encode(p: Int): Long = ( p & COPYID_MASK ) | ((n << 10) & NODEID_MASK)

    def tuple: (Long, Long) = (n.nodeId, n.copyId)

    def mod(p: Int): Int ={
      val rawMod = n.copyId.hashCode() % p
      rawMod + (if (rawMod < 0) p else 0)
    }
  }
}
