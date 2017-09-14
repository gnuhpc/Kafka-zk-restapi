package org.gnuhpc.bigdata

import scala.collection.JavaConversions

object CollectionConvertor {

  def seqConvertJavaList[A](seq: Seq[A]): java.util.List[A] = {
    return JavaConversions.seqAsJavaList(seq)
  }

  def mapConvertJavaMap[A, B](map: scala.collection.Map[A, B]): java.util.Map[A, B] = {
    return JavaConversions.mapAsJavaMap(map);
  }

  def listConvertJavaList[A](list: List[A]): java.util.List[A] = {
    return JavaConversions.bufferAsJavaList(list.toBuffer);
  }
}
