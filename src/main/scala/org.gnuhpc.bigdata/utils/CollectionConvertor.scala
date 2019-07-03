package org.gnuhpc.bigdata.utils

import scala.collection.JavaConverters

object CollectionConvertor {

  def seqConvertJavaList[A](seq: Seq[A]): java.util.List[A] = {
    return JavaConverters.seqAsJavaListConverter(seq).asJava
  }

  def mapConvertJavaMap[A, B](map: scala.collection.Map[A, B]): java.util.Map[A, B] = {
    return JavaConverters.mapAsJavaMapConverter(map).asJava
  }

  def listConvertJavaList[A](list: List[A]): java.util.List[A] = {
    return JavaConverters.bufferAsJavaListConverter(list.toBuffer).asJava;
  }

  def optionListConvertJavaList[A](list: List[A]): java.util.List[A] = {
    return JavaConverters.bufferAsJavaListConverter(list.toBuffer).asJava;
  }
}
