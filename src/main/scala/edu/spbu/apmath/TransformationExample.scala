package edu.spbu.apmath

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TransformationExample {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("transformations-example")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val rdd = sc.parallelize(Seq("Hello", "world", "from", "spark", "rdd"))

    val rdd2 = sc.parallelize(Seq("Hello", "this", "is", "RDD", "2"))

    println("First task: ")
    printRdd(first(rdd, rdd2))
    println("\nSecond task: ")
    printRdd(second(rdd, rdd2))
    println("\nThird task: ")
    printRdd(third(rdd))
    println("\nFourth task: ")
    printRdd(fourth(rdd))
    println("\nFifth task: ")
    printRdd(fifth(rdd))

    sc.cancelAllJobs()
    sc.stop()
  }

  /**
    * @return return RDD with elements from both rdd and rdd2
    */
  def first(rdd: RDD[String], rdd2: RDD[String]): RDD[String] = ???

  //
  /**
    * @return return RDD with common elements from rdd1 and rdd2
    */
  def second(rdd: RDD[String], rdd2: RDD[String]): RDD[String] = ???

  //
  /**
    * @return return RDD with all words in upper case
    */
  def third(rdd: RDD[String]): RDD[String] = ???

  /**
    * @return return RDD with characters from each string of rdd
    */
  def fourth(rdd: RDD[String]): RDD[Char] = ???

  /**
    * Return an RDD with strings longer than 4 characters
    */
  def fifth(rdd: RDD[String]): RDD[String] = ???

  def printRdd[T](rdd: RDD[T]): Unit = println(rdd.collect().mkString(" "))

}
