package edu.calpoly

import org.apache.spark.SparkContext._
import scala.io._
import java.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object project {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("lab7").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
  }
}
