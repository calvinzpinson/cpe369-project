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
    
    
     // Part 1
    println("---- Part 1 ----")
    val courses = sc.textFile("Resources\\A6Courses.csv").map(line => (line.split(",")(0), line.split(",")(1)))
    val max  = courses.values.max
    courses.filter({case(x, y) => y == max}).join(sc.textFile("Resources\\A6Grades.csv").
        map(line => (line.split(",")(1), line.split(",")(0)))).map({case(x, y) => (y._2, x)}).
        join(sc.textFile("Resources\\A6Students.csv").
        map(line => (line.split(",")(0), line.split(",")(1))))foreach({case(x, y) => println(y._2)})
        
    
    println("---- Part 2 ----")
    sc.textFile("Resources\\A6Courses.csv").map(line => (line.split(",")(0), line.split(",")(1))).join(sc.textFile("Resources\\A6Grades.csv").
        map(line => (line.split(",")(1), line.split(",")(0)))).map({case(x, y) => (y._2, y._1.toInt)}).
        rightOuterJoin(sc.textFile("Resources\\A6Students.csv").
        map(line => (line.split(",")(0), line.split(",")(1)))).groupByKey().mapValues(x => (x.head._2, 
            x.map(x => x._1.getOrElse(0)).aggregate((0,0))((a, b) => (a._1 + b, a._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2)))).
        foreach({case(x, y) => println(y._1 + " " + "%.2f".format(y._2._1*1.0 / y._2._2))})
        
   // Part 3         
   println("---- Part 3 ----")
   sc.textFile("Resources\\A6Courses.csv").map(line => (line.split(",")(1).toInt, line.split(",")(0))).sortByKey().take(5).foreach({case(x, y) => println(y)})
    
   // Part 4
   println("---- Part 4 ----")
   sc.textFile("Resources\\A6Courses.csv").map(line => (line.split(",")(0), line.split(",")(1))).join(sc.textFile("Resources\\A6Grades.csv").
        map(line => (line.split(",")(1), line.split(",")(0)))).map({case(x, y) => (y._2, y._1.toInt)}).
        rightOuterJoin(sc.textFile("Resources\\A6Students.csv").
        map(line => (line.split(",")(0), line.split(",")(1)))).groupByKey().mapValues(x => (x.head._2, 
            x.map(x => x._1.getOrElse(0)).aggregate((0,0))((a, b) => (a._1 + b, a._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2)))).
        map({case(x, y) => (x , (y._1, y._2._1*1.0 / y._2._2))}).sortBy({case(x, y) => y._2}).foreach({case(x, y) => println(y._1)})
  }
}
