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
    
    println("\nHate crime biases organized by hate crime victims by bias category, organized descending by number of victims:")
    sc.textFile("Resources\\biasMotivationGroups.csv").map(line => (line.split(",")(0), line.split(",")(1))).
    join(sc.textFile("Resources\\biasMotivations.csv").map(line => (line.split(",")(2), (line.split(",")(0), line.split(",")(1))))).
    map({case(x, y) => (y._2._1, (y._1, y._2._2))}).//foreach(println(_))
    join(sc.textFile("Resources\\offenses.csv").map(line => (line.split(",")(5), line.split(",")(4).toInt))).
    reduceByKey({case(x, y) => ((x._1._1, x._1._2),  x._2 + y._2)}).
    map({case(x, y) => (y._1._1, (y._1._2, y._2))}).groupByKey().mapValues(_.toList.sortBy((-1)*_._2)).collect().
    foreach({case(x, y) => (println(x.replaceAll("\"", "") + ": " + y.map({case(x, y) => x.replaceAll("\"", "")}).mkString(", ")))})
    
    println("\nNumber of hate crime victims by bias category, organized descending:")
    sc.textFile("Resources\\biasMotivationGroups.csv").map(line => (line.split(",")(0), line.split(",")(1))).
    join(sc.textFile("Resources\\biasMotivations.csv").map(line => (line.split(",")(2), line.split(",")(0)))).
    map({case(x, y) => (y._2, y._1)}).
    join(sc.textFile("Resources\\offenses.csv").map(line => (line.split(",")(5), line.split(",")(4).toInt))).
    map({case(x, y) => (y._1.replaceAll("\"", ""), y._2)}).
    reduceByKey((x, y) => x + y).sortBy({case(x, y) => (-1)*y}).collect().foreach({case(x, y) => println(x + ", " + y)})
    
    println("\nOf racially biased hate crimes, number of victims by racial bias, organized descending:")
    sc.textFile("Resources\\biasMotivationGroups.csv").map(line => (line.split(",")(0), line.split(",")(1))).
    filter({case(x, y) => y == "\"Anti-Racial\""}).
    join(sc.textFile("Resources\\biasMotivations.csv").map(line => (line.split(",")(2), (line.split(",")(0), line.split(",")(1))))).//foreach(println(_))
    map({case(x, y) => (y._2._1, y._2._2)}).
    join(sc.textFile("Resources\\offenses.csv").map(line => (line.split(",")(5), line.split(",")(4).toInt))).
    map({case(x, y) => (y._1.replaceAll("\"", ""), y._2)}).
    reduceByKey((x, y) => x + y).sortBy({case(x, y) => (-1)*y}).collect().foreach({case(x, y) => println(x + ", " + y)})
    
    println("\nOf sexual orientation biased hate crimes, number of victims by sexual orientation, organized descending:")
    sc.textFile("Resources\\biasMotivationGroups.csv").map(line => (line.split(",")(0), line.split(",")(1))).
    filter({case(x, y) => y == "\"Anti-Sexual\""}).
    join(sc.textFile("Resources\\biasMotivations.csv").map(line => (line.split(",")(2), (line.split(",")(0), line.split(",")(1))))).//foreach(println(_))
    map({case(x, y) => (y._2._1, y._2._2)}).
    join(sc.textFile("Resources\\offenses.csv").map(line => (line.split(",")(5), line.split(",")(4).toInt))).
    map({case(x, y) => (y._1.replaceAll("\"", ""), y._2)}).
    reduceByKey((x, y) => x + y).sortBy({case(x, y) => (-1)*y}).collect().foreach({case(x, y) => println(x + ", " + y)})
    
    println("\nOf religion biased hate crimes, number of victims by religion, organized descending:")
    sc.textFile("Resources\\biasMotivationGroups.csv").map(line => (line.split(",")(0), line.split(",")(1))).
    filter({case(x, y) => y == "\"Anti-Religous\""}).
    join(sc.textFile("Resources\\biasMotivations.csv").map(line => (line.split(",")(2), (line.split(",")(0), line.split(",")(1))))).//foreach(println(_))
    map({case(x, y) => (y._2._1, y._2._2)}).
    join(sc.textFile("Resources\\offenses.csv").map(line => (line.split(",")(5), line.split(",")(4).toInt))).
    map({case(x, y) => (y._1.replaceAll("\"", ""), y._2)}).
    reduceByKey((x, y) => x + y).sortBy({case(x, y) => (-1)*y}).collect().foreach({case(x, y) => println(x + ", " + y)})
  }
}
