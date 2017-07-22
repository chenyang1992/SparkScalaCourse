package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkContext 

object SparkWordCount { 
   def main(args: Array[String]) { 

     // Set the log level to only print errors
     Logger.getLogger("org").setLevel(Level.ERROR)
     val sc = new SparkContext("local", "Word Count", "/usr/local/Cellar/apache-spark", Nil, Map(), Map()) 
		
      /* local = master URL; Word Count = application name; */  
      /* /usr/local/Cellar/apache-spark = Spark Home; Nil = jars; Map = environment */ 
      /* Map = variables to work nodes */ 
      /*creating an inputRDD to read text file (in.txt) through Spark context*/ 
      val input = sc.textFile("in.txt") 
      /* Transform the inputRDD into countRDD */ 
		
      val count = input.flatMap(line ⇒ line.split(" ")) 
      .map(word ⇒ (word, 1)) 
      .reduceByKey(_ + _) 
       
      /* saveAsTextFile method is an action that effects on the RDD */  
      count.saveAsTextFile("outfile") 
      System.out.println("OK"); 
   } 
}