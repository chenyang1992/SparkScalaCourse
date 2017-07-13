package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

//Count up total amount ordered by customer
object PurchaseByCustomer {
  
  def parseLine(line:String)={
      val fields = line.split(",")
      val customerID=fields(0).toInt
      val amount=fields(2).toFloat
      (customerID, amount)
  }
     
  def main(args: Array[String]) {
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
    
      // Create a SparkContext using the local machine
      val sc = new SparkContext("local", "WordCount")   
    
      // Load each line of my book into an RDD
      val input = sc.textFile("../SparkScalaCourse/resource/customer-orders.csv")
        
      val rdd=input.map(parseLine)
      
      val totalAmount = rdd.reduceByKey((x,y) => x+y)
      
      val flipped=totalAmount.map(x =>(x._2, x._1)).sortByKey()
      
      val results = flipped.collect()
      
      results.foreach(println)
      
  }  
}