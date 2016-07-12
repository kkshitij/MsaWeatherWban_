package org.maybatch.sparkproject

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd._
import scala.collection.JavaConversions._
import org.apache.commons.math3.stat.descriptive.summary.Sum
import org.apache.hadoop.fs.shell.Count

object percipcompute {

  def main(args: Array[String]) {
//val sparkConf = new SparkConf().setAppName("kshitij")
val sparkConf = new SparkConf().setMaster("local").setAppName("kshitij")
val sparkCont = new SparkContext(sparkConf)
val percep = sparkCont.textFile(args(0))
val station = sparkCont.textFile(args(1))
val population = sparkCont.textFile(args(2))
val temp1 = percep.map(x => x.split(","))
				   .filter(d => d(0) != "Wban")
				   .filter(T => T(0).trim() != "" && T(0).trim() != "T" && T(3).trim() != "" && T(3).trim() != "T" && T(2).toInt > 7)
				   .map(x => (x(0) -> (x(3).toFloat)))
val temp2 = station.map(x => x.split(",",-1))
				   .filter(T => T(0).trim() != "" )
				   .filter(d => d(0) != "WBAN")
				   .map(c => (c(15) -> c(0)))
	   
val populationTemp = population.map(x => x.split(","))
				  .filter(d => d(0).trim() != "" && d(0).trim != "CBSA")
				   .map(c => (c(0) -> (c(1),c(2),c(4))))

val joinTemp = temp2.join(populationTemp)		   
 val k = joinTemp.map(T => (T._2._1) -> (T._1,T._2._2))

val percepRDD = temp1.reduceByKey((a,b) => (a + b))//percep file 
val d = percepRDD.join(k).map(t => (t._2._2._1 ->(t._2._1 , t._2._2._2._1,t._2._2._2._2,t._2._2._2._3)))
//d.saveAsTextFile(args(3))
val x = d.reduceByKey((a,b) => (((a._1 + b._1) * a._4.toInt)/(a._1), a._2,a._3,a._4)).saveAsTextFile(args(4))
//val t3 = t2.join(percepRDD).saveAsTextFile(args(3))
//val RedRDD = percepRDD.reduce  //reduced 
//val joinedFinal = percepRDD.join(joinTemp)
//joinedFinal.reduce()
//coalesce(1)sortByKey()
//percepRDD.saveAsTextFile(args(2))
//temp2.saveAsTextFile(args(3))
//joined.saveAsTextFile(args(4))
//.map(x => (x.split(",")(2), x)).partitionBy(obj)
//for (o <- empRDD) println("tuple first element = " + o._1 + " ---Partition = " + obj.getPartition(o._1) )
//
////println("Emplyee size -> "+ empRDD.collect.length)
//val deptRDD = sparkCont.textFile("/home/cloudera/Department.txt").map(x => (x.split(",")(0), x))
////println("dept size -> "+ deptRDD.collect.length)
//val joinedRDD = empRDD.join(deptRDD)
//joinedRDD.collect.foreach(println)
//println("number of Partitions = " + empRDD.partitions.size)
//
//val rePart = empRDD.repartition(2)

//println("Repartitioned ---> " + rePart.partitions.size)


}
}