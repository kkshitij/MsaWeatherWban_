package org.maybatch.sparkproject

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd._
import scala.collection.JavaConversions._
import org.apache.commons.math3.stat.descriptive.summary.Sum
import org.apache.hadoop.fs.shell.Count
import org.apache.log4j.Logger
import org.joda.time.format._
import org.joda.time.DateTime
import org.joda.time.YearMonthDay
import org.joda.time.LocalDate


//comment Final
object percipcompute {

var datef = DateTimeFormat.forPattern("yyyyMMdd")
      
  def main(args: Array[String]) {


var dateM = new LocalDate("2015-05-01")
val sparkConf = new SparkConf().setAppName("kshitij") // YARN 
//val sparkConf = new SparkConf().setMaster("local").setAppName("kshitij") //local
val sparkCont = new SparkContext(sparkConf)
val log = Logger.getLogger(getClass.getName)

val accumPrecepInvRec = sparkCont.accumulator(0, "Invalid Record")
val accumPrecepValRec = sparkCont.accumulator(0, "Valid Record")
val accumPrecepInvWBAN = sparkCont.accumulator(0, "Invalid WBANS")
val accumPrecepInvDate = sparkCont.accumulator(0, "Invalid Date")
val accumPrecepNotMayDate = sparkCont.accumulator(0, "Not May Date")
val accumPrecepInvHr = sparkCont.accumulator(0, "Invalid Hr")
val accumPrecepLess7Hr = sparkCont.accumulator(0, "Records with 12 AM to7 AM")
val accumPrecepInvPrecip = sparkCont.accumulator(0, "Invalid Precipitation")
val accumStationInvRec = sparkCont.accumulator(0, "Station - Invalid Record")
val accumStationInvWBAN = sparkCont.accumulator(0, "Station - Invalid WBAN")
val accumStationInvCBSA = sparkCont.accumulator(0, "Station - Invalid CBSA")
val accumStationBlankCBSA = sparkCont.accumulator(0, "Station - Blank CBSA")
val accumPopulaInvRec = sparkCont.accumulator(0, "Invalid Record")
val accumPopulaBlankCBSA = sparkCont.accumulator(0, "Invalid Record")
val accumPopulaInvalidPop = sparkCont.accumulator(0, "Invalid Record")

val percep = sparkCont.textFile(args(0))
val station = sparkCont.textFile(args(1))
val population = sparkCont.textFile(args(2))


println ("HHHHHHHHHHHHH PREEEEEEEEEECIP")
val percepTemp1 = percep.map(x => { if (x == "" ) { accumPrecepInvRec += 1 }
							     x.split(",") })
  						.filter(T => {          	  
  							(if (validateInt(T(0)) == None) {accumPrecepInvWBAN += 1 ; false} 
							 else {true}) &&
							(validateDate(T(1)) match {
							  case Some(i) if (!(i.getMonthOfYear().equals(5) && i.getYear().equals(2015))) => accumPrecepNotMayDate += 1;  false
							  case None => accumPrecepInvDate += 1 ; false
							   case _ =>  true
  							}) &&
    						(validateInt(T(2)) match {
							  case Some(i) if (i <= 07) =>  {  accumPrecepLess7Hr += 1
							    
							    ; false}
							  case None => accumPrecepInvHr += 1 ; false
							   case _ =>  true
  							}) &&
    						(validateFloat(T(3)) match {
							  case None => accumPrecepInvPrecip += 1 ; false
							   case _ =>  true
    						})
  							
})       	 
         	
     //val h = percepTemp1.map(x => (x(0) ,x(1) ,x(2) ,x(3)) )  //Intermediate  (to debug	
val precepTemp = percepTemp1.map(x => (x(0).toInt,x(3).toFloat) ).reduceByKey((a,b) => a + b)

//###################Station#############

val stationTemp = station.map(x => { if (x == "" ) { accumStationInvRec += 1 }
							    x.split(",",-1) }).filter(T => {          	  
  							(if (validateInt(T(0)) == None) {accumStationInvWBAN += 1 ; false} 
							 else {true}) &&							 
							 (validateInt(T(15)) match {
							   case Some(i) if(i < 0) => accumStationInvCBSA += 1 ; false
					     	   case None if("".equals(T(15).trim)) => accumStationBlankCBSA += 1 ; true
					     	   case None  => accumStationInvCBSA+= 1 ; false
							   case _ =>  true
  							})
							    })
							    
						//val IntermedStation =  stationTemp.map( t => (t(0).toInt,validateInt(t(15)).getOrElse(00000),t(3),t(4),t(5),t(6),t(7),t(8))).saveAsTextFile(args(4)) //
						val IntermedStation1 =  stationTemp.map( t => (t(0).toInt,validateInt(t(15)).getOrElse(00000)))							    

						val JoinedPrecip_Station = precepTemp.join(IntermedStation1)
				
						//						val l = JoinedPrecip_Station.map(a => (a._2._2 -> (a._2._1,1))).reduceByKey((a,b) => ((a._1 + b._1)/(a._2 + b._2),1)).saveAsTextFile(args(3))
					val Precep_StationF = JoinedPrecip_Station.map(a => (a._2._2 -> (a._2._1,1))).reduceByKey((a,b) => ((a._1 + b._1)/(a._2 + b._2),1)).persist//.saveAsTextFile(args(3))
					println ("CAAAAAAAAAACHED JOINED")	
					//val Precep_StationFInter = Precep_StationF.saveAsTextFile(args(4))
			

//###################population#############		
					
		val	populationTemp =  population.map(x => { if (x == "" ) { accumPopulaInvRec += 1 }
							     x.split(",") })
  						.filter(T => {          	  
  							(if (validateInt(T(0)) == None) {accumPopulaBlankCBSA += 1 ; false} 
							 else {true}) &&
							(validateInt(T(4)) match {
							  case Some(i) if (i <= 0) =>  {  accumPopulaInvalidPop += 1 ; false}
							  case None => accumPopulaInvalidPop += 1 ; false
							   case _ =>  true
  							})
  							
}).map(T => (T(0).toInt, (T(1) + T(2) , T(4).toInt)))
		  
		  
		val broadcastedPopulation = sparkCont.broadcast(populationTemp.collectAsMap)
							println ("CAAAAAAAAAACHED JOINED")	

println(broadcastedPopulation.value)
		
	//	val j = Precep_StationF.map(a => (a._1 ,broadcastedPopulation.value.get(a._1))).saveAsTextFile(args(4)) //Intermediate
val Final = Precep_StationF.map(a => {
  var temp : Int = 0
  var MSADet : String = "Others"
   broadcastedPopulation.value.get(a._1) match {
    case Some(i) => {
      
    temp = broadcastedPopulation.value.get(a._1).get._2
    MSADet = broadcastedPopulation.value.get(a._1).get._1
    }
    case None => {   temp = 0
    MSADet = "Others"}
        
  }
  
  (a._1,temp,a._2._1,temp*a._2._1,MSADet)
}).sortBy(a => a._4 , false, 1).saveAsTextFile(args(3))



}
		

  
  
  def validateInt( inpStr: String ): Option[Int] = {
     
    try {
    		Some(inpStr.trim.toInt)           //
  } catch {
    case e:  Exception => None 
    }
    		
    
}

  def validateDate( inpStr: String ): Option[LocalDate] = {
     
    try {
  
    		Some(datef.parseLocalDate(inpStr.trim))
  } catch {
    case e:  Exception => None 
    }
    		
    
}
  
    def validateFloat( inpStr: String ): Option[Float] = {
     
    try {
    		Some(inpStr.trim.toFloat)           //
  } catch {
    case e:  Exception => None 
    }
    		
    
}
}
  
  
  
