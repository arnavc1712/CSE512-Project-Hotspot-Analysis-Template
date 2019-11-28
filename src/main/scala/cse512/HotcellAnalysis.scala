package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
//import scala.math._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("intermediate")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minimums = spark.sql("select min(x) as min_x,min(y) as min_y,min(z) as min_z from intermediate").first()

  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells:Double = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  val uniqueCombsDF = spark.sql("select x,y,x,count(*) as cnt from intermediate group by z,x,y order by z,x,y")

  uniqueCombsDF.createOrReplaceTempView("uniqueCombCount")
  spark.udf.register("Square",(num:Int)=>((num*num)));

  val sumPointsDF = spark.sql("select sum(cnt) as sum,Square(cnt) as sum_squared from uniqueCombCount")

  val sumPoints = sumPointsDF.first().getInt(0) //Only used for mean calculation
  val sumSquared:Double = sumPointsDF.first().getLong(1) //Only used for SD calculation

  val mean:Double = sumPoints/numCells;
  //val mean = mean.asInstanceOf[Double]
  //val i:Double = 2.0;
  val intermed:Double = sumSquared/numCells
  val power:Double = mean*mean
  val value:Double = intermed - power
  val SD:Double = math.sqrt(value)

  spark.udf.register("CalcNumNeighbors",(minX:Int,maxX:Int,minY:Int,maxY:Int,minZ:Int,maxZ:Int,X:Int,Y:Int,Z:Int) => ((
    HotcellUtils.countNeighbors(minX,maxX,minY,maxY,minZ,maxZ,X,Y,Z)
  )))
  val neigboursSum = spark.sql(f"select CalcNeighbors($minX%s,$maxX%s,$minY%s,$maxY%s,$minZ%s,$maxZ%s,l1.x,l1.y,l1.z) l1.x,l1.y,l1.z, sum(l2.count) as n_count from uniqueCombsDF l1,uniqueCombsDF l2 where (l1.x=l2.x and l1.y=l2.y and l1.z=l2.z) or (l2.x=l1.x-1 and l2.y=l1.y-1 and l2.z=l1.z-1) or (l2.x=l1.x+1 and l2.y=l1.y+1 and l2.z=l1.z+1)")












  //return pickupInfo // YOU NEED TO CHANGE THIS PART
  return neigboursSum
}
}
