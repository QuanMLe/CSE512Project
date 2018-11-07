package cse512

import org.apache.spark.sql.SparkSession
import scala.math._

object SpatialQuery extends App{
    def ST_Contains(arg1: String, arg2: String): Boolean = {
            //split point
            val result = arg1.split(",")
            val x = result(0).toDouble
            val y = result(1).toDouble

            //split rectangle points
            val rect_vals = arg2.split(",")
            val x1 = rect_vals(0).toDouble
            val y1 = rect_vals(1).toDouble
            val x2 = rect_vals(2).toDouble
            val y2 = rect_vals(3).toDouble

            //Checks if the coordinates are within the rectangle
            if ((x >= x1 && x <= x2) && (y >= y1 && y <= y2)){
                return true
            }
            else{
                return false
            }
        }    

    def ST_Within(arg1: String, arg2: String, arg3: Double): Boolean = {
        val r1 = arg1.split(",")
        val r1x = r1(0).toDouble
        val r1y = r1(1).toDouble

        val r2 = arg2.split(",")
        val r2x = r2(0).toDouble
        val r2y = r2(1).toDouble

        val dist = arg3
        val pt_dist = Math.sqrt(((r2x-r1x)*(r2x-r1x))+((r2y-r1y)*(r2y-r1y)))
        if (pt_dist <= dist){
            return true
        }
        else{
            return false
        }
    }

    //find all points within given rectangle
    def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(SpatialQuery.ST_Contains(queryRectangle, pointString)))

        val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
        resultDf.show()

        return resultDf.count()
    }

    //modify this to fit requirements
    //find all point/rectangle pairs
    def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point")

        val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
        rectangleDf.createOrReplaceTempView("rectangle")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(SpatialQuery.ST_Contains(queryRectangle, pointString)))

        val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
        resultDf.show()

        return resultDf.count()
    }

    //find all points that are a given distance from point
    def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(SpatialQuery.ST_Within(pointString1, pointString2, distance)))

        val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
        resultDf.show()

        return resultDf.count()
    }

//given point s1 and s2 and a distance, finds all pairs such that s1 is the given distance from s2
    def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point1")

        val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
        pointDf2.createOrReplaceTempView("point2")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(SpatialQuery.ST_Within(pointString1, pointString2, distance)))
        val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
        resultDf.show()

        return resultDf.count()
    }
}
