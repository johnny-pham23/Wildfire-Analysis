package edu.ucr.cs.cs167.jpham

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.{ReadWriteMixinFunctions, SpatialRDD}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD


object WildFireAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WildFireAnalysis")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)
    val command: String = args(0)
    val inputfile: String = args(1)
    try {
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      command match{
        case "data-preparation" =>
          val fireDF = sparkSession.read.format("csv")
            .option("sep", "\t")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(inputfile)
          // Convert x,y to geometry for mapping
          val wildDF = fireDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry")
          val fireRDD: SpatialRDD = wildDF.selectExpr("x", "y", "acq_date", "cast(split(frp, ',')[0] AS double) frp", "acq_time", "geometry").toSpatialRDD
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
          val countyFiresRDD: RDD[(IFeature, IFeature)] = fireRDD.spatialJoin(countiesRDD)
          val countyFire: DataFrame = countyFiresRDD.map({case (county, fire) => Feature.append(county, fire.getAs[String]("GEOID"), "County")}).toDataFrame(sparkSession)
          val drDF = countyFire.drop("geometry")
          val outputFile: String = args(2)
          drDF.write.mode(SaveMode.Overwrite).parquet(outputFile)
        
         case "temporal-analysis" =>
          val CountyName: String = args(2)
          val df = sparkSession.read.parquet(inputfile)
          df.createOrReplaceTempView("WildfireDB")
          sparkContext.shapefile("tl_2018_us_county.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")
          df.show()
          df.printSchema()
          // Query 1
          val GEOID = sparkSession.sql(s"SELECT GEOID " +
                                         s"FROM counties " +
                                         s"WHERE NAME = '$CountyName' and STATEFP = '06' " +
                                         s"GROUP BY GEOID")
          // Convert GEOID query to string. 
          val GeoIDString = GEOID.first().getString(0)

          // GeoIDString is used in the Query 2
          val fireIntensity = sparkSession.sql(
            s"SELECT DATE_FORMAT(acq_date, 'yyyy-MM') as year_month" +
            s", SUM(frp) AS fire_intensity " +
            s"FROM WildfireDB " +
            s"WHERE County = '$GeoIDString'" +
            s"GROUP BY year_month " +
            s"ORDER BY year_month;"
          )
          // Write Query 2 to csv file. 
          fireIntensity.write.mode("overwrite").csv("wildfire" + CountyName)

        case "spatial-analysis" =>
          val us_county: String = args(2)
          val outputFile = args(3)
          val startDate = "01/01/2016"
          val endDate = "12/31/2017"
          val df1 = sparkSession.read.parquet((inputfile)).createOrReplaceTempView("WildfireDB")
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
        
          // Query 1
          val fireIntensityC2 = sparkSession.sql(
            s"""
               |SELECT WDB.County, SUM(WDB.frp) AS fire_intensity
               |FROM WildfireDB AS WDB
               |WHERE to_date(WDB.acq_date, 'yyyy-MM-dd') BETWEEN to_date('$startDate', 'MM/dd/yyyy') AND to_date('$endDate', 'MM/dd/yyyy')
               |GROUP BY WDB.County""".stripMargin).createOrReplaceTempView("fire_count")
        
          // Load the results of Q1 using Beast and convert to DF
          sparkContext.shapefile("tl_2018_us_county.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")
        
          // Equi-join counties and fire_count by GEOID
          // fire_count.County are listed as their GEOID
          val fireIntensityC3 = sparkSession.sql(
            s"""
               |SELECT counties.GEOID, fire_count.County, counties.g, fire_count.fire_intensity
               |FROM fire_count, counties
               |WHERE counties.GEOID = fire_count.County
               |""".stripMargin).toSpatialRDD.coalesce(1).saveAsShapefile(outputFile)

       


      }

    }

  }


}
