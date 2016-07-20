package io.github.adrianulbona

import java.io.PrintWriter
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object App {

  case class Measurement(id: Integer, time: Long, lat: Double, long: Double)

  val base32 = "0123456789bcdefghjkmnpqrstuvwxyz".toList
  val decodeMap = base32.zipWithIndex.toMap

  def mid(interval: (Double, Double)) = (interval._1 + interval._2) / 2.0

  def decode( geohash:String ):((Double,Double),(Double,Double)) = {
    def toBitList(s: String) = s.flatMap {
      c => ("00000" + base32.indexOf(c).toBinaryString).
        reverse.take(5).reverse.map('1' ==)
    } toList

    def split(l: List[Boolean]): (List[Boolean], List[Boolean]) = {
      l match {
        case Nil => (Nil, Nil)
        case x :: Nil => (x :: Nil, Nil)
        case x :: y :: zs => val (xs, ys) = split(zs); (x :: xs, y :: ys)
      }
    }

    def dehash( xs:List[Boolean] , min:Double,max:Double):(Double,Double) = {
      ((min,max) /: xs ){
        case ((min,max) ,b) =>
          if( b )( (min + max )/2 , max )
          else ( min,(min + max )/ 2 )
      }
    }

    val ( xs ,ys ) = split( toBitList( geohash ) )
    ( dehash( ys ,-90,90) , dehash( xs, -180,180 ) )
  }

  def encode(lat: Double, lon: Double, precision: Int = 12): (String) = {
    var idx = 0;
    // index into base32 map
    var bit = 0;
    // each char holds 5 bits
    var evenBit = true
    var geohash = ""

    var latI = (-90.0, 90.0)
    var lonI = (-180.0, 180.0)

    while (geohash.length < precision) {
      if (evenBit) {
        // bisect E-W longitude
        val lonMid = mid(lonI)
        if (lon > lonMid) {
          idx = idx * 2 + 1
          lonI = (lonMid, lonI._2)
        } else {
          idx = idx * 2
          lonI = (lonI._1, lonMid)
        }
      } else {
        // bisect N-S latitude
        val latMid = mid(latI)
        if (lat > latMid) {
          idx = idx * 2 + 1
          latI = (latMid, latI._2)
        } else {
          idx = idx * 2
          latI = (latI._1, latMid)
        }
      }
      evenBit = !evenBit
      bit = bit + 1
      if (bit == 5) {
        // 5 bits gives us a character: append it and start over
        geohash += base32(idx)
        bit = 0
        idx = 0
      }
    }

    return geohash
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hello Spark")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "24")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("C:\\Liceenta\\sample.txt")

    df.take(10).foreach(println)
    df.printSchema()

    val coordinatesRDD: RDD[Measurement] = df.map({
      case Row(id: Int, time: Timestamp, lat: Double, long: Double) =>
        Measurement(id, time.getTime, lat, long)
    })

    coordinatesRDD.groupBy(c => c.id).map({ case (id, coordinates: Iterable[Measurement]) =>
      val sorted: List[Measurement] = coordinates.toList.sortBy(c => c.time)

      val cuttings = sorted
        .sliding(2)
        .filter(pair => pair.tail.head.time - pair.head.time > 1000 * 180)
        .map(_.tail.head).toList

      val tripEnds: List[Measurement] = cuttings.map({
        case Measurement(id: Integer, time: Long, lat: Double, long: Double) =>
          Measurement(id, time, lat, long)
      })

      val sortedCoordinates: List[Measurement] = coordinates.toList.sortBy(m => m.time)
      var trips = List[List[Measurement]]()

      for (i <- 0 until tripEnds.length - 1 by 2) {
        var trip = List[Measurement]()
        for (coordinate <- sortedCoordinates) {
          if ((coordinate.time > tripEnds(i).time) && (coordinate.time < tripEnds(i + 1).time)) {
            trip = coordinate :: trip
          }
        }

        trips = trip :: trips
      }

      new PrintWriter("C:\\Liceenta\\Results\\trips.txt") {
        for (trip <- trips) {
          //println("New Trip")
          for (point <- trip) {
            println(point.id + ","
              + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(point.time) + ","
              + point.lat + ","
              + point.long)
          }
        }
      }

      new PrintWriter("C:\\Liceenta\\Results\\geohash.txt") {
        for (trip <- trips) {
          //println("New Trip")
          for (point <- trip) {
            val geohash = encode(point.lat, point.long)
            println(point.id + ","
              + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(point.time) + ","
              + point.lat + ","
              + point.long + "    encoded: "
              + geohash + "    decoded: "
              + decode(geohash))
          }
        }
      }

      trips.size
    }).collect.foreach(println)

    sc.stop()
  }
}