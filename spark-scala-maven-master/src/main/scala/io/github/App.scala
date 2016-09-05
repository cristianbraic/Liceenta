package io.github

import java.io.PrintWriter
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import ch.hsr.geohash.GeoHash

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object App {

  case class Measurement(id: Integer, time: Long, lat: Double, long: Double)

  case class TripPoint(id: Integer, time: Long, hash: String)

  val base32 = "0123456789bcdefghjkmnpqrstuvwxyz".toList
  val decodeMap = base32.zipWithIndex.toMap
  val sources = "C:\\Facultate\\Liceenta\\Sources\\"
  val results = "C:\\Facultate\\Liceenta\\Results\\"
  var i:Integer = 0

  def mid(interval: (Double, Double)) = (interval._1 + interval._2) / 2.0

  def decodeLimits(geohash: String): ((Double, Double), (Double, Double)) = {
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

    def dehash(xs: List[Boolean], min: Double, max: Double): (Double, Double) = {
      ((min, max) /: xs) {
        case ((min, max), b) =>
          if (b) ((min + max) / 2, max)
          else (min, (min + max) / 2)
      }
    }

    val (xs, ys) = split(toBitList(geohash))
    (dehash(ys, -90, 90), dehash(xs, -180, 180))
  }

  def decode(geohash: String): (Double, Double) = {
    decodeLimits(geohash) match {
      case ((minLat, maxLat), (minLng, maxLng)) => ((maxLat + minLat) / 2, (maxLng + minLng) / 2)
    }
  }

  def encode(lat: Double, lon: Double, precision: Int = 6): (String) = {
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

  def customLocationEquals(o1: Measurement, o2: Measurement) = {
    if ((o1 == null) || (o2 == null)) {
      true
    }
    else {
      o1.lat == o2.lat && o1.long == o2.long
    }
  }

  def main(args: Array[String]) {
    val sequencesFileWriter = new PrintWriter(results + "Sequences.txt")
    val frequentSequenciesWriter = new PrintWriter(results + "FrequentSequences.txt")

    val conf = new SparkConf().setAppName("Detectarea secventelor frecvente in trafic")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "24")
    conf.registerKryoClasses(Array(classOf[ArrayBuffer[String]], classOf[ListBuffer[String]]))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(sources + "taxi_february_remastered.txt")

    df.take(10).foreach(println)
    df.printSchema()

    val coordinatesRDD: RDD[TripPoint] = df.map({
      case Row(id: Int, time: Timestamp, lat: Double, long: Double) =>
        TripPoint(id, time.getTime, GeoHash.withCharacterPrecision(lat, long, 7).toBase32)
      case _ => TripPoint(0, 0, "")
    })

    coordinatesRDD.groupBy(c => c.id).map({ case (id, coordinates: Iterable[TripPoint]) =>
      var sorted: List[TripPoint] = coordinates.toList.sortBy(c => c.time)

      if(sorted.size %  2 == 1) {
        sorted = sorted.drop(1)
      }

      val cuttings = sorted
        .sliding(2)
        .filter(pair => pair.tail.head.time - pair.head.time > 1000 * 180)
        .map(_.tail.head).toList

      val tripEnds: List[TripPoint] = cuttings.map({
        case TripPoint(id: Integer, time: Long, hash: String) =>
          TripPoint(id, time, hash)
      })

      val sortedCoordinates: List[TripPoint] = coordinates.toList.sortBy(m => m.time)
      var rawTrips = List[List[TripPoint]]()

      for (i <- 0 until tripEnds.length - 1 by 2) {
        var trip = List[TripPoint]()
        for (coordinate <- sortedCoordinates) {
          if ((coordinate.time > tripEnds(i).time) && (coordinate.time < tripEnds(i + 1).time)) {
            trip = coordinate :: trip
          }
        }

        if (!trip.isEmpty) {
          trip = trip.groupBy(_.hash).map(_._2.head).toList
          rawTrips = trip :: rawTrips
        }
      }

      rawTrips
    }).collect.foreach(trip => {
      trip.foreach(h => {
        h.map {
          case h: TripPoint => {sequencesFileWriter.print(h.hash + " ")}
          case _ => ""}
        sequencesFileWriter.println()})
      })

    sequencesFileWriter.close()

    val data = sc.textFile(results + "Sequences.txt")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect.foreach ( itemset => {
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
      itemset.items.foreach(item =>
        frequentSequenciesWriter.println(GeoHash.fromGeohashString(item).getBoundingBoxCenterPoint.toString.stripPrefix("(").stripSuffix(")").trim)
      )}
    )

    frequentSequenciesWriter.close()
    sc.stop()
  }
}