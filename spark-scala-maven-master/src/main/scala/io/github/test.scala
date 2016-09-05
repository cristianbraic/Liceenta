package io.github

/**
  * Created by cristian on 9/5/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
// $example on$
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
// $example off$

object SimpleFPGrowth {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Detectarea secventelor frecvente in trafic")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "24")
    conf.registerKryoClasses(Array(classOf[ArrayBuffer[String]], classOf[ListBuffer[String]]))

    val sc = new SparkContext(conf)

    // $example on$
    val data = sc.textFile("C:/Facultate/Liceenta/Sources/ceva.txt")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth()
      .setMinSupport(0.05)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    // $example off$
  }
}