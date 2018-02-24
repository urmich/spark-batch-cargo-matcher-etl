import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.abs

object CargoToTransporterMatchReporter {

  val TIME_DIFFERENCE = 15 * 60 * 1000
  val LONGITUDE = 0
  val LATITUDE = 1
  val transporterCoordinatesIndexes = Array(2, 3)
  val cargoCoordinatesIndexes = Array(14, 15)

  def main(args: Array[String]) {

    if(args.length < 3 ||
      args(0) == null || args(0).length == 0 ||
      args(1) == null || args(1).length == 0 ||
      args(2) == null || args(2).length == 0
    ){
      println("Missing resource location parameter")
    }

    val TRANSPORTERS_FILE = args(0)
    val CARGO_FILE = args(1)
    val OUTPUT_FILE = args(2)

    val conf = new SparkConf().setAppName("CargoToTransporterMatchReporter").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val transporters = sc.textFile(TRANSPORTERS_FILE)
    val transportersRDD = transporters.mapPartitionsWithIndex(
      (i, iterator) =>
        if (i == 0 && iterator.hasNext) {
          iterator.next
          iterator
        } else iterator)

    val transportersGroupedByKey = transporters
      .map(_.split('\t'))
      .keyBy(line => line(transporterCoordinatesIndexes(LONGITUDE)).concat("*").concat(line(transporterCoordinatesIndexes(LATITUDE))))
      .groupByKey()

    val cargo = sc.textFile(CARGO_FILE)
    val cargoRDD = cargo.mapPartitionsWithIndex(
      (i, iterator) =>
        if (i == 0 && iterator.hasNext) {
          iterator.next
          iterator
        } else iterator)

    //add 1 more element to array to later store the transporter id
    //create key
    //group by key
    val validCargoGroupedByKey = cargoRDD
      .map(_.split('\t'))
      .filter(_ (6).contains("/ Valid"))
      .map(line => line :+ "")
      .keyBy(line => line(cargoCoordinatesIndexes(LONGITUDE)).concat("*").concat(line(cargoCoordinatesIndexes(LATITUDE))))
      .groupByKey()

    val joinedCargoAndTransporters = validCargoGroupedByKey.leftOuterJoin(transportersGroupedByKey)

    val matchedCargo =
      joinedCargoAndTransporters
        .map(line => matchAndUpdateCargoData(line))
          .flatMap { case (key, innerList) => innerList.map(key -> _) }
          .map(line => line._2)
          .map(array => array.reduce((el1, el2) => el1 + "\t" + el2))
        .saveAsTextFile(OUTPUT_FILE)

  }

  def matchAndUpdateCargoData(line: Tuple2[String, (Iterable[Array[String]], Option[Iterable[Array[String]]])] ): Tuple2[String, Iterable[Array[String]]] = {

    if(line._2._2 == None){
      return (line._1, line._2._1)
    }

    val cargoDateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm")
    val transporterDateFormatter = new SimpleDateFormat("MM/dd/yy HH:mm")
    val key = line._1
    val cargoDataArray = line._2._1
    val transporterDataArray = line._2._2.get
    var updatedCargoDataArray = scala.collection.mutable.ArrayBuffer.empty[Array[String]]

    for (
      cargoData <- cargoDataArray;
      transporterData <- transporterDataArray
    ) {
        val cargoDate = cargoDateFormatter.parse(cargoData.apply(0))
        val transporterSignalDate = transporterDateFormatter.parse(transporterData.apply(1))
        if(
          cargoData(cargoCoordinatesIndexes(LONGITUDE)).equalsIgnoreCase(transporterData(transporterCoordinatesIndexes(LONGITUDE))) &&
          cargoData(cargoCoordinatesIndexes(LATITUDE)).equalsIgnoreCase(transporterData(transporterCoordinatesIndexes(LATITUDE))) &&
          abs(transporterSignalDate.getTime() - cargoDate.getTime()) < TIME_DIFFERENCE
        ){
          cargoData(cargoData.length - 1) = transporterData(0)
       }
      updatedCargoDataArray += cargoData
    }

    return (key, updatedCargoDataArray)
  }
}
