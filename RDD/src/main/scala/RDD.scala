import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.max
import scala.util.Try
import org.apache.log4j.Logger
import org.apache.log4j.Level

object RDD extends App{
  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sc = new SparkContext()
    val rdd = sc.textFile("airport-codes.csv")
    //rdd.take(3).foreach(println)

    case class AirportSafe(
                            ident: String,
                            `type`: String,
                            name: String,
                            elevationFt: Option[Int],
                            continent: String,
                            isoCountry: String,
                            isoRegion: String,
                            municipality: String,
                            gpsCode: String,
                            iataCode: String,
                            localCode: String,
                            longitude: Option[Float],
                            latitude: Option[Float]
                          )
    val firstElem = rdd.first
    val noHeader = rdd.filter(x => x != firstElem).map(x => x.replaceAll("\"", ""))
    //println(noHeader.first)

    def toAirportOptSafe(data: String): Option[AirportSafe] = {
      val airportArr: Array[String] = data.split(",", -1)

      airportArr match {
        case Array(
        ident,
        aType,
        name,
        elevationFt,
        continent,
        isoCountry,
        isoRegion,
        municipality,
        gpsCode,
        iataCode,
        localCode,
        longitude,
        latitude) => {

          Some(
            AirportSafe(
              ident = ident,
              `type` = aType,
              name = name,
              elevationFt = Try(elevationFt.toInt).toOption,
              continent = continent,
              isoCountry = isoCountry,
              isoRegion = isoRegion,
              municipality = municipality,
              gpsCode = gpsCode,
              iataCode = iataCode,
              localCode = localCode,
              longitude = Try(longitude.toFloat).toOption,
              latitude = Try(latitude.toFloat).toOption
            )
          )
        }
        case _ => Option.empty[AirportSafe]
      }
    }
    val airportFinal: RDD[AirportSafe] = noHeader.flatMap(toAirportOptSafe)
    //airportFinal.take(3).foreach(println)
    //println(airportFinal.count)

    val pairAirport = airportFinal.map(x => (x.isoCountry, x.elevationFt))
    //println(pairAirport.first)

    val fixedElevation: RDD[(String, Int)] = pairAirport.map {
      case (k, Some(v)) => (k, v)
      case (k, None) => (k, Int.MinValue)
    }
    //println(fixedElevation.first)

    println(fixedElevation.getNumPartitions)
    val result = fixedElevation.reduceByKey { (x, y) => max(x,y) }.collect.sortBy( x => -x._2 )
    result.take(10).foreach(println)

    sc.stop
  }
}
