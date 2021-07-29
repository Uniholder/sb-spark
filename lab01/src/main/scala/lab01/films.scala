package lab01

import java.io.FileWriter
import scala.io.Source._
import scala.collection.JavaConverters._
import com.google.gson.Gson
import java.util.List

case class Films(hist_film: List[Int], hist_all: List[Int])

object Film extends App {
  println(System.getProperty("user.dir"))
  val path = "u.data"
  val target_id = "237"

  val input = fromFile(path)
  val parsedInput = input
    .getLines
    .map(_.split("\t"))
    .map { x => (x(1), x(2).toInt) }
    .toList
  input.close()

  val my_film = parsedInput
    .filter(_._1.equals(target_id))
    .groupBy(_._2)
    .mapValues(_.size)
    .toSeq
    .sortBy(_._1)
    .map(x => x._2)
    .toList
    .asJava

  val all_films = parsedInput
    .groupBy(_._2)
    .mapValues(_.size)
    .toSeq
    .sortBy(_._1)
    .map(x => x._2)
    .toList
    .asJava

  val answer = Films(my_film, all_films)
  val g = new Gson
  val jsonString = g.toJson(answer)
  println(jsonString)

  val wr = new FileWriter("lab01.json")
  wr.write(jsonString)
  wr.close()
  println("End")
}
