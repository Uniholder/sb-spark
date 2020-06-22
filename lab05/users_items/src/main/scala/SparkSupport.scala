import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

trait SparkSupport {
  val sc = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(sc)
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val input_dir = spark.conf.get("spark.users_items.input_dir")
  val output_dir = spark.conf.get("spark.users_items.output_dir")
  val update = spark.conf.get("spark.users_items.update")

  def getLatestPath(path : String) : String = {
    if (path.startsWith("file")) {
      val dir = path.replaceFirst("file:/", "")
      val dateDirs = (new File(dir))
        .listFiles
        .filter(_.isDirectory)
        .map(_.getName)
      path + dateDirs.max
    }
    else {
      val fs = FileSystem.get(new Configuration())
      val status = fs.listStatus(new Path(path))
      status.map(x=> x.getPath.toString).toList.max
    }
  }
}
