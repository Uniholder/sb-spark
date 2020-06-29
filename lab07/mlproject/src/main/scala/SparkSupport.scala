import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSupport {
  val sc = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(sc)
    .getOrCreate()
}
