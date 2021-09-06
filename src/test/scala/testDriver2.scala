import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class testDriver2 extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("solution2")
    .master("local[*]").getOrCreate()

  import spark.implicits._

  val list = List("DEBUG, 2017-03-23T09:10:00+00:00, ghtorrent-34 -- retriever.rb: Commit Yangmaomao/jquery-pjax -> 8b442cbe679560fe468165226f4b377a6ee55589 exists",
    "INFO, 2017-03-23T09:17:36+00:00, ghtorrent-41 -- retriever.rb: Added issue_event GoogleCloudPlatform/google-cloud-dotnet 914->1008040145",
    "DEBUG, 2017-03-23T10:31:25+00:00, ghtorrent-32 -- ghtorrent.rb: Repo shuhongwu/hockeyapp exists",
    "INFO, 2017-03-23T09:07:41+00:00, ghtorrent-36 -- ght_data_retrieval.rb: Success processing event. Type: PushEvent, ID: 5530890983, Time: 58 ms",
    "DEBUG, 2017-03-23T10:15:28+00:00, ghtorrent-26 -- retriever.rb: Commit Overfinch/DesignPatternsPHP -> 3c288e10cfa4c5be3e0fb097046d88cf8db7b087 exists")

  val listTODF: DataFrame = list.toDF

  val DF: DataFrame = Service.cleanFirstDF(listTODF.as[String])

  //testCases - positive
  assert(Service.warningsCounter(DF) === 0)

  //testCases - negative
  //  assert(Service.warningsCounter(DF)===1)
}
