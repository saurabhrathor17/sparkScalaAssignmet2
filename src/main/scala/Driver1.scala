import org.apache.spark.sql.SparkSession

object Driver1 extends App {

  implicit val spark: SparkSession = new SparkSession.Builder()
    .appName("spark assignment 2")
    .master("local[*]")
    .getOrCreate()

  // solution -1
  val csvFileDF = Service.readInDF("src/main/resources/ghtorrent-logs.txt")
  val cleanedFirstDF = Service.cleanFirstDF(csvFileDF)

  // solution - 2
  val countingRowsDF = Service.countRows(cleanedFirstDF)

  // solution -3
  val countingWarningDF = Service.warningsCounter(cleanedFirstDF)

  // solution - 4
  val totalRepoProcess = Service.totalProcessRepo(cleanedFirstDF)

  // solution -5
  val mostHtttpByClient = Service.mostHttp(cleanedFirstDF)

  // solution -6
  val mostFailedRequest = Service.mostFailedRequest(cleanedFirstDF)

  // solution -7
  val activeHourDay = Service.activeHour(cleanedFirstDF)

  //solution -8
  val activeRepo = Service.mostActiveRepo(cleanedFirstDF)


  println("total rows in dataframe  --------> " + countingRowsDF)
  println("total warning messages -----------> " + countingWarningDF)
  println("total api repo ------------>" + totalRepoProcess)
  println("most http by client ---------> " + mostHtttpByClient)
  println("most failed by client ----------> " + mostFailedRequest)
  println("most active hour of the day ----------> " + activeHourDay)
  println("most active repository -----------> " + activeRepo)
}
