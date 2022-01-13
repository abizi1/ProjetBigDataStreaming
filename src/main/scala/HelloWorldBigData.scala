import org.apache.spark.sql._

object HelloWorldBigData extends App {
  var ss: SparkSession = null

  /** *
   * Fonction permettant l'initialisation et l'instanciation d'une session spark
   *
   * @param Env  , lorsque Env = paramètre indiquant l'environnemet dans lequel notre application sera déployée
   *            Env = true => en local, false => en mode cluster
   */

  def Session_Spark(Env: Boolean = true): SparkSession = {

    if (Env == true) {
      System.setProperty("hadoop.home.dir", "C:/Hadoop")
      ss = SparkSession.builder()
        .master("local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      ss = SparkSession.builder()
        .appName("ma première application spark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    }
    ss
  }
}
