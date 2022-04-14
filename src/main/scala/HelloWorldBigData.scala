import java.io.FileNotFoundException
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.streaming._
import org.apache.log4j._
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.UserDefinedFunction



object HelloWorldBigData extends App {
  var ss: SparkSession = null
  private var trace_log : Logger = LogManager.getLogger("Logger_Console")

    /**
     * fonction qui initialise et instancie une session spark
     * @param env : c'est une variable qui indique l'environnement sur lequel notre application est déployée.
     *            Si Env = True, alors l'appli est déployée en local, sinon, elle est déployée sur un cluster
     */
    def Session_Spark (env :Boolean = true) : SparkSession = {


      try {
        if (env == true) {
          System.setProperty("hadoop.home.dir", "C:/Hadoop/")
          ss = SparkSession.builder
            .master("local[*]")
            .config("spark.sql.crossJoin.enabled", "true")
            //    .enableHiveSupport()
            .getOrCreate()
        } else {
          ss = SparkSession.builder
            .appName("Mon application Spark")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.crossJoin.enabled", "true")
            .enableHiveSupport()
            .getOrCreate()
        }
      } catch {
        case ex: FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
        case ex: Exception => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace())
      }

      return ss
    }
    def division(numerateur:Double, denominateur:Double):Double={
      return numerateur/denominateur

      val liste = List[Int](1,2,3,4)

    
  }


}
