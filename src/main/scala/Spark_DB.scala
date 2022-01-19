//Construction d'une application qui permet de lire des données Mysql
//depuis Saprk (on premises)//
import HelloWorldBigData.Session_Spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util._


object Spark_DB {

  def main(args: Array[String]): Unit = {
    val ss= Session_Spark(true )
    // definition des propriétés de connexion à la base Mysql pour loader une table entière//

    val propriete_mysql = new Properties()
    propriete_mysql.put("user","consultant")
    propriete_mysql.put("password","pwd#86")
    //lecture et enregistrement dans un Dataframe des données collectées sur Mysql avec Spark//

    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC",
    "jea_db.orders",propriete_mysql)
    df_mysql.show(5)


    //Propriétés pour faire des requetes sur la base depuis mysql //

    val df_mysql2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) requete")
      .load()
       df_mysql2.show(5)


  }
}
