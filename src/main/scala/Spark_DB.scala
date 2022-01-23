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
    // definition des propriétés de connexion à la base Mysql //

    val propriete_mysql = new Properties()
    propriete_mysql.put("user","consultant")
    propriete_mysql.put("password","pwd#86")

    /*

    // definition des propriétés de connexion à la base SqlServer //
    val propriete_SQLServer = new Properties()
    propriete_SQLServer.put("user", "consultant")
    propriete_SQLServer.put("password", "pwd#86")

     */

    // definition des propriétés de connexion à la base PostgreSQl //
    val propriete_postgreSQL = new Properties()
    propriete_postgreSQL.put("user", "postgres")
    propriete_postgreSQL.put("password", "pwd#86")


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


    val df_postgre = ss.read.jdbc("jdbc:postgresql://127.0.0.1:5432/jea_db","orders", propriete_postgreSQL)

    val df_postgre2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/jea_db")
      .option("user", "postgres")
      .option("password", "pwd#86")  //
      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from orders group by state, city) table_postgresql")
      .load()

/*
    val df_sqlserver = ss.read.jdbc("jdbc:sqlserver://JUVENAL\\SPARK_SERVER:1433;databaseName=jea_db;","orders",propriete_SQLServer)

    val df_sqlserver1  = ss.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://JUVENAL\\SPARK_SERVER:1433;databaseName=jea_db;integratedSecurity=true")
      .option("dbtable", "(select state, city, sum(numunits * totalprice) as commandes_totales from orders group by state, city) table_sqlserver")
      .load()

    df_sqlserver.show(10)

*/
  }
}
