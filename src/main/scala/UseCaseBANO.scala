import HelloWorldBigData.Session_Spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._


object UseCaseBANO {

  //Imposition d'un schema à la base BANO (base d'adresse national ouverte)

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("Code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)
  ))

  //Creation de la configuration Hadoop

  val confi_hadoop = new Configuration()
  val fs = FileSystem.get(confi_hadoop)
  val chemin_dest = new Path("C:\\Users\\offre de service\\Desktop\\destination")

  //Creation de la fonction principale

  def main(args: Array[String]): Unit = {
    val ss = Session_Spark(env = true)
    val df_bano_brute = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", true)
      .schema(schema_bano)
      .csv("C:\\Users\\offre de service\\Desktop\\Projet_BANO\\fichier csv\\full.csv")
   // df_bano_brute.show(3)

//Creation des colonnes "code_departement" et "libelle_source"

    val df_bano = df_bano_brute
      .withColumn("code_departement", substring(col("Code_postal"), 1, 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreetMap"))
        .otherwise(when(col("code_source_bano") === lit("OD"), lit("OpenData"))
          .otherwise(when(col("code_source_bano") === lit("O+O"), lit("OpenData OSM"))
            .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
              .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre OSM")))))))

//Creation des listes de departement

    //Creation en utilisant un dataframe
    val df_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)

//creation en utilisant une liste

    val liste_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)
      .collect()
      .map(x => x(0)).toList
  //  liste_departement.foreach(e =>println(e.toString))
  //  df_departement.show()

//Decoupage et ecriture des fichiers dans les dossiers spécifiques méthode 1
    liste_departement.foreach {
      x => df_bano.filter(col("code_departement") === x.toString)
          .coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter",";")
          .option("header","true")
          .mode(SaveMode.Overwrite)
          .csv("C:\\Users\\offre de service\\Desktop\\source\\bano" + x.toString)

       val chemin_source = new Path("C:\\Users\\offre de service\\Desktop\\source\\bano" + x.toString)
       fs.copyFromLocalFile(chemin_source, chemin_dest)
    }


 //Decoupage et ecriture des fichiers dans les dossiers spécifiques méthode 2
    /*
    df_departement.foreach{
      dep =>  df_bano.filter(col("code_departement") === dep.toString())
        .repartition(1)
        .write
        .format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("C:\\Users\\offre de service\\Desktop\\Projet BANO\\fichiers write1\\bano" + dep.toString)

        val chemin_source = new Path("C:\\Users\\offre de service\\Desktop\\Projet BANO\\fichiers write1\\bano")
        fs.copyFromLocalFile(chemin_source, chemin_dest)
    }

     */

  }
}
