import HelloWorldBigData.Session_Spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.fs._
import org.apache.spark
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf._


object UseCaseBANO {

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType,false),
    StructField("numero_voie", StringType,false),
    StructField("nom_voie", StringType,false),
    StructField("Code_postal", StringType,false),
    StructField("nom_commune", StringType,false),
    StructField("code_source_bano", StringType,false),
    StructField("latitude", StringType,true),
    StructField("longitude", StringType,true)
  ))

  val confi_hadoop = new Configuration()
  val fs = FileSystem.get(confi_hadoop)

  def main(args:Array[String]):Unit = {
    val ss =Session_Spark(env = true)
    val df_bano_brute = ss.read
                          .format("csv")
                          .option("delimiter", ",")
                          .option("header",true)
                          .schema(schema_bano)
                          .csv("C:\\Users\\offre de service\\Desktop\\Projet BANO\\full.csv")


    val df_bano = df_bano_brute
      .withColumn("code_departement",substring(col("Code_postal"),1,2))
      .withColumn("libelle_source",when(col("code_source_bano")===lit("OSM"),lit("OpenStreetMap"))
        .otherwise(when(col("code_source_bano")===lit("OD"),lit("OpenData"))
        .otherwise(when(col("code_source_bano")===lit("O+O"),lit("OpenData OSM"))
        .otherwise(when(col("code_source_bano")===lit("CAD"),lit("Cadastre"))
        .otherwise(when(col("code_source_bano")===lit("C+O"),lit("Cadastre OSM")))))))

   val df_departement = df_bano.select(col("code_departement"))
                               .distinct()
                               .filter(col("code_departement").isNotNull)

    val liste_departement =df_bano.select(col("code_departement"))
                                  .distinct()
                                  .filter(col("code_departement").isNotNull)
                                  .collect()
                                  .map(x=> x(0)).toList


    liste_departement.foreach{
      x => df_bano.filter(col("code_departement") === x.toString)
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("C:\\Users\\offre de service\\Desktop\\Projet BANO\\fichiers write " + x.toString)

    }


}
