import com.rockymadden.stringmetric.phonetic.RefinedSoundexAlgorithm
import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MfClientParser extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Megafon ")
    .config("spark.sql.warehouse.dir", ".")
    .getOrCreate()
  val input = "src/main/resources/data.csv"
  val df = spark.read.option("delimiter","|")
    .option("inferSchema", "true")
    .option("header","true")
    .option("DROPMALFORMED","true")
    .option("dateFormat","yyyy.MM.dd")
    .csv(input).toDF
  df.show()

  val dm = new DoubleMetaphone()



  import org.apache.spark.sql.functions.to_timestamp

  val refinedSoundex = udf{(s:String)=>  RefinedSoundexAlgorithm.compute(s)}




  val countTokens = udf {(s:String)⇒

    s.replaceAll("\\W", " ")
      .split("\\s")
      .filter(_.length > 2)
      .map(dm.doubleMetaphone)
      .mkString("#").toLowerCase

  }



  val nonWordCharsRemove = udf {(s:String)⇒


    val s1 = StringUtils.stripAccents(s)


    import org.apache.spark.ml.feature.HashingTF




    s1
      //.split("\\W+").
      //map(_.trim)
      //.mkString(" ")
      .split("(?<=[a-z])(?=[A-Z])")
      .filter(_.length >= 2)
      .mkString(" ")
      .toLowerCase()}

  val hashingTF = new HashingTF().setInputCol("fiotok").setOutputCol("fiotoktf").setNumFeatures(100)




  val tokenizer = new Tokenizer().setInputCol("FIO").setOutputCol("fiotok")

  val tokenized = tokenizer.transform(df)

  tokenized.show(false)




  val df3 = df
    .withColumn("onlywordsfio",nonWordCharsRemove(col("FIO")))
    .withColumn("dt", to_timestamp(col("BirthDate"), "yyyy.MM.dd"))
    .withColumn("dtStringMd5", md5(to_timestamp(col("BirthDate"), "yyyy.MM.dd").cast("String")))
    .withColumn("dtStringSha1", substring(sha1(to_timestamp(col("BirthDate"), "yyyy.MM.dd").cast("String")),0,3))

      .withColumn("FIO1", split(col("FIO"), "\\s").getItem(0))
    .withColumn("FIO2", split(col("FIO"), "\\s").getItem(1))
    .withColumn("FIO3", split(col("FIO"), "\\s").getItem(2))

  df3.show(false)


  val rawFeaturesDF = hashingTF.transform(tokenized)
  rawFeaturesDF.show(false)




  spark.stop()
}
