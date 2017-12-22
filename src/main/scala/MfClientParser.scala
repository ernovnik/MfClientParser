package ru.spark.testapp

import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

/*Приложение для вычисления неуникального идентификатора клиента по ФИО и дате рождения
* со линейной сложностью*/

object MfClientParser extends App {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Megafon ")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()
    val dataInput = "src/main/resources/data.csv"
    val df = spark.read.option("delimiter", "|")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("DROPMALFORMED", "true")
      .option("dateFormat", "yyyy.MM.dd")
      .csv(dataInput).toDF
    df.show()
    //Алгоритм преобразования строк, который строки типа Ivanoff, Ivanov приводит к одной строке
    val dm = new DoubleMetaphone()
    //Года сворачиваются в число, таким образом нам неважен порядок месяца, дня, года
    val hashDate = udf { (s: String) => s.replace("[^0-9]", "").foldLeft[Int](0)((a, b) => a + b.asDigit * 5) }

    val inputNames = spark.sparkContext.textFile("src/main/resources/names.csv").map(line => line.split(" ").toSeq)

    /* Имена хранятся в map, все варианты написания имен приводятся к одному основному варианту*/
    val namesMap = inputNames.map(x => x(0) -> x(1)).collectAsMap()

    /*Замена небуквенных символов на пробелы*/
    val nonWordCharsRemove = udf { (s: String) ⇒
      s.toLowerCase()
        .replaceAll("\\W+", " ")
        .split("\\W")
        .mkString(" ")
    }

  /*Определяет, в какой позиции имя, в какой фамилия. ФИО обычно начинается с имени, либо с фамилии
  * Если имя на первом месте, то фамилия на третьем. Если имя на втором, то фамилия на первом.
  * Отчество не попадает в конечную строку. Далее объединяем имя и фамилию, вычисляем double metahpone*/
    val findNameParts = udf { (s: String) =>
      val spl = s.split("\\s")
      val nameIndex = findNamePosition(spl)
      val firstName = namesMap(spl(nameIndex))
      val secondName = (nameIndex,spl.size) match{
        case (0,2) => spl(1)
        case (1,2) => spl(0)
        case (0,3) => spl(2)
        case (1,3) => spl(0)
        case _ => ""
      }
      (firstName :: secondName :: Nil).map(dm.doubleMetaphone(_)).mkString("")
    }
    def findNamePosition(s: Array[String]): Int = {
      var pos = -1
      var i = 0
      while (i < s.size) {
        if (namesMap.contains(s(i)))
          pos = i
        i += 1
      }
      pos
    }
    val df1 = df
      .withColumn("onlywordsfio", nonWordCharsRemove(col("FIO")))
      .withColumn("hdt", hashDate(col("BirthDate")))
    val df2 = df1
      .withColumn("fio_refined", findNameParts(col("onlywordsfio")))
    val getConcatenated = udf( (first: String, second: String) =>{(first + second).toLowerCase})
    val df3 = df2.withColumn("id", getConcatenated(col("fio_refined"),col("hdt")) )
      .drop("hdt")
      .drop("fio_refined")
      .drop("onlywordsfio")
    val df4 = df3.select("id","FIO","BirthDate")
    df4.show(false)
    spark.stop()
}
