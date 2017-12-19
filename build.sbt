name := "mfuserparser"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.0-M11"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1"
