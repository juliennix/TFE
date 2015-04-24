name := "spark_code"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "net.sf.jung" % "jung2" % "2.0.1"

libraryDependencies += "net.sf.jung" % "jung-api" % "2.0.1"

libraryDependencies += "net.sf.jung" % "jung-graph-impl" % "2.0.1"

libraryDependencies += "net.sf.jung" % "jung-visualization" % "2.0.1"

libraryDependencies += "net.sf.jung" % "jung-algorithms" % "2.0.1"

libraryDependencies += "net.sf.jung" % "jung-io" % "2.0.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.2.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.2.1"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.1"

libraryDependencies += "org.jfree" % "jfreechart" % "1.0.14"
