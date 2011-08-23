import sbt._
//import com.github.retronym.OneJarProject

class WikiCrunchProject(info : ProjectInfo) extends DefaultProject(info) with ProguardProject
{
    val lucenecore = "org.apache.lucene" % "lucene-core" % "3.0.3"
    val scalatest = "org.scalatest" % "scalatest" % "1.3"
    val berkeleydb = "berkeleydb" % "je" % "3.2.76"
    val commonsMath = "org.apache.commons" % "commons-math" % "2.0"
    val automaticresourcemanagement = "com.github.jsuereth.scala-arm" % "scala-arm_2.8.0" % "0.2"
    val httpclient = "org.apache.httpcomponents" % "httpclient" % "4.1.1"
    val commonsCollections = "commons-collections" % "commons-collections" % "3.0"
    //val jung2 = "net.sf.jung" % "jung2" % "2.0.1"
    //val jgrapht = "jgrapht" % "jgrapht" % "0.7.3"
    //val jungApi = "net.sf.jung" % "jung-api" % "2.0.1"
    //val jungGraphImpl = "net.sf.jung" % "jung-graph-impl" % "2.0.1"
    //val jungAlgorithms = "net.sf.jung" % "jung-algorithms" % "2.0.1"
    val h2 = "com.h2database" % "h2" % "1.3.158"
    val scalaQuery = "org.scalaquery" %% "scalaquery" % "0.9.4"
    val dependency = "net.databinder.spde" % "processing-core" % "1.0.3__0.1.3"
    val slf4jLog4j12 = "org.slf4j" % "slf4j-log4j12" % "1.6.0"
    val slf4s = "com.weiglewilczek.slf4s" %% "slf4s" % "1.0.6"


    
    //val entryClass = "Crunch"
    //val entryClass = "SurfaceForms"

    //override def mainClass = Some(entryClass)
    
    override def proguardOptions = List(
        proguardKeepAllScala,
        //proguardKeepMain( entryClass ),
        "-dontskipnonpubliclibraryclasses",
        "-dontskipnonpubliclibraryclassmembers",
        "-keep class * { public protected *; }",
        "-dontobfuscate",
        "-dontoptimize",
        "-keeppackagenames"
    )
    override def proguardInJars = super.proguardInJars +++ scalaLibraryPath
    
}


/*class MyProject(info: ProjectInfo) extends DefaultProject(info) with OneJarProject
{
    val lucenecore = "org.apache.lucene" % "lucene-core" % "3.0.3"
    val scalatest = "org.scalatest" % "scalatest" % "1.3"
    override def mainClass = Some("WikiBatch")
}*/
