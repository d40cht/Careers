import sbt._
//import com.github.retronym.OneJarProject

class WikiCrunchProject(info : ProjectInfo) extends DefaultProject(info) with ProguardProject
{
    val lucenecore = "org.apache.lucene" % "lucene-core" % "3.0.3"
    val scalatest = "org.scalatest" % "scalatest" % "1.3"
    val berkeleydb = "berkeleydb" % "je" % "3.2.76"
    val automaticresourcemanagement = "com.github.jsuereth.scala-arm" % "scala-arm_2.8.0" % "0.2"
    
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
