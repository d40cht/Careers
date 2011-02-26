import sbt._

class WikiCrunchProject(info : ProjectInfo) extends DefaultProject(info) with ProguardProject
{
    override def mainClass = Some("Crunch")
    
    override def proguardOptions = List(
        //proguardKeepMain( "SurfaceForms" )
        proguardKeepAllScala,
        proguardKeepMain( "Crunch" ),
        "-dontskipnonpubliclibraryclasses",
        "-dontskipnonpubliclibraryclassmembers",
        "-keep class * { public protected *; }",
        "-dontobfuscate",
        "-keeppackagenames"
    )
    override def proguardInJars = super.proguardInJars +++ scalaLibraryPath
    
}

