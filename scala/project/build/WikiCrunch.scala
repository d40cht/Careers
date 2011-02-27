import sbt._

class WikiCrunchProject(info : ProjectInfo) extends DefaultProject(info) with ProguardProject
{
    val entryClass = "Crunch"
    //val entryClass = "SurfaceForms"

    override def mainClass = Some(entryClass)
    
    override def proguardOptions = List(
        proguardKeepAllScala,
        proguardKeepMain( entryClass ),
        "-dontskipnonpubliclibraryclasses",
        "-dontskipnonpubliclibraryclassmembers",
        "-keep class * { public protected *; }",
        "-dontobfuscate",
        "-keeppackagenames"
    )
    override def proguardInJars = super.proguardInJars +++ scalaLibraryPath
    
}

