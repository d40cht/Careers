import sbt._

class WikiCrunchProject(info : ProjectInfo) extends DefaultProject(info) with ProguardProject
{
    override def proguardOptions = List(
        proguardKeepMain( "SurfaceForms" )
    )
    
    //override def proguardInJars = super.proguardInJars +++ scalaLibraryPath
}
