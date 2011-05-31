export LD_LIBRARY_PATH=/home/alexw/AW/optimal/scala/lib
java -Xmx4000m -cp "./project/boot/scala-2.8.1/lib/scala-compiler.jar:./project/boot/scala-2.8.1/lib/scala-library.jar:/usr/share/java/jline.jar:." scala.tools.nsc.MainGenericRunner -usejavacp
