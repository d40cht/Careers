export LD_LIBRARY_PATH=~/AW/optimal/scala/lib
java -Xmx6000M -D"log4j.rootCategory=debug" -cp target/scala_2.8.1/wikicrunch_2.8.1-1.0.min.jar WebCVProcess $1 $2 $3
