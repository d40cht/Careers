export HADOOP_HEAPSIZE=6000
export LD_LIBRARY_PATH=/home/alexw/AW/optimal/scala/lib
#java -cp target/scala_2.8.1/wikicrunch_2.8.1-1.0.min.jar Crunch ../data/enwiki-latest-pages-articles.xml.bz2 enwikifull.seq
#/home/hadoop/hadoop/bin/hadoop jar ./target/scala_2.8.1/wikicrunch_2.8.1-1.0.min.jar org.seacourt.wikibatch.WikiBatch -Dmapred.map.child.java.opts=-Xmx900M enwikifull.seq wikiBatch 50
java -Xmx6000M -cp target/scala_2.8.1/wikicrunch_2.8.1-1.0.min.jar PhraseMap wikiBatch/ DisambigData/dbout.sqlite
