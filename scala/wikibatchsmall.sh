
export LD_LIBRARY_PATH=/home/alexw/AW/optimal/scala/lib
/home/hadoop/hadoop/bin/hadoop jar ./target/scala_2.8.1/wikicrunch_2.8.1-1.0.min.jar org.seacourt.wikibatch.WikiBatch enwikismall.seq wikiBatchSmall 10
#java -cp target/scala_2.8.1/wikicrunch_2.8.1-1.0.min.jar PhraseMap wikiBatchSmall/ dbout.sqlite
