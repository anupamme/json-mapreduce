export LIBJARS=hdfs-new/ejml-0.23.jar,hdfs-new/stanford-corenlp-3.4.1-models.jar,hdfs-new/stanford-corenlp-3.4.1-sources.jar,hdfs-new/stanford-corenlp-3.4.1.jar
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export HADOOP_CLIENT_OPTS="-Xmx4g"
