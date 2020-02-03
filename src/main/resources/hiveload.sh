#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
REQSNOTMET="false"
HADOOP_HOME_PATH=/usr/hdp/current/hadoop-client
HADOOP_CONFIG_SCRIPT=$HADOOP_HOME_PATH/libexec/hadoop-config.sh
HADOOP_CLIENT_LIBS=$HADOOP_HOME_PATH/client
HIVE_HOME=/usr/hdp/current/hive-client
if [ -e $HADOOP_CONFIG_SCRIPT ] ; then
        .  $HADOOP_CONFIG_SCRIPT
else
        echo "Hadoop Client not Installed on Node"
        REQSNOTMET="true"
fi
if [ ! -e $HIVE_HOME ] ; then
        echo "Hive Client not Installed on Node"
        REQSNOTMET="true"
fi
if [[ "$JAVA_HOME" == "" ]] ; then
        echo "JAVA_HOME NOT SET"
        REQSNOTMET="true"
fi
if [ ! -e $JAVA_HOME/bin/java ] ; then
        echo "Java Defined for Hadoop Missing on Node"
        REQSNOTMET="true"
fi
if [[ "$REQSNOTMET" == "false"  ]] ; then
        HIVEJAR=`ls -1 $bin/hivemetarep*.jar`
        JAVA_OPTS="-Djavax.security.auth.useSubjectCredsOnly=false $JAVA_OPTS"
        $JAVA_HOME/bin/java -cp $HIVEJAR:$HIVE_HOME/lib/*:$HADOOP_HOME_PATH/lib/*:$HADOOP_HOME_PATH/*:/usr/hdp/current/hadoop-mapreduce-client/*:/etc/hive/conf/:/etc/hadoop/conf/ org.senia.hive.HiveMetaLoad "$@"
else
        echo "Java Defined for Hadoop Missing on Node"
        exit 1
fi