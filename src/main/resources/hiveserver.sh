#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
REQSNOTMET="false"
HIVEMETAEXPSRV_LOGDIR="/var/log/hivemetaexport"
HIVEMETAEXPSRV_PIDIR="/var/run"
HIVECONFDIR="/etc/hive/conf/"
HADOOPCONFDIR="/etc/hadoop/conf/"
HIVE_HOME=/usr/hdp/current/hive-client
HADOOP_CONFIG_SCRIPT=/etc/hadoop/conf/hadoop-env.sh
if [ -e $HADOOP_CONFIG_SCRIPT ] ; then
        .  $HADOOP_CONFIG_SCRIPT
        HADOOP_CLIENT_LIBS=$HADOOP_HOME/client
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
        ulimit -n 60000
        ulimit -u 60000
        ulimit -s 2048

        /usr/lib/bigtop-utils/jsvc -debug -pidfile $HIVEMETAEXPSRV_PIDIR/hivemetaexportsrv.pid -jvm server -Xms2g -Xmx2g -user hdfs -outfile $HIVEMETAEXPSRV_LOGDIR/hms.out -errfile \&1 -cp $bin/lib/*:$HIVE_HOME/lib/*:$HADOOP_CLIENT_LIBS/*:$HADOOP_CLASSPATH:$HIVECONFDIR:$HADOOPCONFDIR -Dlog4j.configuration=file://$bin/conf/log4j.properties org.senia.hive.HiveMetaServer --propsfile $bin/conf/config.props
else
        echo "Java Defined for Hadoop Missing on Node"
        exit 1
fi