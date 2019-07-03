#!/usr/bin/env bash
ENV=$1
if [[ $ENV == '' ]]
then
ENV="dev"
fi


unset CDPATH
export basedir=$(cd `dirname $0`/..; pwd)
configdir=${basedir}/config
libdir=${basedir}/lib
logdir=${basedir}/logs

chmod 755 ${logdir}

java -Xms512m -Xmx512m -server -Xloggc:${logdir}/gc.log -verbose:gc -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${logdir} -cp ${basedir}:${configdir}/*:${libdir}/* -Dbasedir=${basedir} -Dlogging.config=${configdir}/log4j2.properties -Dspring.config.location=${configdir}/ -jar ${libdir}/kafka*-rest-springboot*.jar
