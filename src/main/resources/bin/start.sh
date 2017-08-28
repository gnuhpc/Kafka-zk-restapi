ENV=$1
if [[ $ENV == '' ]]
then
ENV="dev"
fi


unset CDPATH
export basedir=$(cd `dirname $0`/..; pwd)
echo $basedir

chmod 755 ${basedir}/logs

java -Xms256m -Xmx512m -server -Xloggc:${basedir}/logs/gc.log -verbose:gc -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${basedir}/logs -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9888 -Dcom.sun.management.jmxremote.ssl=FALSE -Dcom.sun.management.jmxremote.authenticate=FALSE -Dspring.config.location=${basedir}/config/application.yml -cp $basedir:$basedir/conf:$basedir/libs/* -Dbasedir=${basedir} -jar ${basedir}/lib/kafka-rest-springboot-0.1.0-release.jar
