###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
# ================================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============LICENSE_END=========================================================
###

APP_HOME=$(pwd);
RESOURCES_HOME=${APP_HOME}/resources/;

export SERVER_PORT=${SERVER_PORT:-8449};

scriptName=$1;

if [ ! -z $scriptName ]; then

    if [ -f ${APP_HOME}/bin/${scriptName} ]; then
        shift 1;
        ${APP_HOME}/bin/${scriptName} "$@" || {
            echo "Failed to run the ${scriptName}";
            exit 1;
        }
    else
        echo "Unable to find the script ${scriptName} in ${APP_HOME}/bin";
        exit 1;
    fi;

    exit 0;
fi;

if [ -f ${APP_HOME}/resources/aai-graphadmin-swm-vars.sh ]; then
    source ${APP_HOME}/resources/aai-graphadmin-swm-vars.sh;
fi;

MAX_METASPACE_SIZE=${MAX_METASPACE_SIZE:-512m};

JAVA_CMD="exec java";

JVM_OPTS="${JVM_OPTS} -XX:HeapDumpPath=/opt/app/aai-graphadmin/logs/ajsc-jetty/heap-dump";
JVM_OPTS="${JVM_OPTS} -XX:MaxRAMPercentage=${MAX_RAM_PERCENTAGE:-70}";
JVM_OPTS="${JVM_OPTS} -XX:MaxMetaspaceSize=${MAX_METASPACE_SIZE}";

JVM_OPTS="${JVM_OPTS} -server";
JVM_OPTS="${JVM_OPTS} -XX:NewSize=512m";
JVM_OPTS="${JVM_OPTS} -XX:MaxNewSize=512m";
JVM_OPTS="${JVM_OPTS} -XX:SurvivorRatio=8";
JVM_OPTS="${JVM_OPTS} -XX:+DisableExplicitGC";
JVM_OPTS="${JVM_OPTS} -XX:+UseG1GC";
JVM_OPTS="${JVM_OPTS} -XX:ParallelGCThreads=4";
JVM_OPTS="${JVM_OPTS} -XX:LargePageSizeInBytes=128m";
JVM_OPTS="${JVM_OPTS} -Dsun.net.inetaddr.ttl=180";
JVM_OPTS="${JVM_OPTS} -XX:+HeapDumpOnOutOfMemoryError";
JVM_OPTS="${JVM_OPTS} ${POST_JVM_ARGS}";
JAVA_OPTS="${PRE_JAVA_OPTS} -DAJSC_HOME=$APP_HOME";
if [ -f ${INTROSCOPE_LIB}/Agent.jar ] && [ -f ${INTROSCOPE_AGENTPROFILE} ]; then
        JAVA_OPTS="${JAVA_OPTS} -javaagent:${INTROSCOPE_LIB}/Agent.jar -noverify -Dcom.wily.introscope.agentProfile=${INTROSCOPE_AGENTPROFILE} -Dintroscope.agent.agentName=graphadmin"
fi
JAVA_OPTS="${JAVA_OPTS} -Dserver.port=${SERVER_PORT}";
JAVA_OPTS="${JAVA_OPTS} -DBUNDLECONFIG_DIR=./resources";
JAVA_OPTS="${JAVA_OPTS} -Dserver.local.startpath=${RESOURCES_HOME}";
JAVA_OPTS="${JAVA_OPTS} -DAAI_BUILD_VERSION=${AAI_BUILD_VERSION}";
JAVA_OPTS="${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom";
JAVA_OPTS="${JAVA_OPTS} -Dlogback.configurationFile=./resources/logback.xml";
JAVA_OPTS="${JAVA_OPTS} -Dloader.path=$APP_HOME/resources";
JAVA_OPTS="${JAVA_OPTS} -Dgroovy.use.classvalue=true";
JAVA_OPTS="${JAVA_OPTS} ${POST_JAVA_OPTS}";

JAVA_MAIN_JAR=$(ls lib/aai-graphadmin*.jar);

${JAVA_CMD} ${JVM_OPTS} ${JAVA_OPTS} -jar ${JAVA_MAIN_JAR};
