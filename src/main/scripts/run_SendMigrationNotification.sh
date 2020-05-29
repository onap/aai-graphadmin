#!/bin/bash

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

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh


start_date;
check_user;
source_profile;

INPUT_PATH=$1

if [ ! -d "$INPUT_PATH" ]; then
    echo "Input directory $INPUT_PATH does not exist!!";
    exit
fi

if [ $(ls ${INPUT_PATH}/* 2> /dev/null | wc -l) -eq 0 ]; then
    echo "Input directory $INPUT_PATH does not contain any migration files!!";
    exit
fi

INPUT_DIR_FOR_JAVA=${INPUT_PATH}/combined
mkdir -p "$INPUT_DIR_FOR_JAVA"
INPUT_FILE_FOR_JAVA=${INPUT_DIR_FOR_JAVA}/sorted_dmaap_files.txt
sort -g -k 1 -t '_' $(find ${INPUT_PATH}/* -maxdepth 0 -type f) | awk -F '_' '{ print $2"_"$3; }' > $INPUT_FILE_FOR_JAVA

shift

ARGS="-c ${PROJECT_HOME}/resources/etc/appprops/janusgraph-realtime.properties --inputFile $INPUT_FILE_FOR_JAVA"

if [ -f "$PROJECT_HOME/resources/application.properties" ]; then
    # Get the application properties file and look for all lines
    # starting with either jms dmaap or niws
    # Turn them into system properties and export JAVA_PRE_OPTS so
    # execute spring jar will get those values
    # This is only needed since dmaap is used by run_migrations
    JAVA_PRE_OPTS=$(egrep '^jms.bind.address' $PROJECT_HOME/resources/application.properties | cut -d"=" -f2- |  sed 's/^\(.*\)$/-Dactivemq.tcp.url=\1/g' | tr '\n' ' ');
    export JAVA_PRE_OPTS;
fi;

execute_spring_jar org.onap.aai.util.SendMigrationNotificationsMain ${PROJECT_HOME}/resources/migration-logback.xml ${ARGS} "$@"
end_date;
exit 0