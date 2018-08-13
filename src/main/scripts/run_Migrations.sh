#!/bin/sh

###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2017-2018 AT&T Intellectual Property. All rights reserved.
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

# TODO: There is a better way where you can pass in the function
# and then let the common functions check if the function exist and invoke it
# So this all can be templated out
start_date;
check_user;
source_profile;

ARGS="-c ${PROJECT_HOME}/resources/etc/appprops/janusgraph-realtime.properties";

if [ -f "$PROJECT_HOME/resources/application.properties" ]; then
    # Get the application properties file and look for all lines
    # starting with either jms dmaap or niws
    # Turn them into system properties and export JAVA_PRE_OPTS so
    # execute spring jar will get those values
    # This is only needed since dmaap is used by run_migrations
    JAVA_PRE_OPTS="-Xms8g -Xmx8g";
    JMS_PROPS=$(egrep '^jms.bind.address' $PROJECT_HOME/resources/application.properties | cut -d"=" -f2- |  sed 's/^\(.*\)$/-Dactivemq.tcp.url=\1/g' | tr '\n' ' ');
    JAVA_PRE_OPTS="${JAVA_PRE_OPTS} ${JMS_PROPS}";
    export JAVA_PRE_OPTS;
fi;

execute_spring_jar org.onap.aai.migration.MigrationController ${PROJECT_HOME}/resources/migration-logback.xml ${ARGS} "$@"
end_date;
exit 0