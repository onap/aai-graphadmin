#!/bin/ksh
#
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright © 2017 AT&T Intellectual Property. All rights reserved.
# ================================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============LICENSE_END=========================================================
#
# ECOMP is a trademark and service mark of AT&T Intellectual Property.
# The script invokes GenTester java class to create the DB schema
#
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )	
. ${COMMON_ENV_PATH}/common_functions.sh
start_date;
check_user;
source_profile;
if [ -z "$1" ]; then
    execute_spring_jar org.onap.aai.schema.GenTester4Hist ${PROJECT_HOME}/resources/logback.xml
else
    execute_spring_jar org.onap.aai.schema.GenTester4Hist ${PROJECT_HOME}/resources/logback.xml "$1"
fi;
end_date;
exit 0