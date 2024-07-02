#!/bin/sh

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
#
#
# dupeTool.sh  -- This tool is used to look at or fix duplicate nodes for one nodeType
#       at a time and can be used to limit what it's looking at to just nodes created
#       within a recent time window.
#       It is made to deal with situations (like we have in 1610/1702) where one type
#       of node keeps needing to have duplicates cleaned up (tenant nodes).
#       It is needed because DataGrooming cannot be run often and cannot be focused just
#       on duplicates or just on one nodeType.
#
# Parameters:
#
#  -userId (required) must be followed by a userid
#  -nodeType (required) must be followed by a valid nodeType
#  -timeWindowMinutes (optional) by default we would look at all nodes of the
#        given nodeType, but if a window is given, then we will only look at
#        nodes created that many (or fewer) minutes ago.
#  -autoFix (optional) use this if you want duplicates fixed automatically (if we
#           can figure out which to delete)
#  -maxFix (optional) like with dataGrooming lets you override the default maximum
#           number of dupes that can be processed at one time
#  -skipHostCheck (optional) By default, the dupe tool will check to see that it is running
#           on the host that is the first one in the list found in:
#               aaiconfig.properties  aai.primary.filetransfer.serverlist
#           This is so that when run from the cron, it only runs on one machine.
#           This option lets you turn that checking off.
#  -sleepMinutes (optional) like with DataGrooming, you can override the
#           sleep time done when doing autoFix between first and second checks of the data.
#  -params4Collect (optional) followed by a string to tell what properties/values to use
#  		to limit the nodes being looked at.  Must be in the format
#  		of “propertName|propValue” use commas to separate if there
#  		are more than one name/value being passed.
#  -specialTenantRule (optional) turns on logic which will use extra logic to figure
#       out which tenant node can be deleted in a common scenario.
#
#
#  For example (there are many valid ways to use it):
#
#  dupeTool.sh -userId am8383 -nodeType tenant -timeWindowMinutes 60 -autoFix
#  or
#  dupeTool.sh -userId am8383 -nodeType tenant -specialTenantRule -autoFix -maxFix 100
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;
source_profile;

export JAVA_PRE_OPTS=${JAVA_PRE_OPTS:--Xms6g -Xmx6g};

execute_spring_jar org.onap.aai.dbgen.DupeTool ${PROJECT_HOME}/resources/dupeTool-logback.xml "$@"
end_date;
exit 0
