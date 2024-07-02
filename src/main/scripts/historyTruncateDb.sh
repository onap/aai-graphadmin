#!/bin/ksh

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
# historyTruncateDb.sh  -- This tool is usually run from a cron.
#    It uses the application.property "history.truncate.window.days" to look for
#    and delete nodes and edges that have an end-ts earlier than the truncate window.
#    Or, that can be over-ridden using the command line param, "-truncateWindowDays".
#    That is - they were deleted from the 'real' database before the window.
#    So, if the window is set to 90 days, we will delete all nodes and edges
#    from the history db that were deleted from the real db more than 90 days ago.
#
#    It also uses the property, "history.truncate.mode".  Can be over-ridden using
#    the command line property "-truncateMode"
#       "LOG_ONLY" - look for candidate nodes/edges, but just log them (no deleting)
#       "DELETE_AND_LOG" - like it says...  does the deletes and logs what
#              it deleted (node and edge properties)
#       "SILENT_DELETE"  - not entirely silent, but will pare the logs way back to
#              just recording vertex and edge ids that are deleted.
#
#    Ie.    historyTruncateDb.sh -truncateWindowDays 60 -truncateMode LOG_ONLY
#
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;
source_profile;
execute_spring_jar org.onap.aai.historytruncate.HistoryTruncate ${PROJECT_HOME}/resources/logback.xml "$@"
end_date;

exit 0
