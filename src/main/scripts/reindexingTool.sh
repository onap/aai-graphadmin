#!/bin/sh

###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2025 Deutsche Telekom. All rights reserved.
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
# reindexingTool.sh  -- This tool is used to do reindexing of either all or some indexes based on the parameters passed.
#       It runs in 2 modes
#        1. Partial reindexing - Provide specific index name or names(comma separated complete list of indexes in double quotes). Ex-
#         JAVA_PRE_OPTS='-Xms3G -Xmx12G'  ./scripts/reindexingTool.sh -indexName "service-instance-id,tenant-id"
#         In this mode, passed indexes will only be reindexed
#        2. Full reindexing - Run a full reindex on all indexes, use only when cluster is idle. Ex-
#         JAVA_PRE_OPTS='-Xms3G -Xmx12G'  ./scripts/reindexingTool.sh -fullReindex
#
# Parameters for Partial reindexing:
#
#  -indexName (required) must be followed by a index name that is to be reindexed
#
# Parameters for Full reindexing:
#  -fullReindex (optional) in case you want to run reindexing on all indexes in database use this option. Use this
#   option only when no activity is going on in the cluster as it may impact the outcome of APIs(index-data mismatch)
#
#  For example (there are many valid ways to use it):
#
#  JAVA_PRE_OPTS='-Xms3G -Xmx12G'  ./scripts/reindexingTool.sh -indexName service-instance-id
#  or
#  JAVA_PRE_OPTS='-Xms3G -Xmx12G'  ./scripts/reindexingTool.sh -fullReindex
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;
source_profile;

export JAVA_PRE_OPTS=${JAVA_PRE_OPTS:--Xms6g -Xmx6g};

execute_spring_jar org.onap.aai.dbgen.ReindexingTool ${PROJECT_HOME}/resources/reindexingTool-logback.xml "$@"
end_date;
exit 0
