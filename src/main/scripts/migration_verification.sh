#!/bin/sh

###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
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
# migration_verification.sh  -- This tool is used to provide a summary of migration logs
# This searches for pre-defined strings "Migration Error" and "Migration Summary Count" in log files and outputs those lines.
#

display_usage() {
  cat << EOF
  Usage: $0 [options]

  1. Usage: migration_verification.sh <last_modified> <logs_path>
  2. The <logs_path> should be a directory containing all of the logs. If empty, default path is /opt/app/aai-graphadmin/logs/migration.
  3. The <last_modified> parameter should be an integer for up to how many minutes ago a log file should be parsed.
  4. Example: migration_verification.sh 60 /opt/app/aai-graphadmin/logs/migration
EOF
}

if [ $# -eq 0 ]; then
  display_usage
  exit 1
fi

LOGS_DIRECTORY=${2:-/opt/app/aai-graphadmin/logs/migration/}
MTIME=$1

echo
echo 'Running migration summary:'
print "Logs directory: $LOGS_DIRECTORY"
print "Searching log files modified within last $MTIME minutes: \n"
echo

for i in $(find -L $LOGS_DIRECTORY -mtime -$MTIME -name '*.log' );
do
  echo "Checking Log File: $i"
  grep "Migration Error:" $i
  grep "Migration Summary Count:" $i
  echo
done

echo 'Done'
