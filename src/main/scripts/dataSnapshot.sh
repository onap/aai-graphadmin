#!/bin/ksh
#
# This script invokes the dataSnapshot java class passing an option to tell it to take
# a snapshot of the database and store it as a single-line XML file.
#
#
#
#
#
COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

processStat=$(ps -ef | grep '[D]ataSnapshot');
if [ "$processStat" != "" ]
	then
	echo "Found dataSnapshot is already running: " $processStat
	exit 1
fi

# TODO: There is a better way where you can pass in the function
# and then let the common functions check if the function exist and invoke it
# So this all can be templated out
start_date;
source_profile;

# Only sourcing the file aai-graphadmin-tools-vars for dataSnapshot
# Do not source this for dataRestore or otherwise
# both taking a snapshot and restoring from a snapshot
# will use the same amount of memory but restoring from snapshot
# will use a lot more memory than taking a snapshot
if [ -f "$PROJECT_HOME/resources/aai-graphadmin-tools-vars.sh" ]; then
    source $PROJECT_HOME/resources/aai-graphadmin-tools-vars.sh
fi;
execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot $PROJECT_HOME/resources/logback.xml "$@"
end_date;
exit 0
