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
check_user;
source_profile;
execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot $PROJECT_HOME/resources/logback.xml "$@"
end_date;
exit 0
