#!/bin/ksh
#
# This script does just the first two steps of our normal dataRestoreFromSnapshot script.
# This should only be needed if we are trouble-shooting and need to run step 3 (the 
#    actual call to dataSnapshot) separately with different input params.
#
# This script does these two steps:
#   1) clear out whatever data and schema are currently in the db 
#   2) rebuild the schema (using the SchemaGenerator)
# 

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;
check_user;

if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters"
    echo "usage: $0 previous_snapshot_filename"
    exit 1
fi

source_profile;
export JAVA_PRE_OPTS=${JAVA_PRE_OPTS:--Xms6g -Xmx8g};

#### Step 1) clear out the database
execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot ${PROJECT_HOME}/resources/logback.xml "CLEAR_ENTIRE_DATABASE" "$1" "$2"
if [ "$?" -ne "0" ]; then
    echo "Problem clearing out database."
    exit 1
fi
 
#### Step 2) rebuild the db-schema
execute_spring_jar org.onap.aai.schema.GenTester ${PROJECT_HOME}/resources/logback.xml "GEN_DB_WITH_NO_DEFAULT_CR"
if [ "$?" -ne "0" ]; then
    echo "Problem rebuilding the schema (SchemaGenerator)."
    exit 1
fi


 
end_date;
exit 0
