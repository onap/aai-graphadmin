#!/bin/sh
#
# This script uses the dataSnapshot and SchemaGenerator (via GenTester) java classes to restore
# data to a database by doing three things:
#   1) clear out whatever data and schema are currently in the db
#   2) rebuild the schema (using the SchemaGenerator)
#   3) reload data from the passed-in datafile (which must found in the dataSnapShots directory and
#      contain an xml view of the db data).
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;

if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters"
    echo "usage: $0 previous_snapshot_filename"
    exit 1
fi

source_profile;
export JAVA_PRE_OPTS=${JAVA_PRE_OPTS:--Xms6g -Xmx8g};

#### Step 1) clear out the database
execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot ${PROJECT_HOME}/resources/logback.xml "-c" "CLEAR_ENTIRE_DATABASE" "-f" "$1"
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

#### Step 3) reload the data from a snapshot file

execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot ${PROJECT_HOME}/resources/logback.xml "-c" "RELOAD_DATA" "-f" "$1"
if [ "$?" -ne "0" ]; then
    echo "Problem reloading data into the database."
    end_date;
    exit 1
fi

end_date;
exit 0
