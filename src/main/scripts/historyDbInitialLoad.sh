#!/bin/ksh
#
# This script uses "history" versions of dataSnapshot and SchemaGenerator (via genTester)
#  java classes to do the INITIAL load of a history database based on regular dataSnapShot
#  files (assumed to be 'clean') from an existing non-history database.
# Steps:
#   1) Make sure the db is empty: clear out any existing data and schema.
#   2) rebuild the schema (using the SchemaGenerator4Hist)
#   3) reload data from the passed-in datafiles (which must found in the dataSnapShots directory and
#      contain a json view of the db data).
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;

if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters"
    echo "usage: $0 base_snapshot_filename"
    exit 1
fi

source_profile;
export JAVA_PRE_OPTS=${JAVA_PRE_OPTS:--Xms6g -Xmx8g};

#### Step 1) Make sure the target database is cleared
echo "---- First Step: clear the db ----"
execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot4HistInit ${PROJECT_HOME}/resources/logback.xml "-c" "CLEAR_ENTIRE_DATABASE" "-f" "$1"
if [ "$?" -ne "0" ]; then
    echo "Problem clearing out database."
    exit 1
fi

#### Step 2) rebuild the db-schema
echo "---- Second Step: rebuild the db schema ----"
execute_spring_jar org.onap.aai.schema.GenTester4Hist ${PROJECT_HOME}/resources/logback.xml "GEN_DB_WITH_NO_DEFAULT_CR"
if [ "$?" -ne "0" ]; then
    echo "Problem rebuilding the schema (SchemaGenerator4Hist)."
    exit 1
fi

#### Step 3) load the data from snapshot files
echo "---- Third Step: Load data from snapshot files ----"
execute_spring_jar org.onap.aai.datasnapshot.DataSnapshot4HistInit ${PROJECT_HOME}/resources/logback.xml "-c" "MULTITHREAD_RELOAD" "-f" "$1"
if [ "$?" -ne "0" ]; then
    echo "Problem reloading data into the database."
    end_date;
    exit 1
fi

end_date;
exit 0
