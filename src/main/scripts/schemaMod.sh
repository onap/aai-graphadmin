#!/bin/ksh
#
# This script is used to correct mistakes made in the database schema.  
# It currently just allows you to change either the dataType and/or indexType on properties used by nodes.    
#
# NOTE - Titan is not elegant in 0.5.3 about making changes to the schema.  Bad properties never
#       actually leave the database, they just get renamed and stop getting used.  So it is 
#       really worthwhile to get indexes and dataTypes correct the first time around.
# Note also - This script just makes changes to the schema that is currently live.
#    If you were to create a new schema in a brandy-new environment, it would look like
#    whatever ex5.json (as of June 2015) told it to look like.   So, part of making a 
#    change to the db schema should Always first be to make the change in ex5.json so that
#    future environments will have the change.  This script is just to change existing
#    instances of the schema since schemaGenerator (as of June 2015) does not update things - it 
#    just does the initial creation.
#
# Boy, this is getting to be a big comment section...
#
# To use this script, you need to pass four parameters:
#      propertyName    -- the name of the property that you need to change either the index or dataType on
#      targetDataType  -- whether it's changing or not, you need to give it:  String, Integer, Boolean or Long
#      targetIndexInfo -- whether it's changing or not, you need to give it: index, noIndex or uniqueIndex
#      preserveDataFlag -- true or false.     The only reason I can think of why you'd ever want to
#                   set this to false would be maybe if you were changing to an incompatible dataType so didn't 
#                   want it to try to use the old data (and fail).  But 99% of the time this will just be 'true'.
#
# Ie.    schemaMod flavor-id String index true
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )	
. ${COMMON_ENV_PATH}/common_functions.sh
start_date;
check_user;

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "usage: $0 propertyName targetDataType targetIndexInfo preserveDataFlag"
    exit 1
fi

source_profile;
execute_spring_jar org.onap.aai.dbgen.schemamod.SchemaMod ${PROJECT_HOME}/resources/schemaMod-logback.xml "$1" "$2" "$3" "$4"
if [ "$?" -ne "0" ]; then
    echo "Problem executing schemaMod "
    end_date;
    exit 1
fi
 
end_date;
exit 0
