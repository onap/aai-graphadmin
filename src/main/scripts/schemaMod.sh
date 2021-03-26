#!/bin/ksh
#
# This script is used to correct mistakes made in the database schema.  
# It currently just allows you to change either the dataType and/or indexType on properties used by nodes.    
#
# NOTE - JanusGraph is not particularly elegant in about making changes to the schema.  
#       So it is really worthwhile to get indexes and dataTypes correct the first time around.
# Note also - This script just makes changes to the schema that is currently live.
#    If you were to create a new schema in a brandy-new environment, it would look like
#    whatever our OXM files told it to look like.   So, part of making a 
#    change to the live db schema should Always first be to make the change in the appropriate
#    OXM schema file so that future environments will have the change.  This script is 
#    just to change existing instances of the schema since schemaGenerator does not 
#    update things - it just does the initial creation.
#
# To use this script, there are 5 required parameters, and one optional:
#      propertyName    -- the name of the property that you need to change either the index or dataType on
#      targetDataType  -- whether it's changing or not, you need to give it:  String, Integer, Boolean or Long
#      targetIndexInfo -- whether it's changing or not, you need to give it: index, noIndex or uniqueIndex
#      preserveDataFlag -- true or false.     The only reason I can think of why you'd ever want to
#                   set this to false would be maybe if you were changing to an incompatible dataType so didn't 
#                   want it to try to use the old data (and fail).  But 99% of the time this will just be 'true'.
#      consistencyLock -- true or false. Whether to enable consistency lock on the property or not
#
#      commitBlockSize -- OPTIONAL -- how many updates to commit at once.  
#                  Default will be used if no value is passed.
#
# Ie.    schemaMod flavor-id String index true true
#   or,  schemaMod flavor-id String noIndex true true 50000
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )	
. ${COMMON_ENV_PATH}/common_functions.sh
start_date;
check_user;

if [ "$#" -ne 5 ] && [ "$#" -ne 6 ]; then
    echo "Illegal number of parameters"
    echo "usage: $0 propertyName targetDataType targetIndexInfo preserveDataFlag consistencyLock [blockSize]"
    exit 1
fi

source_profile;
execute_spring_jar org.onap.aai.dbgen.schemamod.SchemaMod ${PROJECT_HOME}/resources/schemaMod-logback.xml "$@"
if [ "$?" -ne "0" ]; then
    echo "Problem executing schemaMod "
    end_date;
    exit 1
fi
 
end_date;
exit 0
