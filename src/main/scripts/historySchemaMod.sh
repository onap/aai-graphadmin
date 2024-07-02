#!/bin/ksh
#
# This script is used to correct mistakes made in the database schema.
# It currently just allows you to change either the dataType and/or indexType on properties used by nodes.
#
# NOTE - This script is for the History db.  That is different than the
#    regular schemaMod in these two ways: 1) it will never create a unique index.
#    Indexes can be created, but they will never be defined as unique.
#    2) the last parameter (preserveDataFlag) is ignored since for history, we do
#    not want to 'migrate' old data.  Old data should not disappear or change.
#
#
# To use this script, you need to pass four parameters:
#      propertyName    -- the name of the property that you need to change either the index or dataType on
#      targetDataType  -- whether it's changing or not, you need to give it:  String, Integer, Boolean or Long
#      targetIndexInfo -- whether it's changing or not, you need to give it: index, noIndex or uniqueIndex
#      preserveDataFlag -- true or false.     The only reason I can think of why you'd ever want to
#                   set this to false would be maybe if you were changing to an incompatible dataType so didn't
#                   want it to try to use the old data (and fail).  But 99% of the time this will just be 'true'.
#
# Ie.    historySchemaMod flavor-id String index true
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh
start_date;

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "usage: $0 propertyName targetDataType targetIndexInfo preserveDataFlag"
    exit 1
fi

source_profile;
execute_spring_jar org.onap.aai.dbgen.schemamod.SchemaMod4Hist ${PROJECT_HOME}/resources/schemaMod-logback.xml "$1" "$2" "$3" "$4"
if [ "$?" -ne "0" ]; then
    echo "Problem executing schemaMod "
    end_date;
    exit 1
fi

end_date;
exit 0
