#!/bin/ksh
#
# The script invokes UniqueProperty java class to see if the passed property is unique in the db and if
#    not, to display where duplicate values are found.
#
# For example:    uniquePropertyCheck.sh subscriber-name
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )	
. ${COMMON_ENV_PATH}/common_functions.sh
start_date;
check_user;
source_profile;

#execute_spring_jar org.onap.aai.util.UniquePropertyCheck ${PROJECT_HOME}/resources/uniquePropertyCheck-logback.xml "$@"
execute_spring_jar org.onap.aai.util.UniquePropertyCheck ${PROJECT_HOME}/resources/uniquePropertyCheck-logback.xml "$@"
ret_code=$?
if [ $ret_code != 0 ]; then
  end_date;
  exit $ret_code
fi

end_date;
exit 0