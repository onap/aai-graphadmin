#!/bin/ksh
#
###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
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
# dynamicPayloadGenerator.sh  -- This tool is used to dynamically load payloads from snapshots
#       It is used to load a snapshot into memory and generate payloads for any input nodes
#       
#
# Parameters:
#
#  -d (required) name of the fully qualified Datasnapshot file that you need to load
#  -s (optional) true or false to enable or disable schema, By default it is true for production, 
# 	     you can change to false if the snapshot has duplicates
#  -c (optional) config file to use for loading snapshot into memory.
#  -o (required) output file to store the data files
#  -f (optional) PAYLOAD or DMAAP-MR
#  -n (optional) input file for the script 
#  
#  
#  For example (there are many valid ways to use it):
#  
#  dynamicPayloadGenerator.sh -d '/opt/app/snapshots/snaphot.graphSON' -o '/opt/app/aai-graphadmin/resources/etc/scriptdata/addmanualdata/payload_dir/'
#              
#  or
#  dynamicPayloadGenerator.sh -d '/opt/app/snapshots/snaphot.graphSON' -s false -c '/opt/app/aai-graphadmin/resources/etc/appprops/dynamic.properties'
#					-o '/opt/app/aai-graphadmin/resources/etc/scriptdata/addmanualdata/payload_dir/' -f PAYLOAD -n '/opt/app/aai-graphadmin/resources/etc/scriptdata/nodes.json'
# 


echo
echo `date` "   Starting $0"

display_usage() {
        cat <<EOF
        Usage: $0 [options]

        1. Usage: dynamicPayloadGenerator -d <graphsonPath> -o  <output-path>
        2. This script has  2 arguments that are required.
           a.	-d (required) Name of the fully qualified Datasnapshot file that you need to load
           b.	-o (required) output file to store the data files
        3. Optional Parameters:
		   a.   -s (optional) true or false to enable or disable schema, By default it is true for production, 
		   b.	-c (optional) config file to use for loading snapshot into memory. By default it is set to /opt/app/aai-graphadmin/resources/etc/appprops/dynamic.properties
		   c.	-f (optional) PAYLOAD or DMAAP-MR
		   d.	-n (optional) input file specifying the nodes and relationships to export. Default: /opt/app/aai-graphadmin/scriptdata/tenant_isolation/nodes.json
		   e.   -m (optional) true or false to read multiple snapshots or not, by default is false
		   f.   -i (optional) the file containing the input filters based on node property and regex/value. By default, it is: /opt/app/aai-graphadmin/scriptdata/tenant_isolation/inputFilters.json
		4. For example (there are many valid ways to use it):
			dynamicPayloadGenerator.sh -d '/opt/app/snapshots/snaphot.graphSON' -o '/opt/app/aai-graphadmin/resources/etc/scriptdata/addmanualdata/tenant_isolation/'
				
			dynamicPayloadGenerator.sh -d '/opt/app/snapshots/snaphot.graphSON' -s false -c '/opt/app/aai-graphadmin/resources/etc/appprops/dynamic.properties'
					-o '/opt/app/aai-graphadmin/resources/etc/scriptdata/addmanualdata/tenant_isolation/' -f PAYLOAD -n '/opt/app/aai-graphadmin/resources/etc/scriptdata/tenant_isolation/nodes.json'
					-m false -i '/opt/app/aai-graphadmin/resources/etc/scriptdata/tenant_isolation/inputFilters.json'
   
EOF
}
if [ $# -eq 0 ]; then
        display_usage
        exit 1
fi

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;
check_user;
source_profile;
export JVM_OPTS="-Xmx9000m -Xms9000m"

while getopts ":f:s:d:n:c:i:m:o:p:" opt; do
      case ${opt} in
        f )
          PAYLOAD=$OPTARG
          echo ${opt}
          ;;
        s )
          VALIDATE_SCHEMA=$OPTARG
          echo ${opt}
          ;;
        d )
          INPUT_DATASNAPSHOT_FILE=$OPTARG
          echo ${opt}
          ;;
        n )
          NODE_CONFIG_FILE=$OPTARG
          echo ${opt}
          ;;
        c )
          DYNAMIC_CONFIG_FILE=$OPTARG
          echo ${opt}
          ;;
        i )
          INPUT_FILTER_FILE=$OPTARG
          echo ${opt}
          ;;
        m )
          MULTIPLE_SNAPSHOTS=$OPTARG
          echo ${opt}
          ;;
        p )
          PARTIAL=$OPTARG
          echo ${opt}
          ;;
        o )
          OUTPUT_DIR=$OPTARG
          echo ${opt}
          ;;
        \? )
          echo "Invalid Option: -$OPTARG" 1>&2
          ;;
        : )
          echo "Invalid Option: -$OPTARG requires an argument" 1>&2
          ;;
      esac
    done
    shift $((OPTIND -1))

echo 'Done'

set -A nodes pserver cloud-region availability-zone tenant zone complex

#Create empty partial file
 > $INPUT_DATASNAPSHOT_FILE".partial"

for nodeType in ${nodes[@]}
	do
         grep "aai-node-type.*\"value\":\"$nodeType\"" $INPUT_DATASNAPSHOT_FILE >>$INPUT_DATASNAPSHOT_FILE'.partial'
    done


execute_spring_jar org.onap.aai.dbgen.DynamicPayloadGenerator ${PROJECT_HOME}/resources/dynamicPayloadGenerator-logback.xml -s ${VALIDATE_SCHEMA} \
		-f ${PAYLOAD} -o ${OUTPUT_DIR} -c ${DYNAMIC_CONFIG_FILE} -i ${INPUT_FILTER_FILE} -m ${MULTIPLE_SNAPSHOTS} \
		-d ${INPUT_DATASNAPSHOT_FILE} -n ${NODE_CONFIG_FILE} ;
		
end_date;
exit 0
