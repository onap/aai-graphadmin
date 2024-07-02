#!/bin/sh
#
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright © 2017 AT&T Intellectual Property. All rights reserved.
# ================================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============LICENSE_END=========================================================
#
# ECOMP is a trademark and service mark of AT&T Intellectual Property.
#
# updatePropertyTool.sh -- This tool is used to update properties in corrupt vertices
#        in the event that an update or delete occurs to a node simultaneously, resulting
#        in inconsistent data. Updating the aai-uri can reset the index and restore
#        the GET information on the node.
#
# Parameters:
#
#  At least one of following two parameters are required
#  The following values are needed to identify the node(s) to be updated
#  --filename, -f filename of a .txt extension required with a list of vertexIds. Vertex Ids must be separated line after line in text file.
#  --vertexId, -v option that may occur multiple times as entries of a list
#
#  --property, -p (required) value to be updated in the corrupted node
#  --help, -h (optional) used to display help on usage of the function
#
#
#  For example:
#
#  updatePropertyTool.sh --filename myFile.txt --vertexId 123 --property myProperty
#  updatePropertyTool.sh --filename myFile.txt --vertexId 123 --vertexId 456 --property myProperty
#  updatePropertyTool.sh -f myFile.txt --vertexId 123 -v 456 -p myProperty
#  updatePropertyTool.sh -f myFile.txt -p -myProperty
#  updatePropertyTool.sh -v 123 -v 456 -p -myProperty
#  updatePropertyTool.sh -v 123 -p -myProperty
#
COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

start_date;

source_profile;
execute_spring_jar org.onap.aai.dbgen.UpdatePropertyTool ${PROJECT_HOME}/resources/updatePropertyTool-logback.xml "$@"
end_date;

exit 0
