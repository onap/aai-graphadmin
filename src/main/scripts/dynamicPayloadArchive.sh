#!/bin/sh
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
# The script is called to tar and gzip the files under /opt/app/aai-graphadmin/data/scriptdata/addmanualdata/tenant_isolation/payload
# which contains the payload files created by the dynamicPayloadGenerator.sh tool.
# /opt/app/aai-graphadmin/data/scriptdata/addmanualdata/tenant_isolation is mounted to the docker container
#

COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh


PROJECT_HOME=/opt/app/aai-graphadmin

PROGNAME=$(basename $0)

TS=$(date "+%Y_%m_%d_%H_%M_%S")

CHECK_USER="aaiadmin"
userid=$( id | cut -f2 -d"(" | cut -f1 -d")" )
if [ "${userid}" != $CHECK_USER ]; then
    echo "You must be  $CHECK_USER to run $0. The id used $userid."
    exit 1
fi
PAYLOAD_DIRECTORY=${PROJECT_HOME}/resources/etc/scriptdata/addmanualdata/tenant_isolation/payload
ARCHIVE_DIRECTORY=${PROJECT_HOME}/resources/etc/scriptdata/addmanualdata/tenant_isolation/archive
if [ ! -d ${PAYLOAD_DIRECTORY} ]
then
	echo " ${PAYLOAD_DIRECTORY} doesn't exist"
	exit 1
fi
if [ ! -d ${ARCHIVE_DIRECTORY} ]
then
	mkdir -p ${ARCHIVE_DIRECTORY}
	chown aaiadmin:aaiadmin ${ARCHIVE_DIRECTORY}
	chmod u+w ${ARCHIVE_DIRECTORY}
fi
cd ${PAYLOAD_DIRECTORY}
tar c * -f ${ARCHIVE_DIRECTORY}/dynamicPayloadArchive_${TS}.tar --exclude=payload
if [ $? -ne 0 ]
then
	echo " Unable to tar ${PAYLOAD_DIRECTORY}"
	exit 1
fi

cd ${ARCHIVE_DIRECTORY}
gzip ${ARCHIVE_DIRECTORY}/dynamicPayloadArchive_${TS}.tar

if [ $? -ne 0 ]
then
	echo " Unable to gzip ${ARCHIVE_DIRECTORY}/dynamicPayloadArchive_${TS}.tar"
	exit 1
fi
echo "Completed successfully: ${ARCHIVE_DIRECTORY}/dynamicPayloadArchive_${TS}.tar"
exit 0
