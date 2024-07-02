#!/bin/ksh

###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
# ================================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============LICENSE_END=========================================================
###

#

display_usage() {
        cat <<EOF
        Usage: $0 [options]

        1. Usage: getDslResult.sh <base-path or optional host url> <optional input-json-filepath> <optional -XFROMAPPID> <optional -XTRANSID>
        2. This script requires one argument, a base-path
        3. Example for basepath: aai/{version}\
        4. Adding the optional input-json-payload replaces the default dsl payload with the contents of the input-file
        5. The query result is returned in the file resources/migration-input-files/dslResults.json
EOF
}
if [ $# -eq 0 ]; then
        display_usage
        exit 1
fi

RESOURCE="dsl?format=resource_and_url&nodesOnly=true"

BASEPATH=$1
if [ -z $BASEPATH ]; then
        echo "base-path parameter is missing"
        echo "usage: $0 <base-path>"
        exit 1
fi


PROJECT_HOME=/opt/app/aai-graphadmin
RESULTDIR=$PROJECT_HOME/logs/data/migration-input-files

if [ ! -d ${RESULTDIR} ]; then
        mkdir -p ${RESULTDIR}
        chown aaiadmin:aaiadmin ${RESULTDIR}
        chmod u+w ${RESULTDIR}
fi

RESULTPATH=$RESULTDIR/dslResults.json

JSONFILE=$2
TEMPFILE=/tmp/dslResult-temp.json
if [ -z $JSONFILE ]; then
        JSONFILE=$TEMPFILE
        echo "{ \"dsl\":\"l3-network('network-type','Tenant Network')>vlan-tag>vlan-range*\" }" > $JSONFILE
fi

echo `date` "   Starting $0 for resource $RESOURCE"


XFROMAPPID="AAI-TOOLS"
XTRANSID=`uuidgen`

if [ ! -z "$3" ]; then
   XFROMAPPID=$3
fi

if [ ! -z "$4" ]; then
   XTRANSID=$4
fi

userid=$( id | cut -f2 -d"(" | cut -f1 -d")" )
if [ "${userid}" != "aaiadmin" ]; then
    echo "You must be aaiadmin to run $0. The id used $userid."
    exit 1
fi

prop_file=$PROJECT_HOME/resources/etc/appprops/aaiconfig.properties
log_dir=$PROJECT_HOME/logs/misc
today=$(date +\%Y-\%m-\%d)

RETURNRESPONSE=true

MISSING_PROP=false
RESTURLPROP=`grep ^aai.server.url.base= $prop_file |cut -d'=' -f2 |tr -d "\015"`
if [ -z $RESTURLPROP ]; then
        echo "Property [aai.server.url.base] not found in file $prop_file"
        MISSING_PROP=true
else
	RESTURL=`echo $RESTURLPROP | sed -e "s#aai/#$BASEPATH/#"`
fi

if [ ! -z "$1" ]; then
	if [[ $1 == *"https"* ]]; then
   		RESTURL=$1
   	fi
fi

USEBASICAUTH=false
BASICENABLE=`grep ^aai.tools.enableBasicAuth $prop_file |cut -d'=' -f2 |tr -d "\015"`
if [ -z $BASICENABLE ]; then
        USEBASICAUTH=false
else
        USEBASICAUTH=true
        USER=`grep ^aai.tools.username $prop_file |cut -d'=' -f2 |tr -d "\015"`
        if [ -z $USER ]; then
                echo "Property [aai.tools.username] not found in file $prop_file"
                MISSING_PROP=true
        fi
        PASSWORD=`grep ^aai.tools.password $prop_file |cut -d'=' -f2 |tr -d "\015"`
        if [ -z $PASSWORD ]; then
                echo "Property [aai.tools.password] not found in file $prop_file"
                MISSING_PROP=true
        fi
fi

fname=$JSONFILE
if [ -f /tmp/$(basename $JSONFILE) ]; then
	fname=/tmp/$(basename $JSONFILE)
elif [ ! -f $JSONFILE ]; then
	echo "The file $JSONFILE does not exist"
	exit -1
fi

if [ $MISSING_PROP = false ]; then
        if [ $USEBASICAUTH = false ]; then
                AUTHSTRING="--certificate $PROJECT_HOME/resources/etc/auth/aaiClientPublicCert.pem --private-key $PROJECT_HOME/resources/etc/auth/aaiClientPrivateKey.pem"
        else
                AUTHSTRING="--http-user $USER --http-password=$PASSWORD"
        fi

        wget --method=PUT --no-check-certificate $AUTHSTRING \
            --header="X-FromAppId: $XFROMAPPID" \
            --header="X-TransactionId: $XTRANSID" \
            --header="Accept: application/json" \
            --header="Content-Type: application/json" \
            --body-file=$fname \
            -O - $RESTURL$RESOURCE > $RESULTPATH
        RC=$?

else
        echo "usage: $0 <base-path>"
        RC=-1
fi
if [ "a$JSONFILE" = "a$TEMPFILE" ]; then
	rm $TEMPFILE
fi
echo `date` "   Done $0, exit code is $RC, returning result in $RESULTPATH"
exit $RC
