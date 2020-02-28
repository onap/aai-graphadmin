#!/bin/bash

###
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright (C) 2017-18 AT&T Intellectual Property. All rights reserved.
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
# resend-dmaap-events.sh  -- This tool is used to resend dmaap events.
#    On certain scenarios due to dns or other networking issue, if A&AI fails to publish events
#       We need a mechanism to resend the dmaap events for objects that haven't modified since
#       So if a pserver object event was supposed to be sent but got lost and a later dmaap event
#       was sent out then we shouldn't be sending dmaap messages
#       It identifies if a dmaap message was already sent by looking at the resource version
#       of the dmaap object that was failed to sendand checks the snapshot and see if they are the same
#
# Parameters:
#
# -b,  (required) <string> the base url for the dmaap server 
# -e,  (required) <file>   filename containing the missed events
# -l,  (optional)          indicating that the script should be run it debug mode
#                          it will not send the dmaap messages to dmaap server 
#                          but it will write to a file named resend_dmaap_server.out
# -p,  (required) <string> the password for the dmaap server 
# -s,  (required) <file>   containing the data snapshot graphson file to compare the resource versions against
# -u,  (required) <string> the username for the dmaap server 
#
# An example of how to use the tool:
# Please use right credentials and right dmaap server in the cluster
#
#  ./resend-dmaap-events.sh -e example_events.txt -s dataSnapshot.graphSON.201808091545 -u username -p example_pass -b https://localhost:3905
#
# For each dmaap message in the example_events.txt, it will check 
# against graphson and try to send it to the dmaap server
# If the example_events.txt contains two events one that wasn't sent to dmaap
# and the other that was already updated by another PUT/DELETE
# and the output of the run will look something like this:
# 
# Output:
# Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f was sent
# Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f not sent
# 
# If lets say, there was a username password issue, you will see something like this:
# Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f was not sent due to dmaap error, please check logs
# Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f not sent
# 
# From the directory in which you are executing the script (not where the script is located)
# You can have it be located and executed in the same place 
# Check for a file called resend_dmaap_error.log as it will give you more details on the error
#
# For testing purposes, if you are trying to run this script and don't want to actually
# send it to a dmaap server, then you can run either of the following:
#
# ./resend-dmaap-events.sh -l -e example_events.txt -s dataSnapshot.graphSON.201808091545
# or
# ./resend-dmaap-events.sh -l -e example_events.txt -s dataSnapshot.graphSON.201808091545 -u username -p example_pass -b https://localhost:3905
#
# Following will output what would have been sent out based on checking the datasnapshot with example_events.txt
#
# Output:
# Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f was sent
# Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f not sent
# 
# Also it will write the dmaap events to a file called dmaap_messages.out that 
# would have been sent out in the current directory where you are executing this script
# 

current_directory=$( cd "$(dirname "$0")" ; pwd -P );
resend_error_log=${current_directory}/resend_dmaap_error.log
resend_output=${current_directory}/dmaap_messages.out

# Prints the usage of the shell script
usage(){
    echo "Usage $0 [options...]";
    echo;
    echo "  -b,   <string> the base url for the dmaap server";
    echo "  -e,   <file>   filename containing the missed events";
    echo "  -l, (optional) indicating that the script should be run it debug mode"
    echo "                 it will not send the dmaap messages to dmaap server "
    echo "                 but it will write to a file named resend_dmaap_server.out"
    echo "  -p,   <string> the password for the dmaap server";
    echo "  -s,   <file>   containing the data snapshot graphson file to compare the resource versions against";
    echo "  -u,   <string> the username for the dmaap server";
    echo;
    echo;
    echo " An example of how to use the tool:";
    echo " Please use right credentials and right dmaap server in the cluster";
    echo;
    echo "  ./resend-dmaap-events.sh -e example_events.txt -s dataSnapshot.graphSON.201808091545 -u username -p example_pass -b https://localhost:3905";
    echo;
    echo " For each dmaap message in the example_events.txt, it will check ";
    echo " against graphson and try to send it to the dmaap server";
    echo " If the example_events.txt contains two events one that wasn't sent to dmaap";
    echo " and the other that was already updated by another PUT/DELETE";
    echo " and the output of the run will look something like this:";
    echo;
    echo " Output:";
    echo " Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f was sent";
    echo " Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f not sent";
    echo " ";
    echo " If lets say, there was a username password issue, you will see something like this:";
    echo " Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f was not sent due to dmaap error, please check logs";
    echo " Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f not sent";
    echo;
    echo " From the directory in which you are executing the script (not where the script is located)";
    echo " You can have it be located and executed in the same place ";
    echo " Check for a file called resend_dmaap_error.log as it will give you more details on the error";
    echo;
    echo " For testing purposes, if you are trying to run this script and don't want to actually";
    echo " send it to a dmaap server, then you can run either of the following:";
    echo;
    echo " ./resend-dmaap-events.sh -l -e example_events.txt -s dataSnapshot.graphSON.201808091545";
    echo " or";
    echo " ./resend-dmaap-events.sh -l -e example_events.txt -s dataSnapshot.graphSON.201808091545 -u username -p example_pass -b https://localhost:3905";
    echo;
    echo " Following will output what would have been sent out based on checking the datasnapshot with example_events.txt";
    echo;
    echo " Output:";
    echo " Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f was sent";
    echo " Dmaap Event with Id 7f7d8a7b-4034-46f3-a969-d7e5cbcbf75f not sent";
    echo;
    echo " Also it will write the dmaap events to a file called dmaap_messages.out that ";
    echo " would have been sent out in the current directory where you are executing this script";
    exit;
}

# Validate the arguments being passed by user
# Checks if the argument of the string is greater than zero
# Also check if the file actually exists
validate(){
    local type_of_file=$1;

    if [ $# -eq 0 ]; then
        echo "Error expecting the validate method to have at least one argument indicating what type";
        exit -1;
    fi;

    shift;

    local arg=$1;

    if [ -z "$arg" ]; then
        echo "Error missing the expected argument for ${type_of_file}";
        exit -1;
    fi;

    if [ ! -f "$arg" ]; then
        echo "Error: file $arg cannot be found, please check the file again";
        exit -1;
    fi;
}

# Checks if the resource version in dmaap message passed for an aai-uri
# is the same as the value in the snapshot file for that version
# If the resource version is the same it will return 0 for success
# Otherwise it will return non zero to indicate that this method failed
resource_version_matches_snapshot_file(){

    local snapshot_file=$1;
    local entity_link=$2;
    local resource_version=$3;
    local action=$4;
    
    if [ -z ${resource_version} ]; then
        echo "Missing the parameter resource version to be passed";
        return -1;
    fi

    # Modify the entity link passed to remove the /aai/v[0-9]+
    aai_uri=$(echo $entity_link | sed 's/\/aai\/v[0-9][0-9]*//g');

    local line=$(grep '"value":"'${aai_uri}'"' ${snapshot_file} 2> /dev/null);

    if [ -z "${line}" ] ; then
        if [ "${action}" = "DELETE" ]; then
            return 0;
        else
            return -1;
        fi;
    fi;

    cnt=$(echo $line | grep -o '"resource-version":\[{"id":"[^"]*","value":"'$resource_version'"}\]' | wc -l);

    if [ $cnt -eq 1 ]; then
        return 0;
    else
        return -1;
    fi;
}

# From a array being passed, it will determine the smallest element
# and return the index of the smallest element
# If the array length is zero, then it will return -1
retrieve_smallest_index(){

    local elements=("${@}");

    if [ ${#elements} -eq 0 ]; then
        return -1;
    fi;

    local smallest_element=${elements[0]};

    local index=0;
    local smallest_index=0;

    for element in ${elements[@]}; do
        if [ $element -lt $smallest_element ]; then
            smallest_index=${index};
        fi;
        index=$((index+1));
    done;

    return ${smallest_index};
}

# Send the dmaap event to the host based on
# the line that was send to the function
send_dmaap(){

    local local_mode=$1;
    local line=$2;
    local username=$3;
    local password=$4;
    local baseurl=$5;
    local resp_code=0;
    
    generated_file=$(uuidgen);

    local json_file=/tmp/${generated_file}.json;
    local curl_output=/tmp/${generated_file}.txt;

    echo ${line} > ${json_file};
    > ${curl_output};
    id=$(echo $line | grep -o '"id":"[^"]*"' | cut -d":" -f2- | sed 's/"//g');

    if [ "$local_mode" = true ]; then
        echo $line >> ${resend_output};
    else

        response_code=$(curl \
            -k -o ${curl_output} -s -w "%{http_code}\n" \
            -u "${username}:${password}" \
            -X POST \
            -H "Content-Type: application/json" \
            -d "@${json_file}" \
            "${baseurl}/events/AAI-EVENT"\
        );

        if [ "$response_code" -ne "200" ]; then
            echo -n "Response failure for dmaap message with id ${id}," >> ${resend_error_log};
            echo " code: ${response_code} body: $(cat ${curl_output})" >> ${resend_error_log};
            resp_code=-1;
        fi;
    fi;
    
    if [ -f "${json_file}" ]; then
        rm $json_file;
    fi;

    if [ -f "${curl_output}" ]; then
        rm $curl_output;
    fi;

    return ${resp_code};
}

# Validates the events file and the snapshot file
# Goes through each line in the missed events file
# Gets all the resource versions there are 
# Finds the smallest resource version there 
# checks if the smallest resource version for the aai uri
# is what is currently in the last snapshot file provided by user
# If it is, it will send an dmaap event out

main(){

    if [ "${#}" -eq 0 ]; then
        usage;
    fi;

    # Get the first character of the first command line argument
    # If the character doesn't start with dash (-)
    # Then fail the script and display usage

    if [ "${1:0:1}" != "-" ]; then
        echo "Invalid option: $1" >&2
        usage;
    fi;

    while getopts ":e:s:u:lp:b:h" opt; do
        case ${opt} in
            l ) # Specify that the application will not send messages to dmaap but save it a file
                local local_mode=true
                ;;
            e ) # Specify the file for missed events
                local missed_events_file=$OPTARG
                ;;
            s ) # Specify the file for snapshot
                local snapshot_file=$OPTARG
                ;;
            u ) # Specify the username to dmaap
                local username=$OPTARG
                ;;
            p ) # Specify the password to dmaap
                local password=$OPTARG
                ;;
            b ) # Specify the baseurl to dmaap
                local hostname=$OPTARG
                ;;
            h ) 
                usage;
                ;;
            \? ) 
                echo "Invalid option: -$OPTARG" >&2
                usage;
                ;;
        esac
    done;

    validate "events_file" $missed_events_file;
    validate "snapshot_file" $snapshot_file;

    if [ "$local_mode" = true ]; then
        > ${resend_output};
    fi;
    
    while read dmaap_event; do
        entity_link=$(echo $dmaap_event | grep -o '"entity-link":"[^"]*"' | cut -d":" -f2- | sed 's/"//g');
        id=$(echo $dmaap_event | grep -o '"id":"[^"]*"' | cut -d":" -f2- | sed 's/"//g');
        action=$(echo $dmaap_event | grep -o '"action":"[^"]*"' | cut -d":" -f2- | sed 's/"//g');
        smallest_resource_version=$(echo $dmaap_event | jq -M '.' | grep 'resource-version' | sort | tail -1 | sed 's/[^0-9]//g');
        resource_version_matches_snapshot_file "${snapshot_file}" "${entity_link}" "${smallest_resource_version}" "${action}" && {
            send_dmaap "${local_mode}" "$dmaap_event" "$username" "$password" "$hostname" && {
                echo "Dmaap Event with Id $id was sent";
            } || {
                echo "Dmaap Event with Id $id was not sent due to dmaap error, please check logs";
            }
        } || {
            echo "Dmaap Event with Id $id not sent";
        }

    done < ${missed_events_file};

}

main $@
