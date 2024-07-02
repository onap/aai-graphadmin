#!/bin/sh

#Create empty partial snapshot file
INPUT_DATASNAPSHOT_FILE=$1

set -A nodes customer service-subscription service pserver cloud-region availability-zone tenant zone complex
 > $INPUT_DATASNAPSHOT_FILE".partial"

for nodeType in ${nodes[@]}
	do
         grep "aai-node-type.*\"value\":\"$nodeType\"" $INPUT_DATASNAPSHOT_FILE >>$INPUT_DATASNAPSHOT_FILE'.partial'
    done
exit 0
