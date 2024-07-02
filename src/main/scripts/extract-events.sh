#!/bin/sh
usage(){
    echo "Usage $0 input-file output-file event-type";
}

if [ "${#}" -lt 3 ]; then
        usage;
	exit -1
fi;

input_file=$1
output_file=$2
event_type=$3

grep "|${event_type}|" ${input_file} > ${output_file}.1
sed -i -e '/InvokeReturn/s/^.*$//g' ${output_file}.1
sed -i '/^$/d' ${output_file}.1
cat ${output_file}.1 | awk -F '|' '{print $29}' > ${output_file}
rm ${output_file}.1
exit 0
