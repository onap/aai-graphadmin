#!/bin/bash 
 
filename=$1; 
 
if [ -z "${filename}" ]; then 
  echo "Please provide a graphson file"; 
  exit 1; 
fi; 
 
if [ ! -f "${filename}" ]; then 
  echo "Unable to find the graphson file ${filename}"; 
  exit 1; 
fi; 
 
sed 's/"label":"vertex"\(.*\)"aai-node-type":\[{"id":"\([^"]*\)","value":"\([^"]*\)"/"label":"\3"\1"aai-node-type":[{"id":"\2","value":"\3"/g' ${filename} > "with_label_${filename}";