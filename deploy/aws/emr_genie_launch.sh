#!/bin/bash

if [ "$EMR_GENIE_BOOTSTRAP_LOC" == "" ]; then
    echo "Set EMR_GENIE_BOOTSTRAP_LOC to point to emr_genie_bootstrap.sh on S3"
    exit 1
fi

echo "Using EMR Genie bootstrap action from: $EMR_GENIE_BOOTSTRAP_LOC"
 
# Launching EMR
elastic-mapreduce --create --alive --instance-type m1.xlarge --instance-count 2  \
    --ssh --debug --trace --visible-to-all-users --name "Genie Testing" \
    --hive-interactive --hive-versions 0.11.0 --pig-interactive --pig-versions 0.11.1 \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/run-if \
    --args "instance.isMaster=true,$EMR_GENIE_BOOTSTRAP_LOC"