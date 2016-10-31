#!/bin/bash
#
# Kills cassandra on all nodes mentioned in the dcl_config_file
#

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [vicci_dcl_config_file]"
    exit
fi

dcl_config=$1
usr_name="$USER"
cops_dir=$(pwd)

num_dcs=$(grep num_dcs $dcl_config | awk -F "=" '{ print $2 }')
ips=($(grep cassandra_ips $dcl_config | awk -F "=" '{ print $2 }'))
ips=($(echo "echo ${ips[@]}" | bash))

#kill in parallel
set -m #need monitor mode to fg processes
for ip in ${ips[@]}; do
#HL: we need to modify this to reflect our cluster's setting
    ssh -t -t -o StrictHostKeyChecking=no ${usr_name}@$ip "${cops_dir}/kill_all_cassandra.bash" &
done

for ip in ${ips[@]}; do
    fg
done