#!/bin/bash
#Dynamic Workload

set -u

dynamic_dir=$(echo $0 | awk -F"dynamic" '{ print "dynamic"$2 }' | sed 's/.bash//g')

if [ "$dynamic_dir" == "" ]; then
    echo "autodetect of exp name failed, set dynamic dir manually"
    exit
fi

#######################################
#
# Cluster Setup
#
#######################################

# MacroBenchmark: Big Clusters

set -x

# Initial setup
machine_name=$(uname -n)
username="$USER"

if [ "$(uname -n | grep -E "kodiak|nome")" != "" ]; then
    if [ $# -ne 1 ]; then
	echo "$0: [# servers]"
	exit
    fi

    nservers=$1
    dcl_config=${nservers}_in_kodiak
    client_config=${nservers}_clients_in_kodiak

    #location specific config
    cops_dir="/users/${username}/SNOW/ins1"
    vanilla_dir="/users/${username}/vanilla"
    tools_dir="/users/${username}/vanilla"
    exp_dir="${cops_dir}/experiments"
    stress_dir="${cops_dir}/tools/stress"
    output_dir_base="${exp_dir}/${dynamic_dir}"
    exp_uid=$(date +%s)
    output_dir="${output_dir_base}/${exp_uid}"
    mkdir -p ${output_dir}
    rm $output_dir_base/latest
    ln -s $output_dir $output_dir_base/latest 



    dcl_config_full="${cops_dir}/vicci_dcl_config/${dcl_config}"

    all_servers=($(cat $dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
    all_servers=$(echo "echo ${all_servers[@]}" | bash)
    num_dcs=$(cat $dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')

    strategy_properties="DC0:1"
    for i in $(seq 1 $((num_dcs-1))); do
	strategy_properties=$(echo ${strategy_properties}",DC${i}:1")
    done

    num_servers=$(echo $all_servers | wc -w)
    num_servers_per_dc=$((num_servers / num_dcs))
    
    for dc in $(seq 0 $((num_dcs-1))); do
	this_dc_servers=$(echo $all_servers | sed 's/ /\n/g' | head -n $((num_servers_per_dc * (dc+1))) | tail -n $num_servers_per_dc | xargs)
	servers_by_dc[$dc]=${this_dc_servers}
    done
    echo ${servers_by_dc[@]}



    client_config_full="${cops_dir}/vicci_dcl_config/${client_config}"

    all_clients=$(cat $client_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs)
    all_clients=$(echo "echo ${all_clients[@]}" | bash)

    num_clients=$(echo $all_clients | wc -w)
    num_clients_per_dc=$((num_clients / num_dcs))
    
    for dc in $(seq 0 $((num_dcs-1))); do
	this_dc_clients=$(echo $all_clients | sed 's/ /\n/g' | head -n $((num_clients_per_dc * (dc+1))) | tail -n $num_clients_per_dc | xargs)
	clients_by_dc[$dc]=${this_dc_clients}
    done
    echo ${clients_by_dc[@]}

    kill_all_cmd="${cops_dir}/kodiak_cassandra_killer.bash ${cops_dir}/vicci_dcl_config/${dcl_config}"
    stress_killer="${cops_dir}/kill_stress_kodiak.bash"

    
    source $exp_dir/dynamic_common


else
    echo "Unknown machine_name: ${machine_name}"
    exit
fi



#######################################
#
# Actual Experiment
#
#######################################

source ${exp_dir}/dynamic_defaults

# Test: Latency, Throughput of different operations
# Control: # of dependencies

# fixed parameters
run_time=60
trim=15

echo -e "STARTING $0 $@" >> ${cops_dir}/experiments/progress

num_trials=1
for trial in $(seq $num_trials); do
    #100K columns seems to break stuff
    #for sizeANDkeys in 1:3200000 10:3200000 100:3200000 1000:3200000 10000:320000 100000:32000; do
    #for cols_per_key_read in 1 2 4 5 8 16 32 64; do
    #for cpkrANDkeys in 8:100000 16:10000 32:10000 64:10000 1:1000000 2:100000 4:1000000 5:1000000; do
    #for cpkrANDkeys in 8:100000 16:100000 4:100000 32:10000; do
    #for cpkrANDkeys in 2:100000 4:100000 8:100000 16:100000 32:10000; do
     for cpkrANDkeys in 8:100000; do
    #for cpkrANDkeys in 1:100000 2:100000 4:100000 5:100000 8:100000 16:100000; do
    	cols_per_key_read=$(echo $cpkrANDkeys | awk -F":" '{ print $1 }')
    	total_keys=$(echo $cpkrANDkeys | awk -F":" '{ print $2 }')
    	variable=$cols_per_key_read
#	cols_per_key_write=$cols_per_key_read

#for sizeANDkeys in 1:100000 64:100000 128:100000 256:10000; do
 #       value_size=$(echo $sizeANDkeys | awk -F":" '{ print $1 }')
  #      total_keys=$(echo $sizeANDkeys | awk -F":" '{ print $2 }')
   #     variable=$value_size
#for write_frac in 0.0 0.1 0.2 0.5 1.0; do
#	variable=$write_frac


#for keys_per_read in 1 2 4 5 8 16 32; do
#	variable=$keys_per_read

	echo -e "Running $0\t$variable at $(date)" >> ${cops_dir}/experiments/progress


	cops_cluster_start_cmd
#	cops_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

#        cops_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial $zipfian

#	$kill_all_cmd


	#vanilla_cluster_start_cmd
	#vanilla_populate_cluster ${total_keys} ${cols_per_key_read} ${value_size} ${cols_per_key_write}

	#vanilla_run_experiment $total_keys $value_size $cols_per_key_read $cols_per_key_write $keys_per_read $keys_per_write $write_frac $write_trans_frac $run_time $variable $trial

	#$kill_all_cmd

#	gather_results
    done
done

echo -e "FINISHED $0\tat $(date)" >> {$cops_dir}/experiments/progress

#######################################
#
# Cleanup Experiment
#
#######################################
set +x
#$kill_all_cmd
set -x

#######################################
#
# Process Output
#
#######################################
cd $exp_dir
./dynamic_postprocess_full.bash . ${output_dir} ${run_time} ${trim} shuffle



