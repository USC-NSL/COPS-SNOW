# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 11:18:13 2015

@author: khiem
"""

import numpy as np
import matplotlib.pyplot as plt

percentile_list = ['', 'Median', '90%-ile', '95%-ile', '99%-ile', '99.9%-ile']
def read_file(fn):
    data = []
    with open(fn, "rb") as f:
        lines = f.readlines()
    
    for l in lines:
        data.append(l.strip().split("\t"))

    return np.array(data).astype(int)

def compute_avg_latency(fn):
    data = read_file(fn)

    nrows = data.shape[0]
    #ncols = data.shape[1]
    nclients = 6
    pdata = []    
    
    for ri in range(0, nrows, 6):
        avg_lat = np.average(data[ri:ri+nclients][:], axis=0)
        #print avg_lat
        pdata.append(avg_lat)
        
    pdata = np.array(pdata)
    
    return pdata
    
def compute_mat(latency_fn, throughput_fn):
    latency_mat = compute_avg_latency(latency_fn)/1000
    throughput_mat = read_file(throughput_fn)
    
    #print throughput_mat[:,1:]
    return np.hstack([latency_mat,throughput_mat[:,1:]])


def plot(ro6_latency_fn,ro6_throughput_fn,eiger_latency_fn,eiger_throughput_fn,idx=1):
    # 10% write
    #ro6_latency_fn = "/Users/khiem/ro6_01.read.latency"
    #ro6_throughput_fn = "/Users/khiem/ro6_01.ops"
    #eiger_latency_fn = "/Users/khiem/eiger_01.read.latency"
    #eiger_throughput_fn = "/Users/khiem/eiger_01.ops"
    
    ro6_read = compute_mat(ro6_latency_fn, ro6_throughput_fn)
    eiger_read = compute_mat(eiger_latency_fn, eiger_throughput_fn)
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    
    plt.plot(ro6_read[:,-1], ro6_read[:,idx],label='ro6')
    for (lbl,xy) in zip(ro6_read[:,0],zip(ro6_read[:,-1], ro6_read[:,idx])):
        ax.annotate('%s' % str(int(lbl*1000)), xy=xy)
    plt.plot(eiger_read[:,-1], eiger_read[:,idx],label='eiger')
    for (lbl,xy) in zip(eiger_read[:,0],zip(eiger_read[:,-1], eiger_read[:,idx])):
        ax.annotate('%s' % str(int(lbl*1000)), xy=xy)

    graph_title = ""
    if "read" in ro6_latency_fn:
        graph_title = "Read Latency " + percentile_list[idx]
    elif "write" in ro6_latency_fn:
        graph_title = "Write Latency " + percentile_list[idx]
    elif "overall" in ro6_latency_fn:
        graph_title = "Overall Latency " + percentile_list[idx]

    plt.xlabel("Throughput (ops/sec)")
    plt.ylabel("Latency (ms)")
    plt.title(graph_title)
    plt.legend(loc=2)
    plt.grid()
    plt.show()


if __name__ == "__main__":
    
    latency_type = "read.latency"
    #latency_type = "write.latency"
    #latency_type = "overall.latency"
    # 10% write
    ro6_latency_fn = "/Users/khiem/ro6_01." + latency_type
    ro6_throughput_fn = "/Users/khiem/ro6_01.ops"
    eiger_latency_fn = "/Users/khiem/eiger_01." + latency_type
    eiger_throughput_fn = "/Users/khiem/eiger_01.ops"
    
    #idx = 2
    #plot(ro6_latency_fn,ro6_throughput_fn,eiger_latency_fn,eiger_throughput_fn,idx)
    for idx in range(1,6):
        plot(ro6_latency_fn,ro6_throughput_fn,eiger_latency_fn,eiger_throughput_fn,idx)
        break


#    ro6_latency_fn = "/Users/khiem/ro6_04.read.latency"
#    ro6_throughput_fn = "/Users/khiem/ro6_04.ops"
#    eiger_latency_fn = "/Users/khiem/eiger_04.read.latency"
#    eiger_throughput_fn = "/Users/khiem/eiger_04.ops"
    
#    ro6_04_read = compute_mat(ro6_latency_fn, ro6_throughput_fn)
#    eiger_04_read = compute_mat(eiger_latency_fn, eiger_throughput_fn)
    
    
