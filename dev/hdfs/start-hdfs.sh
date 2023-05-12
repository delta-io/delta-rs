#!/bin/bash

[ ! -d "/tmp/hadoop-hadoop/dfs/name" ] && hdfs namenode -format

hdfs --daemon start namenode
hdfs --daemon start datanode

hdfs dfs -chmod 777 /

exec tail -n 1000 -f /var/log/hadoop/*