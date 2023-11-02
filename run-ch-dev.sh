#!/bin/bash

echo "starting clickhouse server .."

#docker run -d \
  #--cap-add=SYS_NICE --cap-add=NET_ADMIN --cap-add=IPC_LOCK \
  #--user ${UID}:${GID} \
  #--name some-clickhouse-server \
  #-v $(realpath ./ch_data):/var/lib/clickhouse/ \
  #-v $(realpath ./ch_logs):/var/log/clickhouse-server/ \
  #--network=host \
  #--ulimit nofile=262144:262144 \
  #clickhouse/clickhouse-server;
  #

#mkdir -p ch_data
#mkdir -p ch_logs

docker run -d \
  --name ch-dev \
  --ulimit nofile=262144:262144 \
  --network=host \
  --cap-add=SYS_NICE --cap-add=NET_ADMIN --cap-add=IPC_LOCK \
  clickhouse/clickhouse-server

# docker exec -it ch-dev bash (or clickhouse-client)
