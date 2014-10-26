#!/bin/bash

# This script will start 4 workers, a master server, and a client, each 
# as its own separate process. This script will likely come in handy as
# you test your code. You must run this script from your hw6 project's
# root directory.

WORKER_PORTS=('15220' '15221' '15222' '15223')
MASTER_PORT="15219"

echo "Starting ${#WORKER_PORTS[@]} worker server(s)..."
for i in `seq 0 $((${#WORKER_PORTS[@]} - 1))`
do
    (java -Dfile.encoding=UTF-8 -classpath bin edu.cmu.cs.cs214.hw6.WorkerServer ${WORKER_PORTS[$i]}) &
    WORKER_PID[$i]=$!
done

# Sleep for 5 seconds to let workers start.
sleep 5

echo "Starting master server..."
(java -Dfile.encoding=UTF-8 -classpath bin edu.cmu.cs.cs214.hw6.MasterServer ${MASTER_PORT} \
    worker1 127.0.0.1 ${WORKER_PORTS[0]} 1,4,5,7,9 \
    worker2 127.0.0.1 ${WORKER_PORTS[1]} 1,3,6,8,9 \
    worker3 127.0.0.1 ${WORKER_PORTS[2]} 2,3,6,8,10 \
    worker4 127.0.0.1 ${WORKER_PORTS[3]} 2,4,5,7,10) &
MASTER_PID=$!

# Sleep for 2 seconds for master to start.
sleep 2

echo "Running WordCountClient..."
(java -Dfile.encoding=UTF-8 -classpath bin edu.cmu.cs.cs214.hw6.plugin.wordcount.WordCountClient 127.0.0.1 ${MASTER_PORT})

# Kill all worker servers.
for i in `seq 0 $((${#WORKER_PORTS[@]} - 1))`
do
    kill -9 ${WORKER_PID[$i]} 2> /dev/null
done

# Kill the master server.
kill -9 ${MASTER_PID} 2> /dev/null
