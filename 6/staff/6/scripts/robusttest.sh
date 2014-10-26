#!/bin/bash

# This script will start 4 workers, a master server, and a client, each
# as its own separate process. This script will likely come in handy as
# you test your code. You must run this script from your hw6 project's
# root directory.

# To ensure there are no false negatives when testing map worker
# failures, you might want to add a sleep statement (for a few seconds)
# in between the map and reduce phases in the student's MasterServer.java.

if [ -z "$1" ]
then
    SLEEP_TIME="0.5"
else
    SLEEP_TIME="$1"
fi

WORKER_PORTS=('15230' '15231' '15232' '15233')
MASTER_PORT="15229"

for i in `seq 0 $((${#WORKER_PORTS[@]} - 1))`
do
    (java -Dfile.encoding=UTF-8 -classpath bin edu.cmu.cs.cs214.hw6.WorkerServer ${WORKER_PORTS[$i]})& > /dev/null 2>&1
    WORKER_PID[$i]=$!
done

# Sleep for 5 seconds to let workers start.
sleep 5

echo "Starting master server..."
(java -Dfile.encoding=UTF-8 -classpath bin edu.cmu.cs.cs214.hw6.MasterServer ${MASTER_PORT} \
    worker1 127.0.0.1 ${WORKER_PORTS[0]} 1,4,5,7,9 \
    worker2 127.0.0.1 ${WORKER_PORTS[1]} 1,3,6,8,9 \
    worker3 127.0.0.1 ${WORKER_PORTS[2]} 2,3,6,8,10 \
    worker4 127.0.0.1 ${WORKER_PORTS[3]} 2,4,5,7,10) & > /dev/null 2>&1
MASTER_PID=$!

# Sleep for 2 seconds for master to start.
sleep 2

echo -e "\033[;7;mStarting WordCountClient...\033[m"
(java -Dfile.encoding=UTF-8 -classpath bin edu.cmu.cs.cs214.hw6.plugin.wordcount.WordCountClient 127.0.0.1 ${MASTER_PORT}) &
CLIENT_PID=$!

RAND=$((RANDOM % 4))
KILL_PORT=${WORKER_PORTS[$RAND]}
echo -e "\033[;7;mKilling worker on port ${KILL_PORT} in $SLEEP_TIME seconds...\033[m"
sleep $SLEEP_TIME

echo -e "\033[;7;mKilling worker on port ${RAND_PORT}...\033[m"
kill -9 ${WORKER_PID[$RAND]}
wait ${WORKER_PID[$RAND]} 2> /dev/null
echo -e "\033[;7;mWorker killed\033[m"

# Setup background thread to kill client process if timeout occurs.
echo -e "\033[;7;mWaiting 30 seconds for client to receive data...\033[m"
sleep 30 && kill -9 ${CLIENT_PID} &> /dev/null &
wait ${CLIENT_PID} 2> /dev/null
if [ "$?" -ne 0 ]
then
    echo "\033[;7;mTimed out waiting for client to exit.\033[m"
fi

# Kill all worker servers.
for i in `seq 0 $((${#WORKER_PORTS[@]} - 1))`
do
    kill -9 ${WORKER_PID[$i]} 2> /dev/null
done

# Kill the master server.
kill -9 ${MASTER_PID} 2> /dev/null
