#!/bin/bash

WORDS=('matter' 'length' 'heaven' 'happy' 'a' 'world' 'gate')
NUMS=('99'     '67'     '87'     '86'   '9976' '243' '20')

for i in `seq 0 $((${#WORDS[@]} - 1))`
do
        grep -r -w ${WORDS[$i]} worker_storage/*/final_results
        echo 'ACTUAL: '${NUMS[$i]}
        echo ''
done
