#!/bin/bash
while true; do
        echo "Iterated"
        python3 writerrange.py $1 $2
done
echo "Finished Writing Requested Logs"
