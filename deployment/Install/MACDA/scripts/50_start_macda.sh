#!/bin/bash
cd /opt/MACDA && \
sleep 60 && \
nohup /opt/MACDA/venv/bin/faust --datadir=/tmp/worker1 -A app worker -l info --web-port=6166 > /dev/null 2>&1 &