#!/bin/bash

for((j=0;j<10;))
do
    for((i=1;i<=33;i++));
    do
    cp "/data/source/FRAME-DATA_1.BIN" "/data/input/FRAME-DATA_1$i.BIN"
    cp "/data/source/FRAME-DATA_2.BIN" "/data/input/FRAME-DATA_2$i.BIN"
    cp "/data/source/FRAME-DATA_3.BIN" "/data/input/FRAME-DATA_3$i.BIN"
    done
    rm -rf /data/finished/*
sleep 30
done
