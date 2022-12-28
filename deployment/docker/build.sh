#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=nb-v1.0911
docker build --no-cache -t $IMGNAME:$IMGVERSION .
