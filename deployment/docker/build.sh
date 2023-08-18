#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=nb-v1.0913
docker build --no-cache -t $IMGNAME:$IMGVERSION .
