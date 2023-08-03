#!/bin/bash
IMGNAME=jointhero/macda
IMGVERSION=nb-v1.0912
docker build --no-cache -t $IMGNAME:$IMGVERSION .
