#!/bin/bash
set -e
set -x
apt-get update && DEBIAN_FRONTEND=noninteractive && \
apt -y dist-upgrade && \
apt-get install -y --no-install-recommends build-essential libssl-dev libffi-dev python3-dev net-tools libsasl2-dev curl wget procps netcat git libnss3-tools python3-pip && \
pip3 install virtualenv && \
cd /opt && \
git clone https://github.com/zinohome/MACDA.git && \
cd /opt/MACDA && \
git pull && \
mkdir -p /opt/MACDA/log && \
virtualenv venv && \
. venv/bin/activate && \
pip3 install -r requirements.txt && \
cd /opt/MACDA && cp /bd_build/default_env /opt/MACDA/.env && \
cp /bd_build/wait-for /usr/bin/wait-for && chmod 755 /usr/bin/wait-for && \
ls -l /opt/MACDA/.env && cat /opt/MACDA/.env && \
cp /opt/MACDA/docker/bd_build/50_start_h.sh /etc/my_init.d/50_start_macda.sh && \
chmod 755 /etc/my_init.d/50_start_macda.sh