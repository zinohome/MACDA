venv/bin/faust -A app worker -l info --web-port=6000
faust -A app worker -l info --web-port=6000

Kaitai:
cd kaitai
python3 -m http.server 8000

 faust --datadir=/tmp/worker1 -A app worker -l info --web-port=6166
 faust --datadir=/tmp/worker2 -A app worker -l info --web-port=6167
 faust --datadir=/tmp/worker3 -A app worker -l info --web-port=6168
 faust --datadir=/tmp/worker4 -A app worker -l info --web-port=6169
 faust --datadir=/tmp/worker5 -A app worker -l info --web-port=6170