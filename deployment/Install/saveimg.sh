docker save -o redpanda.tar harbor.zinohome.com/macda/redpanda:v22.3.9 && \
gzip redpanda.tar && \
docker save -o redpanda-console.tar harbor.zinohome.com/macda/redpanda-console:v2.1.1 && \
gzip redpanda-console.tar && \
docker save -o kafka-connect.tar harbor.zinohome.com/macda/kafka-connect:latest && \
gzip kafka-connect.tar && \
docker save -o passenger.tar phusion/passenger-full:latest && \
gzip passenger.tar && \
docker save -o kafka-ui.tar harbor.zinohome.com/macda/kafka-ui:latest && \
gzip kafka-ui.tar && \
docker save -o ksqldb.tar harbor.zinohome.com/macda/ksqldb-server:0.27.2 && \
gzip ksqldb.tar && \
docker save -o ksqldb-cli.tar harbor.zinohome.com/macda/ksqldb-cli:0.27.2 && \
gzip ksqldb-cli.tar && \
docker save -o mirror-maker.tar harbor.zinohome.com/macda/mirror-maker2:2.8.1 && \
gzip mirror-maker.tar && \
docker save -o minio.tar harbor.zinohome.com/macda/minio:latest && \
gzip minio.tar && \
docker save -o mc.tar harbor.zinohome.com/macda/mc:latest && \
gzip mc.tar && \
docker save -o timescaledb.tar timescale/timescaledb-ha:pg14-latest && \
gzip timescaledb.tar && \
docker save -o pgadmin4.tar dpage/pgadmin4:latest && \
gzip pgadmin4.tar && \
docker save -o macda-nb.tar jointhero/macda:nb-v1.0911 && \
gzip macda-nb.tar && \
docker save -o desktop.tar harbor.zinohome.com/kasmweb/kasm-desktop-focal-web:1.12.1 && \
gzip desktop.tar

