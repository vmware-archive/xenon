# This image is built by Dockerfile in machines repository at docker/jre8.
FROM jre8-minimal:latest

ADD root /

RUN mkdir -p /opt/dcp
ADD base /opt/dcp

RUN echo "hosts: files dns" > /etc/nsswitch.conf

WORKDIR /opt/dcp
CMD bin/run.sh
