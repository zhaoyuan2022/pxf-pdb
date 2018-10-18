ARG BASE_IMAGE=gpdb-dev:centos6

FROM pivotaldata/${BASE_IMAGE}

ADD pxf_src /tmp/pxf_src

RUN cd /tmp/pxf_src/server && make tar && \
    CWD=$(pwd) && cd /tmp/pxf_src/automation && \
    PXF_HOME=/tmp/pxf_src/server/build/stage make dev && \
    mkdir -p /home/gpadmin && \
    mv /root/.m2 /home/gpadmin && \
    chown -R 1000:1000 /home/gpadmin/.m2 && \
    ln -s /home/gpadmin/.gradle /root/.gradle && \
    rm -rf /tmp/pxf_src
