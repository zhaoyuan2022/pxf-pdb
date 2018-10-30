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

# install Go utilities in root's home for CI, and in gpadmin's home for development
RUN export ORIG_PWD="$(pwd)" && \
    cd /tmp && \
        wget https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz && \
        tar -C /usr/local -xzf go1.11.1.linux-amd64.tar.gz && \
    cd "$ORIG_PWD" && \
    GOPATH=/root/go go get github.com/golang/dep/cmd/dep && \
    GOPATH=/root/go go get github.com/onsi/ginkgo/ginkgo && \
    echo >>/root/.bash_profile 'export PATH="/root/go/bin:/usr/local/go/bin:$PATH"' && \
    GOPATH=/home/gpadmin/go go get github.com/golang/dep/cmd/dep && \
    GOPATH=/home/gpadmin/go go get github.com/onsi/ginkgo/ginkgo && \
    echo >>/home/gpadmin/.bash_profile 'export PATH="/home/gpadmin/go/bin:/usr/local/go/bin:$PATH"'
