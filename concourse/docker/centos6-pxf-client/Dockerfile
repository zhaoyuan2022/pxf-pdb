ARG BASE_IMAGE

FROM pivotaldata/${BASE_IMAGE}

ARG YUM_REPOFILE_URL

RUN cd /etc/yum.repos.d && wget -q ${YUM_REPOFILE_URL} && \
  yum install -y hadoop-client hive hbase && yum clean all
