ARG BASE_IMAGE

FROM pivotaldata/${BASE_IMAGE} as scratch

ADD singlecluster/singlecluster-*.tar.gz /

RUN cd / && mv singlecluster-* singlecluster && chmod a+w singlecluster

FROM pivotaldata/${BASE_IMAGE}

COPY --from=scratch /singlecluster /singlecluster

RUN cp /singlecluster/hadoop/etc/hadoop/{core,hdfs,mapred,yarn}-site.xml /etc/hadoop/conf && \
    cp /singlecluster/hive/conf/hive-site.xml /etc/hive/conf && \
    cp /singlecluster/hbase/conf/hbase-site.xml /etc/hbase/conf
