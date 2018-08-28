FROM pivotaldata/gpdb-dev:centos6

RUN rpm --import http://package.mapr.com/releases/pub/maprgpg.key && \
  groupadd -g 5000 mapr ; useradd -g 5000 -u 5000 mapr && echo -e "mapr\nmapr" | passwd mapr && \
  echo -e "[maprtech]\nname=MapR Technologies" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "baseurl=http://package.mapr.com/releases/v5.2.0/redhat/" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "enabled=1\ngpgcheck=0\nprotect=1\n" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "[maprecosystem]\nname=MapR Technologies" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "baseurl=http://package.mapr.com/releases/MEP/MEP-4.0.0/redhat/" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "enabled=1\ngpgcheck=0\nprotect=1\n" >> /etc/yum.repos.d/maprtech.repo && \
  cat /etc/yum.repos.d/maprtech.repo && echo JAVA_HOME=$JAVA_HOME

RUN yum install -y mapr-cldb mapr-core mapr-core-internal mapr-fileserver mapr-zookeeper mapr-zk-internal mapr-webserver && \
  yum install -y mapr-hbase mapr-hbasethrift mapr-hbase-rest mapr-hive mapr-hiveserver2 mapr-hivemetastore mapr-hivewebhcat

RUN echo "sed -i 's@#export JAVA_HOME=@export JAVA_HOME=/etc/alternatives/java_sdk@g' /opt/mapr/conf/env.sh" > /root/init-script && \
  echo '/opt/mapr/server/configure.sh -C `hostname` -Z `hostname` -N maprdemo.cluster' >> /root/init-script && \
  echo "sed -i 's/AddUdevRules(list/#AddUdevRules(list/' /opt/mapr/server/disksetup" >> /root/init-script && \
  echo "mkdir -p /opt/mapr/disks && fallocate -l 10G /opt/mapr/disks/docker.disk" >> /root/init-script && \
  echo "/opt/mapr/disks/docker.disk" > /tmp/disks && cat /tmp/disks && \
  echo "/opt/mapr/server/disksetup -F /tmp/disks" >> /root/init-script && \
  echo "/opt/mapr/server/configure.sh -C `hostname -i` -Z `hostname -i` -N maprdemo.cluster" >> /root/init-script && \
  chmod +x /root/init-script

ENTRYPOINT /root/init-script && /bin/bash
