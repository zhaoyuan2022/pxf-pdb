#!/usr/bin/env bash

# gsutil cp initialization-for-kerberos.sh gs://pxf-perf/scripts/initialization-for-kerberos.sh

sed -i '/<name>dfs.permissions.enabled<\/name>/ {n; s#false#true#}' /etc/hadoop/conf/hdfs-site.xml
