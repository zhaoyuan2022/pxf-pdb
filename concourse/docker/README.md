#Docker container for GPDB development/testing
##Requirements

- docker 1.13 (with 3-4 GB allocated for docker host)

Map-R 5.2 Image
```
docker run --rm --privileged -p 8443:8443 -it pivotaldata/gpdb-dev:centos6-mapr5.2
```