ARG BASE_IMAGE

FROM pivotaldata/${BASE_IMAGE} as scratch

ADD pxf_src pxf_src

RUN cd pxf_src/server && make tar

RUN CWD=$(pwd) && cd pxf_src/automation && PXF_HOME=${CWD}/pxf_src/server/build/stage make dev

FROM pivotaldata/${BASE_IMAGE}

RUN mkdir -p /home/gpadmin/.m2 /home/gpadmin/.gradle

COPY --from=scratch /root/.m2 /home/gpadmin/.m2

COPY --from=scratch /root/.gradle /home/gpadmin/.gradle
