ARG BASE_IMAGE

FROM pivotaldata/${BASE_IMAGE} as scratch

ADD pxf_src pxf_src

RUN pushd pxf_src/server && make tar && popd

RUN CWD=$(pwd) && pushd pxf_src/automation && PXF_HOME=${CWD}/pxf_src/server/build/stage make dev && popd

FROM pivotaldata/${BASE_IMAGE}

RUN mkdir -p /home/gpadmin/.m2 /home/gpadmin/.gradle

COPY --from=scratch /root/.m2 /home/gpadmin/.m2

COPY --from=scratch /root/.gradle /home/gpadmin/.gradle
