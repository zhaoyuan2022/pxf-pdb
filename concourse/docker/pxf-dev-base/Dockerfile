ARG BASE_IMAGE

FROM pivotaldata/${BASE_IMAGE} as scratch

ADD pxf_src pxf_src

RUN cd pxf_src/server && make tar && chown -R 1000:1000 /root/.gradle

RUN CWD=$(pwd) && cd pxf_src/automation && \
    PXF_HOME=${CWD}/pxf_src/server/build/stage make dev && \
    chown -R 1000:1000 /root/.m2

FROM pivotaldata/${BASE_IMAGE}

COPY --from=scratch /root/.m2 /root/.m2

COPY --from=scratch /root/.gradle /root/.gradle

RUN mkdir -p /home/gpadmin && \
    ln -s /root/.m2 /home/gpadmin/.m2 && \
    ln -s /root/.gradle /home/gpadmin/.gradle
