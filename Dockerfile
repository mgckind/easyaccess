FROM oraclelinux:7-slim as oracle

RUN  curl -o /etc/yum.repos.d/public-yum-ol7.repo https://yum.oracle.com/public-yum-ol7.repo && \
     yum-config-manager --enable ol7_oracle_instantclient && \
     yum -y install oracle-instantclient18.3-basic

FROM ubuntu:20.04

# ORACLE DB Client installation (https://oracle.github.io/odpi/doc/installation.html#oracle-instant-client-zip)
ENV PATH=$PATH:/usr/lib/oracle/18.3/client64/bin
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/oracle/18.3/client64/lib:/usr/lib
COPY --from=oracle /usr/lib/oracle/ /usr/lib/oracle
COPY --from=oracle /lib64/libaio.so.1 /usr/lib

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3-pip         \
  libaio1             \
  wget                \
  imagemagick         \
  stiff               \
  mpich               \
  libopenmpi-dev      \
  openssh-client      \
  unzip               \
  && rm -rf /var/lib/apt/lists/*

ARG UID=1001
RUN echo "Building image with \"worker\" user ID: ${UID}"
RUN useradd --create-home --shell /bin/bash worker --uid ${UID}

# Copy easyaccess and compile:
WORKDIR /home/worker
COPY --chown=worker:worker ./ ./easyaccess
WORKDIR /home/worker/easyaccess
RUN python3 setup.py install
