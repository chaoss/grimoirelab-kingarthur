# Copyright (C) 2018 Bitergia
# GPLv3 License

FROM debian:stretch-slim

ARG ARTHUR_VERSION

LABEL \
  maintainer="Alvaro del Castillo <acs@bitergia.com>" \
  description="Arthur is a distributed job queue platform that schedules and executes Perceval" \
  project="https://github.com/chaoss/grimoirelab-kingarthur" \
  version="$ARTHUR_VERSION"

EXPOSE 8080

ENV DEBIAN_FRONTEND noninteractive
ENV DEPLOY_USER grimoirelab
ENV DEPLOY_USER_DIR /home/${DEPLOY_USER}

RUN \
  groupadd -g 1000 arthur && \
  useradd -m -d /arthur -g 1000 -u 1000 -s /bin/sh arthur

# install dependencies
RUN apt-get update && \
    apt-get -y install --no-install-recommends \
        bash locales \
        git gcc \
        python3 \
        python3-pip \
        python3-venv \
        python3-dev \
        unzip curl wget sudo ssh vim \
        && \
    apt-get clean && \
    find /var/lib/apt/lists -type f -delete

RUN echo "${DEPLOY_USER} ALL=NOPASSWD: ALL" >> /etc/sudoers

RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    echo 'LANG="en_US.UTF-8"'>/etc/default/locale && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=en_US.UTF-8

ENV \
  ARTHUR_HOST="0.0.0.0"\
  ARTHUR_PORT="8080" \
  ARTHUR_DATABASE="redis://localhost/8" \
  ARTHUR_LOGS_DIR="/arthur/logs" \
  LANG="en_US.UTF-8" \
  LANGUAGE="en_US:en" \
  LC_ALL="en_US.UTF-8" \
  LANG="C.UTF-8"

COPY --chown=arthur . /arthur

WORKDIR /arthur

USER arthur

RUN \
  mkdir /arthur/logs && \
  pip3 install setuptools && \
  pip3 install --no-cache-dir --upgrade pip && \
  pip3 install --no-cache-dir --user -r requirements.txt && \
  python3 setup.py install --user

# /tmp is used by arthur to store temp files like git repository
VOLUME ["/tmp", "/arthur/logs"]

ENTRYPOINT [\
  "/bin/sh", "-c", \
  "python3 bin/arthurd --no-daemon --host ${ARTHUR_HOST} --port ${ARTHUR_PORT} --database ${ARTHUR_DATABASE} --log-path ${ARTHUR_LOGS_DIR} --sync"\
  ]
