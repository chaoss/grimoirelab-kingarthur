# Copyright (C) 2018 Bitergia
# GPLv3 License

FROM debian:stretch-slim
MAINTAINER Alvaro del Castillo <acs@bitergia.com>

ENV DEBIAN_FRONTEND noninteractive
ENV DEPLOY_USER grimoirelab
ENV DEPLOY_USER_DIR /home/${DEPLOY_USER}

# Initial user
RUN useradd ${DEPLOY_USER} --create-home --shell /bin/bash

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

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV LANG C.UTF-8

# Install pip packages needed
RUN pip3 install setuptools
# Installing perceval extra packages break arthur for a reason not understood yet
RUN pip3 install perceval-mozilla
RUN pip3 install perceval-opnfv
# Not yet available
# RUN pip3 install perceval-puppet
# The above packages modify the perceval/__init.py__ breaking perceval in some cases
# Reinstall perceval so the file is restored
RUN pip3 install perceval --upgrade --force-reinstall
RUN pip3 install kingarthur

ADD stage ${DEPLOY_USER_DIR}/stage
RUN chmod 755 ${DEPLOY_USER_DIR}/stage

USER ${DEPLOY_USER}
WORKDIR ${DEPLOY_USER_DIR}

CMD ${DEPLOY_USER_DIR}/stage
