FROM python:3.5-alpine

ARG ARTHUR_VERSION

ENV \
  ARTHUR_HOST="0.0.0.0"\
  ARTHUR_PORT="8080" \
  ARTHUR_DATABASE="redis://localhost/8" \
  ARTHUR_LOGS_DIR="/arthur/logs"

EXPOSE 8080

LABEL \
  maintainer="Chaoss" \
  description="Arthur is a distributed job queue platform that schedules and executes Perceval" \
  project="https://github.com/chaoss/grimoirelab-kingarthur" \
  version="$ARTHUR_VERSION"

RUN \
  addgroup -g 1000 arthur && \
  adduser -G arthur -D -u 1000 -h /arthur arthur

RUN \
  apk add --no-cache tini git gcc musl-dev

COPY --chown=arthur . /arthur

WORKDIR /arthur

USER arthur

RUN \
  mkdir /arthur/logs && \
  pip3.5 install --no-cache-dir --upgrade pip && \
  pip3.5 install --no-cache-dir --user -r requirements.txt && \
  python3 setup.py install --user

# /tmp is used by arthur to store temp files like git repository
VOLUME ["/tmp", "/arthur/logs"]

ENTRYPOINT [\
  "/sbin/tini","--",\
  "/bin/sh", "-c", \
  "/usr/local/bin/python3 bin/arthurd --no-daemon --host ${ARTHUR_HOST} --port ${ARTHUR_PORT} --database ${ARTHUR_DATABASE} --log-path ${ARTHUR_LOGS_DIR} --sync"\
  ]
