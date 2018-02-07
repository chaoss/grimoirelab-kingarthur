ARG ARTHUR_VERSION

FROM grimoirelab/arthur:$ARTHUR_VERSION

ENTRYPOINT [\
  "/bin/sh", "-c", \
  "python3 bin/arthurd --no-daemon --host ${ARTHUR_HOST} --port ${ARTHUR_PORT} --database ${ARTHUR_DATABASE} --log-path ${ARTHUR_LOGS_DIR} --no-archive"\
  ]
