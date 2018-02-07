ARG ARTHUR_VERSION

FROM grimoirelab/arthur:$ARTHUR_VERSION

ENTRYPOINT [\
  "/bin/sh", "-c", \
  "exec python3 bin/arthurw -d ${ARTHUR_DATABASE} ${ARTHUR_WORKER_QUEUE}"\
  ]
