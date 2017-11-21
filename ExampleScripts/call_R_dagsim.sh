#!/bin/bash
WS_PORT=${WS_PORT:-8080}
curl localhost:${WS_PORT}/bigsea/rest/ws/dagsimR/13/8G/1000/query26 || exit $?
exit 0
