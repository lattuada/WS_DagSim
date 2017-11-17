#!/bin/bash
WS_PORT=${WS_PORT:-8080}

#                                            AppID/DatasetSize/Deadline
curl http://localhost:${WS_PORT}/bigsea/rest/ws/resopt/query26/1000/400000

