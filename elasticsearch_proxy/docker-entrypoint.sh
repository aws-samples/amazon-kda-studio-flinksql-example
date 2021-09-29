#!/usr/bin/env sh
set -eu
envsubst '${ES_ENDPOINT}' < /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf
exec "$@"

