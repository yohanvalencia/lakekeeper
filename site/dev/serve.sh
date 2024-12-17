#!/usr/bin/env bash

set -e

./dev/setup_env.sh

mkdocs serve -a localhost:8087 --dirty --watch .
