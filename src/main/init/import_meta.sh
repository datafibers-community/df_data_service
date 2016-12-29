#!/bin/bash
set -e

mongoimport -c df_installed -d DEFAULT_DB --file df_installed.json