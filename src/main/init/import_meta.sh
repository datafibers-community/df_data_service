#!/bin/bash
set -e

mongoimport -d DEFAULT_DB -c df_installed --file df_installed.json --drop