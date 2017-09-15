#!/usr/bin/env bash
npm run build
rm -r -f ../df_data_service/src/main/resources/dfa/
cp -r build/* ../df_data_service/src/main/resources/dfa/
