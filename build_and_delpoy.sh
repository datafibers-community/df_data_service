#!/usr/bin/env bash
npm run build
rm -rf ../df_data_service/src/main/resources/apidoc/
cp -r ./build/* ../df_data_service/src/main/resources/apidoc/