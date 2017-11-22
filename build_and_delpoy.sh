#!/usr/bin/env bash
npm run build
echo "[INFO] Deploy the UI code to data service ..."
rm -r -f ../df_data_service/src/main/resources/dfa/*
rm -r -f build/*
cp -r build/* ../df_data_service/src/main/resources/dfa/
echo "[INFO] Deployed. Switch to data service dir .."
cd ../df_data_service
