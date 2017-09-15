#!/usr/bin/env bash
npm run build
echo "Deploy the UI code to data service ..."
rm -r -f ../df_data_service/src/main/resources/dfa/
cp -r build/* ../df_data_service/src/main/resources/dfa/
echo "Deployed. Do not forget to checkin the changes in df_data_service"
