#!/bin/bash
rm -rf node_modules
rm index.zip
npm install
zip -r index.zip *
s3cmd put index.zip s3://collier-matthew --signature-v2
