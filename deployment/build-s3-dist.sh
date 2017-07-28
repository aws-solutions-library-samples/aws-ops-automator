#!/usr/bin/env bash
echo "Staring to build distribution"
echo "make build"
echo $1
cd ../source/code
make bucket=$1
cd ../../deployment
echo "mkdir -p dist"
mkdir -p dist
cp ops-automator-latest.template dist/ops-automator.template
cp ops-automator-`cat ../source/code/version.txt`.zip dist
rm ops-automator-`cat ../source/code/version.txt`.zip
rm ops-automator-`cat ../source/code/version.txt`.template
echo "Completed building distribution"