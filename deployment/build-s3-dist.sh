#!/usr/bin/env bash
echo "Staring to build distribution"
echo "make build"
echo "Pipeline type ${1}"
echo "Version ${VERSION}"

export BUCKET_PREFIX=solutions
if [ $1 = "mainline" ]; then
    export BUCKET_PREFIX=solutions-test
fi
if [ $1 = "feature" ]; then
    export BUCKET_PREFIX=solutions-features
fi
echo ${VERSION} > ../source/code/version.txt

echo "Bucket prefix for distribution '${BUCKET_PREFIX}'"
cd ../source/code
echo "make bucket=${BUCKET_PREFIX}"
make bucket=$BUCKET_PREFIX

cd ../../deployment
echo "mkdir -p dist"
mkdir -p dist
cp ops-automator-latest.template dist/ops-automator.template
cp ops-automator-`cat ../source/code/version.txt`.zip dist/ops-automator-`cat ../source/code/version.txt`.zip
# adding cloudwatch handler zip
cp cloudwatch-handler-`cat ../source/code/version.txt`.zip dist/cloudwatch-handler-`cat ../source/code/version.txt`.zip
# rm instance-scheduler-`cat ../source/code/version.txt`.template
# rm instance-scheduler-remote-`cat ../source/code/version.txt`.template
echo "Completed building distribution"
