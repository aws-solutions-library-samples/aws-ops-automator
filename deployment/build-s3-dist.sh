#!/usr/bin/env bash
# 
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned 
# 
# This script should be run from the repo's deployment directory 
# cd deployment 
# ./build-s3-dist.sh source-bucket-base-name trademarked-solution-name version-code 
# 
# Parameters: 
#  - source-bucket-base-name: Name for the S3 bucket location where the template will source the Lambda 
#    code from. The template will append '-[region_name]' to this bucket name. 
#    For example: ./build-s3-dist.sh solutions my-solution v1.0.0 
#    The template will then expect the source code to be located in the solutions-[region_name] bucket 
# 
#  - trademarked-solution-name: name of the solution for consistency 
# 
#  - version-code: version of the solution 
function do_cmd { 
    echo "------ EXEC $*" 
    $* 
} 
function do_replace { 
    replace="s/$2/$3/g" 
    file=$1 
    do_cmd sed -i -e $replace $file 
} 

if [ -z "$1" ] | [ -z "$2" ]; then 
	echo "Usage: $0 [bucket] [solution-name] {version}"
    echo "Please provide the base source bucket name, trademark approved solution name and version where the lambda code will eventually reside." 
    echo "For example: ./build-s3-dist.sh solutions trademarked-solution-name v1.0.0" 
    exit 1 
fi 

bucket=$1
echo "export DIST_OUTPUT_BUCKET=$bucket" > ./setenv.sh
solution_name=$2
echo "export DIST_SOLUTION_NAME=$solution_name" >> ./setenv.sh

# Version from the command line is definitive. Otherwise, use version.txt
if [ ! -z "$3" ]; then
	version=$3
elif [ -e ../source/version.txt ]; then
	version=`cat ../source/version.txt`
else
	echo "Version not found. Version must be passed as argument 3 or in version.txt in the format vn.n.n"
fi

if [[ ! "$version" =~ ^v.*? ]]; then
	version=v$version
fi
echo "export DIST_VERSION=$version" >> ./setenv.sh

echo "=========================================================================="
echo "Building $solution_name version $version for bucket $bucket"
echo "=========================================================================="

# Get reference for all important folders  
template_dir="$PWD" # /deployment
template_dist_dir="$template_dir/global-s3-assets"  
build_dist_dir="$template_dir/regional-s3-assets"  
source_dir="$template_dir/../source"
dist_dir="$template_dir/dist"

echo "------------------------------------------------------------------------------"   
echo "[Init] Clean old dist folders"   
echo "------------------------------------------------------------------------------"   
do_cmd rm -rf $template_dist_dir   
do_cmd mkdir -p $template_dist_dir   
do_cmd rm -rf $build_dist_dir   
do_cmd mkdir -p $build_dist_dir   
do_cmd rm -rf $dist_dir
do_cmd mkdir -p $dist_dir

# Copy the source tree to deployment/dist
do_cmd cp -r $source_dir/* $dist_dir 

do_cmd pip install --upgrade pip
# awscli will also install the compatible version of boto3 and botocore
do_cmd pip install --upgrade awscli
do_cmd pip install -r $source_dir/code/requirements.txt -t $dist_dir/code

echo "------------------------------------------------------------------------------"   
echo "[Make] Set up and call make from deployment/dist/code"   
echo "------------------------------------------------------------------------------"
cp $source_dir/version.txt $dist_dir/code 
cd $dist_dir/code
do_cmd make bucket=$bucket solution=$solution_name version=$version
cd $template_dir
# rm -rf dist
chmod +x setenv.sh
echo "Completed building distribution"
