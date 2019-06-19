# dockerfile for Ops Automator Image, version %version%
FROM 137112412989.dkr.ecr.us-west-2.amazonaws.com/amazonlinux:latest
ENV HOME /homes/ec2-user
WORKDIR /homes/ec2-user
ADD ops-automator-ecs-runner.py ./
RUN yum update -y && \
    curl https://bootstrap.pypa.io/get-pip.py>get-pip.py && \
    python get-pip.py && \
    rm get-pip.py && \
    pip install boto3