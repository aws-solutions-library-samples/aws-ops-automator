# dockerfile for Ops Automator Image, version %version%
FROM amazonlinux:latest
ENV HOME /homes/ec2-user
WORKDIR /homes/ec2-user
ADD ops-automator-ecs-runner.py ./
ADD requirements.txt ./
RUN yum update -y && \
	yum install -y python3
RUN echo "alias python=python3" >> /homes/ec2-user/.bashrc
RUN alias python=python3 && \
	alias pip=pip3 && \
	pip3 install boto3 && \
	pip3 install -r requirements.txt