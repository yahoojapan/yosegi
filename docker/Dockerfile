# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM centos:centos7 as java-base

RUN yum -y install wget

# Install OpenJDK
RUN wget https://download.java.net/java/ga/jdk11/openjdk-11_linux-x64_bin.tar.gz
RUN tar xvf openjdk-11_linux-x64_bin.tar.gz -C /usr/local

# Install maven
RUN curl -OL https://archive.apache.org/dist/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.tar.gz
RUN tar -xzvf apache-maven-3.6.1-bin.tar.gz
RUN mv apache-maven-3.6.1 /opt/

FROM centos:centos7
MAINTAINER elmo

ENV BUILD_WORK_DIR=$BUILD_WORK_DIR
ENV JAVA_HOME=/usr/local/jdk-11
ENV PATH=$PATH:/usr/local/jdk-11/bin:/opt/apache-maven-3.6.1/bin

COPY --from=java-base /usr/local/jdk-11 /usr/local/jdk-11
COPY --from=java-base /opt/apache-maven-3.6.1 /opt/apache-maven-3.6.1

RUN alternatives --install /usr/bin/java java /usr/local/jdk-11/bin/java 1

RUN yum install -y  git

COPY build.sh /build.sh
RUN chmod 700 /build.sh
CMD ["/build.sh"]
