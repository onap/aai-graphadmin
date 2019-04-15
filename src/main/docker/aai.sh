#
# ============LICENSE_START=======================================================
# org.onap.aai
# ================================================================================
# Copyright © 2017 AT&T Intellectual Property. All rights reserved.
# ================================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============LICENSE_END=========================================================
#
# ECOMP is a trademark and service mark of AT&T Intellectual Property.
#

# set system related env
# and make script compatible both with ubuntu and alpine base images
# jre-alpine image has $JAVA_HOME set and added to $PATH
# ubuntu image requires to set $JAVA_HOME and add java to $PATH manually
if ( uname -v | grep -i "ubuntu" ); then
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-`dpkg --print-architecture | awk -F- '{ print $NF }'`
    export PATH=${JAVA_HOME}:$PATH
fi

# set app related env
export PROJECT_HOME=/opt/app/aai-graphadmin
export AAIENV=dev
export PROJECT_OWNER=aaiadmin
export PROJECT_GROUP=aaiadmin
export PROJECT_UNIXHOMEROOT=/opt/aaihome
export idns_api_url=
export idnscred=
export idnstenant=
umask 0022


