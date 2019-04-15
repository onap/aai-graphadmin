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

PROJECT_HOME=/opt/app/aai-graphadmin
export PROJECT_HOME

AAIENV=dev
export AAIENV

PROJECT_OWNER=aaiadmin
PROJECT_GROUP=aaiadmin
PROJECT_UNIXHOMEROOT=/opt/aaihome
export PROJECT_OWNER PROJECT_GROUP PROJECT_UNIXHOMEROOT
umask 0022

export idns_api_url=
export idnscred=
export idnstenant=


