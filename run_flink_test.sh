#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
function stop_pyflink_process {
    # Terminate Kafka process if it still exists
    jps -vl | grep -i 'pythonGatewayServer' | awk '{print $1}' | xargs kill
}

CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"

export PYTHONPATH=${PYTHONPATH}:${CURRENT_DIR}
# run PyFlink Python UDF job
python ${CURRENT_DIR}/python/flink/flink-kafka-pyflink-perf-test.py java 1.11 &
python ${CURRENT_DIR}/python/flink/flink-kafka-pyflink-perf-test.py python 1.11 &
# run 1.10
source /Users/duanchen/test/test_case/venv/bin/activate
python ${CURRENT_DIR}/python/flink/flink-kafka-pyflink-perf-test.py python 1.10 &
source deactivate
