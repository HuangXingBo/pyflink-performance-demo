################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import logging
import sys
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf
from pyflink.table.descriptors import Kafka, Schema, Json, Elasticsearch


def register_source(st_env):
    st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("0.11")
            .topic("performance_source")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")) \
        .with_format(  # declare a format for this system
            Json()
            .schema(DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT())]))
            .fail_on_missing_field(True)) \
        .with_schema(  # declare the schema of the table
            Schema()
            .field("a", DataTypes.INT())) \
        .in_append_mode() \
        .create_temporary_table("source")


def register_sink(st_env, index_name):
    st_env \
        .connect(
            Elasticsearch()
            .version("7")
            .host("localhost", 9200, "http")
            .index(index_name)
            .document_type('pyflink')
            .key_delimiter("_")
            .key_null_literal("null")
            .failure_handler_ignore()
            .disable_flush_on_checkpoint()
            .bulk_flush_max_actions(42)
            .bulk_flush_max_size("42 mb")
            .bulk_flush_interval(3000)
            .bulk_flush_backoff_constant()
            .bulk_flush_backoff_max_retries(3)
            .bulk_flush_backoff_delay(3000)
            .connection_max_retry_timeout(3)) \
        .with_schema(
            Schema()
            .field("a", DataTypes.INT())) \
        .with_format(
            Json()
            .schema(DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT())]))) \
        .in_upsert_mode() \
        .create_temporary_table("sink")


def main(args):
    func = args[1]
    version = args[2]
    index_name = '_'.join(["performance_pyflink", func, version])
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    environment_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=environment_settings)

    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 300000)
    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 1000)
    t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse", True)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["id"],
    #         [DataTypes.INT(False)]))

    @udf(input_types=[DataTypes.INT(False)], result_type=DataTypes.INT(False))
    def inc(x):
        return x + 1

    t_env.register_function("inc", inc)
    t_env.register_java_function("java_inc", "com.alibaba.flink.function.JavaInc")
    register_source(t_env)
    register_sink(t_env, index_name)

    source = t_env.from_path("source")

    if func == 'java':
        table = source.select("java_inc(a) as a")
    else:
        table = source.select("inc(a) as a")

    table.filter("a % 1000000 = 0") \
        .insert_into("sink")

    beg_time = time.time()
    t_env.execute("Python UDF")
    print("PyFlink Python UDF inc() consume time: " + str(time.time() - beg_time))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    main(sys.argv)
