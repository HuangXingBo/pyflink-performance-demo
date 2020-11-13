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
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, BatchTableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table import expressions as expr
from pyflink.table.udf import udf, udaf, AggregateFunction
from pyflink.table.window import Tumble

from python.connectors.sink import PrintTableSink
from python.connectors.source import RangeTableSource, RangeDynamicTableSource


def test_batch_job():
    t_env = BatchTableEnvironment.create(
        environment_settings=EnvironmentSettings.new_instance()
            .in_batch_mode().use_blink_planner().build())
    t_env._j_tenv.getPlanner().getExecEnv().setParallelism(1)

    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 500)
    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 1000)
    t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse", True)

    @udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
    def mean(x):
        return x.mean()

    @udf(result_type=DataTypes.INT())
    def inc(x):
        return x + 1

    class MaxAdd(AggregateFunction):

        def open(self, function_context):
            pass

        def get_value(self, accumulator):
            return accumulator[0]

        def create_accumulator(self):
            return []

        def accumulate(self, accumulator, *args):
            result = 0
            for arg in args:
                result += arg.max()
            accumulator.append(result)

    t_env.register_function("mean", mean)
    t_env.create_temporary_system_function("max_add", udaf(MaxAdd(),
                                                           result_type=DataTypes.INT(),
                                                           func_type="pandas"))

    num_rows = 10000000

    t_env.execute_sql(f"""
        CREATE TABLE source (
            id INT,
            num INT,
            rowtime TIMESTAMP(3)
        ) WITH (
          'connector' = 'Range',
          'start' = '1',
          'end' = '{num_rows}',
          'step' = '1',
          'partition' = '200000'
        )
    """)

    # ------------------------ batch group agg -----------------------------------------------------
    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["value"],
    #         [DataTypes.FLOAT(False)], 100))
    # result = t_env.from_path("source").group_by("num").select("mean(id)")
    # result.insert_into("sink")
    # beg_time = time.time()
    # t_env.execute("Python UDF")
    # print("PyFlink Pandas batch group agg consume time: " + str(time.time() - beg_time))

    # ------------------------ batch group window agg ----------------------------------------------
    # t_env.get_config().get_configuration().set_integer(
    #     "table.exec.window-agg.buffer-size-limit", 100 * 10000)
    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["start", "end", "value"],
    #         [DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3), DataTypes.FLOAT(False)],
    #         100))
    # tumble_window = Tumble.over(expr.lit(1).hours) \
    #     .on(expr.col("rowtime")) \
    #     .alias("w")
    # result = t_env.from_path("source").window(tumble_window) \
    #     .group_by("w, num") \
    #     .select("w.start, w.end, mean(id)")
    # result.insert_into("sink")
    # beg_time = time.time()
    # t_env.execute("Python UDF")
    # print("PyFlink Pandas batch group window agg consume time: " + str(time.time() - beg_time))

    # ------------------------ batch over window agg -----------------------------------------------
    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["num", "a", "b", "c", "d", "e", "f", "g", "h", "i"],
    #         [DataTypes.INT(), DataTypes.FLOAT(), DataTypes.INT(), DataTypes.FLOAT(),
    #          DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT(),
    #          DataTypes.FLOAT(), DataTypes.FLOAT(),
    #          ],
    #         100))
    # beg_time = time.time()
    # t_env.execute_sql("""
    #             insert into sink
    #             select num,
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN UNBOUNDED preceding AND UNBOUNDED FOLLOWING),
    #              max_add(id, id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN UNBOUNDED preceding AND 0 FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
    #             from source
    #         """).wait()
    # print("PyFlink Pandas batch group over window agg consume time: " + str(time.time() - beg_time))


def test_stream_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    environment_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=environment_settings)

    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 500)
    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 1000)
    t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse", True)

    @udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
    def mean(x):
        return x.mean()

    @udf(result_type=DataTypes.INT())
    def inc(x):
        return x + 1

    class MaxAdd(AggregateFunction):

        def open(self, function_context):
            pass

        def get_value(self, accumulator):
            return accumulator[0]

        def create_accumulator(self):
            return []

        def accumulate(self, accumulator, *args):
            result = 0
            for arg in args:
                result += arg.max()
            accumulator.append(result)

    t_env.register_function("mean", mean)
    t_env.create_temporary_system_function("max_add", udaf(MaxAdd(),
                                                           result_type=DataTypes.INT(),
                                                           func_type="pandas"))

    num_rows = 100000

    t_env.execute_sql(f"""
        CREATE TABLE source (
            id INT,
            num INT,
            rowtime TIMESTAMP(3),
            WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
        ) WITH (
          'connector' = 'Range',
          'start' = '1',
          'end' = '{num_rows}',
          'step' = '1',
          'partition' = '20000'
        )
    """)

    # ------------------------ stream group window agg ---------------------------------------------
    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["start", "end", "value"],
    #         [DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3), DataTypes.FLOAT(False)],
    #         100))
    # tumble_window = Tumble.over(expr.lit(1).hours) \
    #     .on(expr.col("rowtime")) \
    #     .alias("w")
    # result = t_env.from_path("source").window(tumble_window) \
    #     .group_by("w, num") \
    #     .select("w.start, w.end, mean(id)")
    # result.insert_into("sink")
    # beg_time = time.time()
    # t_env.execute("Python UDF")
    # print("PyFlink Pandas stream group window agg consume time: " + str(time.time() - beg_time))

    # ------------------------ stream over window agg ----------------------------------------------
    t_env.register_table_sink(
        "sink",
        PrintTableSink(
            ["num", "a"],
            [DataTypes.INT(), DataTypes.FLOAT()],
            10000))
    beg_time = time.time()
    t_env.execute_sql("""
                insert into sink
                select num,
                 mean(id)
                 over (PARTITION BY num ORDER BY rowtime
                 RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
                from source
            """).wait()
    print("PyFlink Pandas stream group over window agg consume time: " +
          str(time.time() - beg_time))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # test_batch_job()
    test_stream_job()
