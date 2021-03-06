#!/usr/bin/python

# import os
# user_paths = os.environ['PYTHONPATH']
#
# with open('/Users/roadan/pypath.txt', 'a') as the_file:
#     the_file.write(user_paths)

import ast
import codegen
import os
import sys
from runtime import AmaContext
os.chdir(os.getcwd() + '/build/resources/test/')
# import zipfile
# zip = zipfile.ZipFile('pyspark.zip')
# zip.extractall()
# zip = zipfile.ZipFile('py4j-0.10.4-src.zip', 'r')
# zip.extractall()
# sys.path.insert(1, os.getcwd() + '/executor/src/test/resources/pyspark')
# sys.path.insert(1, os.getcwd() + '/executor/src/test/resources/py4j')
from py4j.java_gateway import JavaGateway, GatewayClient, java_import
from py4j.protocol import Py4JJavaError
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark import accumulators
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer
from pyspark.sql import SparkSession

client = GatewayClient(port=int(sys.argv[1]))
gateway = JavaGateway(client, auto_convert = True)
entry_point = gateway.entry_point
queue = entry_point.getExecutionQueue()

java_import(gateway.jvm, "org.apache.spark.SparkEnv")
java_import(gateway.jvm, "org.apache.spark.SparkConf")
java_import(gateway.jvm, "org.apache.spark.api.java.*")
java_import(gateway.jvm, "org.apache.spark.api.python.*")
java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
java_import(gateway.jvm, "org.apache.spark.sql.*")
java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
java_import(gateway.jvm, "scala.Tuple2")

jconf = entry_point.getSparkConf()
jsc = entry_point.getJavaSparkContext()

conf = SparkConf(_jvm = gateway.jvm, _jconf = jconf)

sc = SparkContext(jsc=jsc, gateway=gateway, conf=conf)

spark = SparkSession(sc, entry_point.getSparkSession())
sqlc = spark._wrapped

ama_context = AmaContext(sc, sqlc)

while True:
    actionData = queue.getNext()
    resultQueue = entry_point.getResultQueue(actionData._2())
    actionSource = actionData._1()
    tree = ast.parse(actionSource)

    for node in tree.body:

        wrapper = ast.Module(body=[node])
        try:
            co  = compile(wrapper, "<ast>", 'exec')
            exec(co)
            resultQueue.put('success', actionData._2(), codegen.to_source(node), '')
        except:
            resultQueue.put('error', actionData._2(), codegen.to_source(node), str(sys.exc_info()[1]))
    resultQueue.put('completion', '', '', '')