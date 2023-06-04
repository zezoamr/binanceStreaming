from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import column, expr, alias, window, current_timestamp, json_tuple
import os, sys

class spark_analysis():
    
    def __init__(self):
        self.slidingwindow : dataframe = None
        self.archive : dataframe = None
        scala_version = '2.12'  # TODO: Ensure this is correct
        spark_version = '3.3.0'
        packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.2.3','org.apache.commons:commons-pool2:2.11.1'
        ,'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1'
        ]
        
        self.spark = (SparkSession.builder.config("spark.master", "local[2]")
                    .config("spark.jars.packages", ",".join(packages))
                    .getOrCreate())
        self.sc = self.spark.sparkContext
        self.sc.setLocalProperty("spark.scheduler.mode", "FAIR")
        
        #self.sc = self.spark.sparkContext #for debugging
        #self.sc.setLogLevel("INFO")
        
    def terminate(self):
            self.spark.stop()
    
    def read_kafka(self, topic : str, source : str = "localhost:9092",  **kwargs):
        self.archive = ( self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", source)
        .option("subscribe", topic).option("startingOffsets", "earliest") 
        .load()
        )
    
    def write(self):
        self.archive.writeStream.format("csv")\
        .outputMode("append").save(r"D:\projects\binanceStreaming\output\archive")\
        .start() \
        
        self.slidingwindow.writeStream.format("csv")\
        .outputMode("append").save(r"D:\projects\binanceStreaming\output\window_analysis")\
        .start() \
        .awaitTermination()
        
    
    def windowAnalysis(self):
        self.slidingwindow = self.archive.select(expr("cast(key as string) as STREAM"), 
                                                expr("cast(value as string)"), current_timestamp().alias("processingTime")) \
        .withColumn("H", json_tuple(column("value"),"HIGH"))\
        .withColumn("L", json_tuple(column("value"),"LOW"))\
        .withColumn("C", json_tuple(column("value"),"CLOSE"))\
        .groupBy( window( ("processingTime"),  "1 minute").alias("window"), column("STREAM")) \
        .agg({"HIGH" : "avg", "LOW" : "AVG", "CLOSE" : "avg" }) \
        .select(
        column("STREAM"),
        column("window").getField("start").alias("start"),
        column("window").getField("end").alias("end"),
        column("avg(HIGH)"),
        column("avg(LOW)"),
        column("avg(CLOSE)")
        )
        
    def analysis(self):
        try:
            self.read_kafka()
            self.windowAnalysis()
            self.write()
        except KeyboardInterrupt:
            print('Interrupted')
            self.terminate()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
                
    def terminate(self):
            self.spark.stop()
                        
#if __name__ == '__main__':
    # m = spark_analysis()
    # m.analysis()