/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--deploy-mode client \
--driver-memory 1G \
--executor-memory 1G \
--executor-cores 1 \
--conf "spark.cores.max=2" \
--conf "spark.flight.host=0.0.0.0" \
--conf "spark.flight.port=9000" \
--conf "spark.flight.public.host=localhost" \
--conf "spark.flight.public.port=$FORWARDED_PORT" \
--conf "spark.flight.peers=$PEERS" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.hive.metastore.uris=thrift://hive-metastore:9083" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.sql.warehouse.dir=/warehouse" \
--driver-class-path /opt/spark-jars/spark-flight-sql.jar \
--conf "spark.executor.extraClassPath=/opt/spark-jars/spark-flight-sql.jar" \
--class com.tokoko.spark.flight.SparkFlightSqlServer \
/opt/spark-jars/spark-flight-sql.jar

#spark.flight.host 0.0.0.0
#spark.flight.port 9000
#spark.flight.internal.host localhost
#spark.flight.internal.port 9000
#spark.flight.public.host localhost
#spark.flight.public.port 9000
#spark.flight.peers "172.19.0.4:9000,localhost:9002;172.19.0.3:9000,localhost:9001"
