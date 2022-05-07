/opt/spark/bin/spark-shell \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.hive.metastore.uris=thrift://hive-metastore:9083" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.sql.warehouse.dir=/warehouse"
