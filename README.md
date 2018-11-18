# sparkperformance

Code for blog - https://ashkrit.blogspot.com/2018/09/anatomy-of-apache-spark-job.html


Sample Data used from https://www.kaggle.com/new-york-city/nyc-parking-tickets

How to Run

spark-submit --master spark://host:port --conf spark.eventLog.enabled=true --class sparkperformance.parking.ParkingTicketApplication ../target/sparkperformance-1.0-SNAPSHOT.jar /data/nyc-parking-tickets


--conf spark.executor.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps'
--conf spark.executor.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:NewSize=400m'

--conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1000000

--conf spark.serializer=org.apache.spark.serializer.KryoSerializer


spark-submit --master spark://ashkrit-laptop.localdomain:7077 --conf spark.eventLog.enabled=true --conf spark.executor.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps' --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --class sparkperformance.parking.ParkingTicketApplication ./target/sparkperformance-1.0-SNAPSHOT.jar /data/nyc-parking-tickets