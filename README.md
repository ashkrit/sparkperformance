# sparkperformance

Code for blog - https://ashkrit.blogspot.com/2018/09/anatomy-of-apache-spark-job.html


Sample Data used from https://www.kaggle.com/new-york-city/nyc-parking-tickets

How to Run

spark-submit --master spark://host:port --class sparkperformance.parking.ParkingTicketApplication ../target/sparkperformance-1.0-SNAPSHOT.jar /data/nyc-parking-tickets