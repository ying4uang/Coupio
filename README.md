

# Coupio
## Coupon Distribution Simulation

Coupio allows you to create campaigns on the fly and see realtime results. In terms of system architecture, there are three main components of Coupio.


* Streaming Layer - Aggregate results on the fly
* Batch Layer - for business analytics
* Data Generation
* Web Layer - [go to repository](https://github.com/ying4uang/Coupio_Web)

##Full System Architecture
[image1]: https://github.com/ying4uang/coupio_demo/blob/master/image/pipeline.png "pipeline"

![alt text][image1]


[//]: # (Image References)


### Streaming Layer
Transaction data is streamed from Kafka to Spark Streaming, joining with lookup data(which contains campaign information and products to pair with a coupon). The joined data is then aggregated and written to redis and displayed on front end flask pages.

### Batch Layer
Transaction data is also pushed to S3 for fault tolerance and further analysis in the data warehouse. I used Secor to push data from Kafaka to S3 and Luigi to schedule to load from S3 into Redshift. The luigi code can be found [here](https://github.com/ying4uang/Coupon_Dist/tree/master/luigi). For secor, since it is mainly configuration files and I have ignored it here for security considerations.


### Data Generation
Transaction data is generated from python with the following information:

customer id
product
category
transaction amount
transaction volume

See detailed code [here](https://github.com/ying4uang/Coupon_Dist/tree/master/data_gen)

### Web Layer
Allows input of campaign information and display joined data
