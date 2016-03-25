## Analysis of domestic US flights departing from San Francisco, CA
==
### Description
This project consists of a REST service to analyze data from flights departing from San Francisco, CA. It allows to compare the performance of different airlines in multiple ways (ie. choose a destination and receive a comparison of the airlines operating the route, including number of flights, average delay, maximum delay, percentage of flights delayed, percentage of flights cancelled, etc.). This information is helpful when buying a flight ticket.

### Data
The data used for this project corresponds to on-time performance of domestic US flights departing from San Francisco, CA from October 2014 to September 2015. This data was obtained from the [Bureau of Transportation Statistics](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236)
The BTS returns a CSV file per month, which can be merged in the terminal:
```
cat *.csv >> merged.csv
```
### AWS
This project uses AWS:
- RDS: The data is loaded into a postgreSQL database through ```data_loader.py``` script
- EC2: The server is run in an EC2 box

### How to run this code
Once the data is loaded into the database in AWS and the EC2 box is spinned up, we can setup the server by 
```
screen python server.py
```


