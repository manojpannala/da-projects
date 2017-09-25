###Data Loading into HDFS

##Setting up environment variables
Sys.setenv("HADOOP_CMD"="/opt/cloudera/parcels/CDH/bin/hadoop")
Sys.setenv("HADOOP_STREAMING"="/opt/cloudera/parcels/CDH-4.7.0-1.cdh4.7.0.p0.40/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.7.0.jar")

##Importing airlines data directly to HDFS using rhdfs package
library(rhdfs)
hdfs.init() #Initializes rhdfs package

#Copying Airlines.txt data from Local to HDFS path
hdfs.put('/home/data/Class_12/Airlines.txt','/user/hduser/assignments/') #Copying file to HDFS
hdfs.ls('/user/hduser/assignments/') #Checking the file details

#Loading the rmr2 package
library(rmr2)

#Declaring the variable names and respective data types of Airlines.txt data
col.classes = 
  c(Year = "numeric", Month = "numeric", DayofMonth = "numeric", DayofWeek = "numeric",
    DepTime = "character", CRSDepTime = "character", ArrTime = "character", CRSArrTime = "character", 
    UniqueCarrier = "character", FlightNum = "character", TailNum = "character", 
    ActualElapsedTime = "character", CRSElapsedTime = "character", AirTime = "numeric", 
    ArrDelay = "numeric", DepDelay = "numeric", Origin="character", Dest = "character", 
    Distance = "numeric", TaxiIn = "character", TaxiOut = "character", Cancelled = "character", 
    CancellationCode = "character", Diverted = "numeric", CarrierDelay = "numeric",
    WeatherDelay = "numeric", NASDelay = "numeric", SecurityDelay = "numeric",
    LateAircraftDelay = "numeric")

##Defining custom input format for Airlines.txt data
airline.format =
  make.input.format(
    "csv", 
    sep = ",",
    col.names = names(col.classes),
    colClasses = col.classes)

#Looking at the input data object in HDFS
check<-from.dfs("/user/hduser/assignments/Airlines.txt",format=airline.format)
mode(check) #list object
mode(check$key) #NULL object
mode(check$val) #list object
head(check$val) #Checking values under val part of input object

###Problem4
##Compute the total number of flights distributed across each day of the month
daywiseFlights<-as.data.frame(count(input("/user/hduser/assignments/Airlines.txt",format=airline.format),DayofMonth)) #works like table function in base R

class(daywiseFlights)
colnames(daywiseFlights)<-c("DayofMonth","No_Flights")
head(daywiseFlights)

#Visualizing the output using ggplot2 functions
library(ggplot2)
qplot(x = DayofMonth, 
      y = No_Flights, 
      data = daywiseFlights)

###Problem5
##Compute the total number of canceled flights operated by each airliner for the year 2008 and the reason for Cancelation
canceledFlights<-as.data.frame(
  count(input("/user/hduser/assignments/Airlines.txt",
              format=airline.format),
        Year:UniqueCarrier:Cancelled:CancellationCode)) 

class(canceledFlights)
colnames(canceledFlights)<-c("Year","UniqueCarrier","Canceled","Cancellation_Reason","No_Flights")
head(canceledFlights)

#Visualizing the output using ggplot2 functions
library(ggplot2)
ggplot(canceledFlights[canceledFlights$Canceled=="1",], aes(x=factor(UniqueCarrier),y=No_Flights,fill=Cancellation_Reason)) + geom_bar(stat ="identity")

###Problem6
##Identify the distribution of overall flight delay across various days of the month
#Using rmr2 functions
#Defining the map function
map <- function(k,v) {
  key<-v$DayofMonth
  val<-rowSums(v[c("ArrDelay","DepDelay","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")],na.rm=T)
  keyval(key,val)
}

#Defining the reduce function
reduce <- function(k,vv) {
  keyval(k, sum(vv))
}

#Running the mapreduce function
out<-mapreduce(input = "/user/hduser/assignments/Airlines.txt",
               input.format = airline.format,
               map = map,
               reduce = reduce)

#Getting the output from HDFS
overallDelay.df<-data.frame(from.dfs(out))
colnames(overallDelay.df)<-c("DayofMonth","OverallDelay")
overallDelay.df

#Using plyrmr functions
overallDelay<-as.data.frame(
  input("/user/hduser/assignments/Airlines.txt",
      format=airline.format) %|%
  group(DayofMonth) %|%
  transmute(Overall_Delay = sum(ArrDelay,DepDelay,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,na.rm=T)))

class(overallDelay)
colnames(overallDelay)<-c("DayofMonth","Overall_Delay")
head(overallDelay)

#Visualizing the output using ggplot2 functions
library(ggplot2)
ggplot(overallDelay, aes(x=factor(DayofMonth),y=Overall_Delay)) + geom_bar(stat ="identity")
