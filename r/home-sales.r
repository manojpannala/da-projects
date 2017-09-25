###Data Loading into HDFS

##Setting up environment variables
Sys.setenv("HADOOP_CMD"="/opt/cloudera/parcels/CDH/bin/hadoop")
Sys.setenv("HADOOP_STREAMING"="/opt/cloudera/parcels/CDH-4.7.0-1.cdh4.7.0.p0.40/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.7.0.jar")

##Importing input data directly to HDFS using rhdfs package
library(rhdfs)
hdfs.init() #Initializes rhdfs package
#Creating new folder under root HDFS directory
#You should be using your own HDFS path based on username
hdfs.mkdir("/user/hduser/assignments") 
hdfs.put('/home/data/Class_14/Home_Sales.txt','/user/hduser/assignments/') #Copying file to HDFS
hdfs.ls('/user/hduser/assignments/') #Checking the file details

#Loading the rmr2 and plyrmr packages
library(rmr2)
library(plyrmr)

#Declaring the variable names and respective data types of input data
col.classes = 
  c(city = "character", zip = "numeric", 
    date = "character", price = "numeric", 
    bedrooms = "numeric", squarefeet = "numeric", 
    lotsize = "numeric", builtyear = "numeric", 
    neighbourhood = "character")

#Defining custom input format for input data
data.format =
  make.input.format(
    "csv", 
    sep = ",",
    col.names = names(col.classes),
    colClasses = col.classes)

#Looking at the input data object in HDFS
check<-from.dfs("/user/hduser/assignments/Home_Sales.txt",format=data.format)
mode(check) #list object
mode(check$key) #NULL object
mode(check$val) #list object
head(check$val) #Checking values under val part of input object

###Problem1
##Compute number of rows and columns present in the given dataset
input("/user/hduser/assignments/Home_Sales.txt",format=data.format) %|% dim
input("/user/hduser/assignments/Home_Sales.txt",format=data.format) %|% nrow
input("/user/hduser/assignments/Home_Sales.txt",format=data.format) %|% ncol

##Find out the distribution of homes present across different neighborhoods
count(input("/user/hduser/assignments/Home_Sales.txt",format=data.format),neighbourhood) #works like table function in base R

###Problem2
##Compute quantile values of all the numeric variables present in the dataset like Price, Bedroom, Squarefeet and Lotsize
select(input("/user/hduser/assignments/Home_Sales.txt",format=data.format), price, bedrooms, squarefeet, lotsize) %|% quantile

##Looking at the final output, we can conclude all the variables
##seems to have extreme values present. This needs to be further
##validated from the business whether presence of such variables
##is expected in the data

###Problem3
##Identify the number of missing values present across all the variables in the dataset
missing = function(x) colSums(is.na(x))

gapply(input("/user/hduser/assignments/Home_Sales.txt",format=data.format), 
       missing)

##Perform a missing value treatment on the “neighbourhood” variable 
##mapreduce code
#Defining the map function
#Identify missing row values in neighbourhood and replace them with highest occuring category i.e South of Market
map <- function(k,v) {
  key<-1:nrow(v)
  keyval(key, v)
}

#Defining the reduce function
reduce <- function(k,vv) {
  vv[is.na(vv$neighbourhood),"neighbourhood"]<-"South of Market"
  keyval(k, vv)
}

#Running the mapreduce function
out<-mapreduce(input = "/user/hduser/assignments/Home_Sales.txt",
               input.format = data.format,
               map = map,
               reduce = reduce)

##Reading the outputs from HDFS
homeSalesNew<-data.frame(from.dfs(out))
colnames(homeSalesNew) = 
  c("sno","city","zip","date","price","bedrooms",
    "squarefeet","lotsize","builtyear","neighbourhood")
head(homeSalesNew)
colSums(is.na(homeSalesNew))