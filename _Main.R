library(data.table)

## set directory
directory <- 
setwd(directory)

## load data: give path to data file
## this data file should contain the clustered data (ATS output)
load("")

## make the selection of the relevant columns
data <- setDT(data)[, c("user_id", "obs_id", "cluster_id", "lon" , "lat" , "timestamp", "accuracy", "event")]

## remove double lat lons
data <- unique(data, by = c("lon", "lat", "user_id", "cluster_id")) 

## create logfile
log <- data.table(input = character(), duration = numeric())

#### classification layer:

#load function
source("Classifier_TransportMode.R")

# run function and store output 
output <- classTM(data = data, 
                  log = log, 
                  directory = directory)
log <- output[[1]]
classTM_clu <- output[[2]] # dataset at cluster level
classTM_trj <- output[[3]] # dataset at measurement level

# provide filename and location in order to save the output
save(classTM_clu, classTM_trj, log, file = "")