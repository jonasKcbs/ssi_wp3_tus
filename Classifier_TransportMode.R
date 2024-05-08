classTM <- function(data, log, directory){
  
  #store start time for logging
  start <- Sys.time()
  
  #select only tracks
  unclass_data <- data[event == "track"]

  ## load OSM data and combine to one dataframe
  BusCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/busCoordinates.csv"), header=FALSE))
  TrainCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/TrainCoordinates.csv"), header=FALSE))
  CarCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/CarCoordinates.csv"), header=FALSE))
  SubwayCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/SubwayCoordinates.csv"), header=FALSE))
  TramCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/TramCoordinates.csv"), header=FALSE))
  BikeCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/BicycleCoordinates.csv"), header=FALSE))
  WalkCoordinates <- setDT(read.csv(paste0(directory, "/OSM data/WalkingCoordinates.csv"), header=FALSE))
  
  TrainCoordinates[, mode := "Train"]
  BusCoordinates[, mode := "Bus"]
  CarCoordinates[, mode := "Car"]
  SubwayCoordinates[, mode := "Subway"]
  TramCoordinates[, mode := "Tram"]
  BikeCoordinates[, mode := "Bike"]
  WalkCoordinates[, mode := "Walk"]
  
  OSMCoordinates <- rbind(TrainCoordinates, BusCoordinates, CarCoordinates, SubwayCoordinates, TramCoordinates, BikeCoordinates, WalkCoordinates)
  names(OSMCoordinates) <- c("id", "lat", "lon", "mode")
  
  rm(TrainCoordinates, BusCoordinates, CarCoordinates, SubwayCoordinates, TramCoordinates, BikeCoordinates, WalkCoordinates)
 
  ## calculate proportion for each possible transportation mode based on OSM
  
  # setting parameters 
  p <- pi/180
  r <- 6371 # radius earth in km
  sq <- 0.0005 # size of bounding box
  
  ## function to calculate the distance to the closest OSM point per mode
  # input is one measurement at the time, with its accuracy
  closestPoint <- function (lat2, lon2, acc) {
    
    #create temporary file with OSM coordinates within bounding box, based on longitude only
    temp <- OSMCoordinates[(lon < lon2 + (sq*acc) & lon > lon2 - (sq*acc))   ,]
    
    ## Calculate distance in meters to OSM in the bounding box
    temp[, distance := 2 * r * asin(sqrt(0.5 - cos((lat2 - lat) * p) / 2 + 
                                                     cos(lat * p) * cos(lat2 * p) * (1 - cos((lon2 - lon) * p)) / 2)) * 1000]
    
    ## Get the minimum distance per mode
    trainDistance <- ifelse(nrow(temp[mode == "Train",]) == 0, NaN, min(temp[mode =="Train", distance], na.rm = TRUE))
    busDistance <- ifelse(nrow(temp[mode == "Bus",]) == 0, NaN, min(temp[mode =="Bus", distance], na.rm = TRUE))
    carDistance <- ifelse(nrow(temp[mode == "Car",]) == 0, NaN, min(temp[mode =="Car", distance], na.rm = TRUE))
    subwayDistance <- ifelse(nrow(temp[mode == "Subway",]) == 0, NaN, min(temp[mode =="Subway", distance], na.rm = TRUE))
    tramDistance <- ifelse(nrow(temp[mode == "Tram",]) == 0, NaN, min(temp[mode =="Tram", distance], na.rm = TRUE))
    bikeDistance <- ifelse(nrow(temp[mode == "Bike",]) == 0, NaN, min(temp[mode =="Bike", distance], na.rm = TRUE))
    walkDistance <- ifelse(nrow(temp[mode == "Walk",]) == 0, NaN, min(temp[mode =="Walk", distance], na.rm = TRUE))
    
    return(c(trainDistance, 
             busDistance,
             carDistance, 
             subwayDistance, 
             tramDistance, 
             bikeDistance, 
             walkDistance))
  }
  
  ## apply the function above to each measurement 
  unclass_data[1:nrow(unclass_data), c("trainDistance", "busDistance", "carDistance", "subwayDistance", "tramDistance", "bikeDistance", "walkDistance") := transpose(lapply(1:nrow(unclass_data), function(i) closestPoint(unclass_data[i, lat], unclass_data[i, lon], unclass_data[i, accuracy])))]
 
  ## check whether the minimum distance per mode was smaller than the accuracy, if so: set boolean to TRUE
  unclass_data[, inBusRad :=  ifelse(busDistance < accuracy, TRUE, FALSE)]
  unclass_data[, inTrainRad :=  ifelse(trainDistance < accuracy, TRUE, FALSE)]
  unclass_data[, inCarRad :=  ifelse(carDistance < accuracy, TRUE, FALSE)]
  unclass_data[, inSubwayRad :=  ifelse(subwayDistance < accuracy, TRUE, FALSE)]
  unclass_data[, inTramRad :=  ifelse(tramDistance < accuracy, TRUE, FALSE)]
  unclass_data[, inBikeRad :=  ifelse(bikeDistance < accuracy, TRUE, FALSE)]
  unclass_data[, inWalkRad :=  ifelse(walkDistance < accuracy, TRUE, FALSE)]
  
  ## for each cluster, calculate the transport mode proportion
  proportion <- unclass_data[, .(nr_obs = .N, 
                                   nr_inBusRad = sum(inBusRad == TRUE), 
                                   nr_inTrainRad = sum(inTrainRad == TRUE), 
                                   nr_inCarRad = sum(inCarRad == TRUE), 
                                   nr_inSubwayRad = sum(inSubwayRad == TRUE), 
                                   nr_inTramRad = sum(inTramRad == TRUE), 
                                   nr_inBikeRad = sum(inBikeRad == TRUE), 
                                   nr_inWalkRad = sum(inWalkRad == TRUE)), by = list(cluster_id, user_id)]
  
  proportion[, Bus := nr_inBusRad/nr_obs]
  proportion[, Train := nr_inTrainRad/nr_obs]
  proportion[, Car := nr_inCarRad/nr_obs]
  proportion[, Subway :=nr_inSubwayRad/nr_obs]
  proportion[, Tram := nr_inTramRad/nr_obs]
  proportion[, Bike := nr_inBikeRad/nr_obs]
  proportion[, Walk := nr_inWalkRad/nr_obs]
  
  ## compare proportions and choose biggest for classification
  #if classification is NaN set to 0
  proportion[, c("Bus", "Train", "Car", "Subway", "Tram", "Bike", "Walk")][is.na( proportion[, c("Bus", "Train", "Car", "Subway", "Tram", "Bike", "Walk")])] <- 0
  
  #Retrieve names of modes with highest proportion
  proportion[, classification:= ifelse((Bus == 0 & Car == 0 & Subway == 0 & Train == 0 & Tram ==0 & Bike == 0 & Walk == 0), "No classification",
                                          apply(proportion[,.(Bus, Car, Subway, Train, Tram, Bike, Walk)], 1,
                                                function(x) names(which(x==max(x, na.rm = TRUE)))))]

  ## clean up dataframe
  classifiedClusters_ext <- proportion[, .(user_id, cluster_id, nr_obs, classification, 
                                           nr_inBusRad, Bus, 
                                           nr_inCarRad, Car, 
                                           nr_inSubwayRad, Subway,
                                           nr_inTrainRad, Train, 
                                           nr_inTramRad, Tram, 
                                           nr_inBikeRad, Bike, 
                                           nr_inWalkRad, Walk)]
  classifiedClusters <- classifiedClusters_ext[, .( user_id, cluster_id, nr_obs, classification)]
  
  ## Create log file
  duration <- difftime(as.POSIXct(Sys.time(), format = "%Y-%m-%d %H:%M:%S"),as.POSIXct(start, format = "%Y-%m-%d %H:%M:%S"), units = "min")
  log_layer <- data.table(input = paste0("number of userpoints = ", nrow(unclass_data), " sq = ", sq, " , lon only"), 
                           duration = as.numeric(duration))
  
  log <- rbind(log, log_layer)
  
  rm(duration)
  
  return(list(log, classifiedClusters_ext, unclass_data))
}