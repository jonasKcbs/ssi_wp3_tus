# Applying the ats optics for clustering track/stops
# this will add the variable label_ats_startstop with an indication of 
# start/stop classification by the ats optics algorithm

library(tidyverse)
library(data.table)

Sys.setenv("ATS_OPTICS_CPP_FILE"="../../ssi_wp3_tus/R/ATS_OPTICS/ATS_OPTICS.cpp")

source("../../ssi_wp3_tus/R/ATS_OPTICS/ATS_OPTICS.R")

locs <- fread("../../ssi_wp3_tus/open_geo_data/ssi_open_geodata.csv")  # ssi repository
# locs <- fread("../data/locs_UU_anonymized_raw.csv")  # internap repository

# prepare lists to catch the results of the ats clustering
ats_clusters_list <- c()
split_points_list <- c()
trajectory_list <- c()

for(user_id_tmp in unique(locs$user_id)){ 
  print(user_id_tmp)
  
  # select user
  locs_user <- locs %>%
    filter(user_id == user_id_tmp)
  
  for(date_tmp in unique(date(locs_user$timestamp))){
    
    # select data for a single day
    locs_user_day <- locs_user %>%
      filter(date(timestamp) == date_tmp) %>%
      arrange(timestamp)  # order the data chronologically
    
    # prepare data and apply ATS OPTICS
    data <- setDT(locs_user_day)
    trajectory_tmp <- data.table(lon = data$longitude, lat = data$latitude, timestamp = data$timestamp)#, accuracy = data$accuracy)
    start_time <- Sys.time()
    locations <- data.table(id = numeric(), lon = numeric(), lat = numeric(), radius = numeric(), metadata = NULL)
    ats <- ATS_OPTICS(trajectory_tmp,locations=locations,temporal_threshold_seconds=180,spatial_threshold_meter=50,new_cluster_threshold_meter=500)
    
    # put data in format for analysis
    ats_clusters_tmp <- ats$clusters %>%
      mutate(user_id = user_id_tmp,# add user_id
             date = date_tmp)  # add date
    
    split_points_tmp <- ats$split_points %>%
      mutate(user_id = user_id_tmp,# add user_id
             date = date_tmp)  # add date
    
    trajectory_tmp <- trajectory_tmp %>%
      add_cluster_id_to_trajectory(ats_clusters_tmp) %>%
      mutate(user_id = user_id_tmp,# add user_id
             date = date_tmp) %>%  # add date
      left_join(ats_clusters_tmp[, c("cluster_id", "event")], by="cluster_id") %>%  # add cluster_id
      rename(label_ats_startstop = event)
    
    # add data for this user to the lists
    ats_clusters_list <- append(ats_clusters_list, list(ats_clusters_tmp))
    split_points_list <- append(split_points_list, list(split_points_tmp))
    trajectory_list <- append(trajectory_list, list(trajectory_tmp))
    
    # print time 
    end_time <- Sys.time()
    print(end_time-start_time)
    
  }
}

# merge the lists into one dataframe
ats_clusters <- rbindlist(ats_clusters_list)
split_points <- rbindlist(split_points_list)
trajectory <- rbindlist(trajectory_list)
