  # ATS with cluster
  trajectory <- data.table(lon = data$longitude, 
                           lat = data$latitude, 
                           timestamp = data$timestamp, 
                           accuracy = data$accuracy)
  
  ats_clusters <- ATS_OPTICS(trajectory)
  
  trajectory <- add_cluster_id_to_trajectory(trajectory, ats_clusters)
  
  # add the user id of the specific user (jk)
  ats_clusters$user_id <- ids[i, tmp_id] 
  trajectory$user_id <- ids[i, tmp_id] 
  
  # add the cluster id (jk)
  ats_clusters$cluster_id <- seq(1, nrow(ats_clusters),1)
  
  # add the observation id (jk)
  trajectory$obs_id <- seq(1, nrow(trajectory), 1)