# self-Adaptive Trajectory Segmentation in combination with OPTICS
# ATS paper: Individual and collective stop-based adaptive trajectory segmentation

library(dplyr)
library(data.table)
library(geosphere)
library(scales)
library(outliers)
library(tidyverse)
library(dbscan)
library(Rcpp)
library(purrr)


sourceCpp(Sys.getenv(x="ATS_OPTICS_CPP_FILE", unset="src/ATS_OPTICS.cpp"))

calc_SD_bins <- function(SDs)
{
   bins = rep(0,60) # bin of 1 minute, max is 1 hour
   sapply(SDs,function(x){
    if(is.na(x) || x>3600)
        bin = 60
    else {
        bin = 1 + as.integer(x/60)
    }
    bins[bin] <<- bins[bin] + 1
   })
   bins
   #rescale(bins)
}

tau_table = data.frame(
    n = c(seq(3,38),seq(40,50,2),55,60,65,70,80,90,100,200,500,1000,5000,Inf),
    tau = c(1.1511,1.4250,1.5712,1.6563,1.7110,1.7491,1.7770, # 3 -> 9
            1.7984,1.8153,1.8290,1.8403,1.8498,1.8579,1.8649,1.8710,1.8764,1.8811, # 10 -> 19
            1.8853,1.8891,1.8926,1.8957,1.8985,1.9011,1.9035,1.9057,1.9078,1.9096, # 20 -> 29
            1.9114,1.9130,1.9146,1.9160,1.9174,1.9186,1.9198,1.9209,1.9220, # 30 -> 38
            1.9240,1.9257,1.9273,1.9288,1.9301, # 40 -> 48
            1.9314,1.9340, # 50 -> 55
            1.9362,1.9362, # 60 -> 65
            1.9397,1.9423,1.9443, # 70, 80, 90
            1.9459,1.9530,1.9572, # 100, 200, 500
            1.9586,1.9597,1.9600 # 1000, 5000, Inf
            ))

get_tau <- function (sample_size)
{
    indices <- which(tau_table$n >= sample_size)
    data_subset <- tau_table[indices,]
    data.table::first(data_subset)$tau
}

calc_modified_thompson_tau <- function (SDs)
{
    # put in bins
    SD_bins <- calc_SD_bins(SDs)

    res <- c()
    l <- length(SD_bins)
    for(i in 1:50)
    {
        SD_bins_subset <- SD_bins[i:l]
        mean = mean(SD_bins_subset)
        sd = sd(SD_bins_subset)
        #print(c(min,max,mean,sd))
        sample_size = purrr::reduce(SD_bins_subset,sum)
        tau = get_tau(sample_size)
        #print(c(sample_size,tau))
        tau_sd = tau * sd
        #print(c(sample_size, tau_sd))

        delta_1 = abs(SD_bins_subset[1] - mean)
        #print(c(delta_1,tau_sd,mean))
        if(delta_1 > tau_sd)
            res[i] <- 1
        else
            res[i] <- 0
        #print(c(delta_i,tau_sd))
    }
    seconds <- match(0,res)*60
    #list(SD_bins=SD_bins,temporal_threshold_seconds=match(0,res)*60)
}

splitpoints_to_stopclusters <- function (trajectory, split_points, temporal_threshold_seconds, spatial_threshold_meter, new_cluster_threshold_meter, new_cluster_threshold_seconds, minPts=1, eps_cl=0.01)
{
    col_lon <- trajectory$lon
    col_lat <- trajectory$lat
    col_timestamp <- trajectory$timestamp

    nrows = nrow(split_points)
    if(nrows == 0)
    {
        return(data.table())
    }
    else if(nrows == 1)
    {
        return(make_cluster(col_lon,col_lat,col_timestamp,split_points[1,]$index,split_points[1,]$index,'stop',temporal_threshold_seconds,spatial_threshold_meter))
    }

    x <- cbind(x = split_points$lon, y = split_points$lat)
    res <- dbscan::optics(x, minPts = minPts)
    res <- dbscan::extractDBSCAN(res, eps_cl = eps_cl)

    clusters = data.table()
    cur_c = -1
    timestamp = 0
    start_index = 0
    cur_index = 0

    d_prev2 = split_points$d_prev2
    t_prev2 = split_points$t_prev2
    indices = split_points$index
    timestamps = split_points$timestamp
    for (i in 1:nrows)
    {
        # new cluster?
        c = res$cluster[i]
        # start new cluster when:
        # - cluster index is different, or
        # - distance difference is too big between 2 stop points
        if(c != cur_c || d_prev2[i] > new_cluster_threshold_meter || t_prev2[i] > new_cluster_threshold_seconds)
        {
            if(cur_index != 0)
            {
                clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,start_index,cur_index,'stop',temporal_threshold_seconds,spatial_threshold_meter))
            }
            start_index = indices[i]
            cur_c = c
        }

        cur_index = indices[i]
        timestamp = timestamps[i]
    }

    clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,start_index,cur_index,'stop',temporal_threshold_seconds,spatial_threshold_meter))
}

add_tracks <- function(trajectory,stop_clusters,temporal_threshold_seconds,spatial_threshold_meter,new_cluster_threshold_seconds)
{
    col_lon <- trajectory$lon
    col_lat <- trajectory$lat
    col_timestamp <- trajectory$timestamp

    clusters = data.table()

    prev_index_start = 0
    prev_index_end = 0
    indices_start = stop_clusters$index_start
    indices_end = stop_clusters$index_end
    for(i in 1:nrow(stop_clusters))
    {
        index_start = indices_start[i]
        index_end = indices_end[i]

        if(!prev_index_end)
        {
            if(index_start != 1)
            {
                # start with a track if there are tracking points before the first stop cluster
                clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,1,index_start,'track',temporal_threshold_seconds,spatial_threshold_meter,1,index_start-1))
            }
        }
        else
        {
            # 2 possibilities depending on time between 2 stops:
            # - almost no time in between -> merge stop clusters
            #   (regardless of inbetween tracking points, and also OPTICS sometimes makes different stop cluster because of density)
            # - else create a track in between the 2 stop clusters _if_
            #   - tracking points available, or
            #   - no tracking points but relatively short time between the stops

            t_end_prev = trajectory[prev_index_end,]$timestamp
            t_start = trajectory[index_start,]$timestamp

            if( t_start-t_end_prev > temporal_threshold_seconds)
            {
                # add track
                if(prev_index_end+1 == index_start)
                {
                    if(t_start-t_end_prev < new_cluster_threshold_seconds)
                    {
                        # track has no tracking points
                        real_start_index = -1
                        real_end_index = -1
                        clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,prev_index_end,index_start,'track',temporal_threshold_seconds,spatial_threshold_meter,real_start_index,real_end_index))
                    }
                }
                else
                {
                    # track has tracking points
                    real_start_index = prev_index_end + 1
                    real_end_index = index_start - 1
                    clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,prev_index_end,index_start,'track',temporal_threshold_seconds,spatial_threshold_meter,real_start_index,real_end_index))
                }
            }
            else
            {
                # merge 2 stop clusters by removing the previous cluster and move the index_start to the start of the previous cluster
                clusters = head(clusters,-1)
                index_start = prev_index_start
            }
        }
        clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,index_start,index_end,'stop',temporal_threshold_seconds,spatial_threshold_meter))

        prev_index_start = index_start
        prev_index_end = index_end
    }

    # there are tracking points after the last stop cluster => make track cluster
    if(nrow(trajectory) > prev_index_end)
    {
        clusters <- rbind(clusters,make_cluster(col_lon,col_lat,col_timestamp,prev_index_end,nrow(trajectory),'track',temporal_threshold_seconds,spatial_threshold_meter,prev_index_end+1,nrow(trajectory)))
    }

    clusters
}

ATS_OPTICS <- function (trajectory,locations,temporal_threshold_seconds=180,spatial_threshold_meter=50,new_cluster_threshold_meter=500,new_cluster_threshold_seconds=86400)
{
    print(paste("Temporal threshold (s):              ",temporal_threshold_seconds))
    print(paste("Spatial  threshold (s):              ",spatial_threshold_meter))
    print(paste("New cluster threshold (meter):",new_cluster_threshold_meter))
    print(paste("New cluster threshold (secs): ",new_cluster_threshold_seconds))
    trajectory <- tibble::rowid_to_column(trajectory,'index')

    col_lon <- trajectory$lon
    col_lat <- trajectory$lat
    col_timestamp <- trajectory$timestamp

    # calculate distances: 'd' at index i is distance between i and i+1
    start_time <- Sys.time()
    trajectory = cbind(trajectory,d_prev=calc_distances(col_lon,col_lat))
    end_time <- Sys.time()
    print(paste("Performance trajectory distances:",difftime(end_time,start_time,units="secs"),"secs"))

    # calculate time diffs
    start_time <- Sys.time()
    trajectory = cbind(trajectory,data.table(t_prev=calc_time_diffs(col_timestamp)))
    end_time <- Sys.time()
    print(paste("Performance trajectory timediffs:",difftime(end_time,start_time,units="secs"),"secs"))

    # calculate SDs -> inside a location is always regarded as an infinite stop
    start_time <- Sys.time()
    col_loc_lon <- locations$lon
    col_loc_lat <- locations$lat
    col_loc_radius <- locations$radius
    SDs <- calc_SDs(col_lon,col_lat,col_timestamp,spatial_threshold_meter,col_loc_lon,col_loc_lat,col_loc_radius)
    end_time <- Sys.time()
    print(paste("Performance SDs:",difftime(end_time,start_time,units="secs"),"secs"))

    if(temporal_threshold_seconds==0)
    {
        start_time <- Sys.time()
        temporal_threshold_seconds <- calc_modified_thompson_tau(SDs)
        end_time <- Sys.time()
        print(paste("Using temporal threshold:",temporal_threshold_seconds,"sec"))
        print(paste("Performance modified Thompson tau:",difftime(end_time,start_time,units="secs"),"secs"))
    }

    start_time <- Sys.time()
    split_point_indices <- ats_get_split_points(col_lon,col_lat,col_timestamp,SDs,temporal_threshold_seconds,spatial_threshold_meter)
    if(length(split_point_indices))
    {
        split_points <- data.table()
        split_points <- rbindlist(lapply(split_point_indices, function(index) {trajectory[index,]}))
        split_points = cbind(split_points,
                                data.table(d_prev2=calc_distances_split_points(trajectory$d_prev,as.numeric(split_point_indices))),
                                data.table(t_prev2=calc_time_diffs(split_points$timestamp)))
        end_time <- Sys.time()
        print(paste("Performance split points with diffs:",difftime(end_time,start_time,units="secs"),"secs"))

        start_time <- Sys.time()
        stop_clusters <- splitpoints_to_stopclusters(trajectory,split_points,temporal_threshold_seconds,spatial_threshold_meter,new_cluster_threshold_meter,new_cluster_threshold_seconds)
        end_time <- Sys.time()
        print(paste("Performance stop clusters:",difftime(end_time,start_time,units="secs"),"secs"))
    }
    else
    {
        stop_clusters <- data.table()
    }

    start_time <- Sys.time()
    clusters <- add_tracks(trajectory,stop_clusters,temporal_threshold_seconds,spatial_threshold_meter,new_cluster_threshold_seconds)
    end_time <- Sys.time()
    print(paste("Performance With tracks:",difftime(end_time,start_time,units="secs"),"secs"))

    cluster_id = seq(1,nrow(clusters))
    clusters <- cbind(cluster_id, clusters)

    list(split_points=split_points,clusters=clusters)
}

add_cluster_id_to_trajectory <- function(trajectory, ats_clusters)
{
    cluster_id = c()
    indices_start = ats_clusters$index_start
    indices_end = ats_clusters$index_end
    for(i in 1:nrow(ats_clusters))
    {
        index_start <- indices_start[i]
        index_end <- indices_end[i]
        if(index_start != -1 && index_end != -1)
        {
            for(j in index_start:index_end)
            {
                cluster_id <- append(cluster_id,i)
            }
        }
    }
    trajectory <- cbind(cluster_id, trajectory)
}
