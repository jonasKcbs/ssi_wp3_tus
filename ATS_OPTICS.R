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

sourceCpp('ATS.cpp')

# algorithm parameters
spatial_threshold_meter = 50
temporal_threshold_seconds = FALSE
SDs = c()

calc_SDs <- function(trajectory)
{
    col_lon <- trajectory$lon
    col_lat <- trajectory$lat
    col_timestamp <- as.numeric(trajectory$timestamp)
    l = list()
    for(i in 1:nrow(trajectory))
    {
        l <- append(l,list(SD(col_lon,col_lat,col_timestamp,i,spatial_threshold_meter)))
    }
    as.numeric(l)
}

calc_SD_bins <- function()
{
   bins = rep(0,60) # bin of 1 minute, max is 1 hour
   sapply(SDs,function(x){
    if(x==Inf || x>3600)
        bin = 60
    else {
        bin = 1 + as.integer(x/60)
    }
    bins[bin] <<- bins[bin] + 1
   })
   bins
   #rescale(bins)
}

# S = p_s,... p_e
segmented_trajectory_get_e <- function (trajectory,s,temporal_threshold_seconds)
{
    n = nrow(trajectory)
    j = s
    sd_j = SDs[j]
    while(j <= n && sd_j != Inf && sd_j <= temporal_threshold_seconds)
    {
        j = j + 1
        sd_j = SDs[j]
    }
    j
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
    first(data_subset)$tau
}

calc_modified_thompson_tau <- function ()
{
    # put in bins
    SD_bins <- calc_SD_bins()

    res <- c()
    l <- length(SD_bins)
    for(i in 1:50)
    {
        SD_bins_subset <- SD_bins[i:l]
        mean = mean(SD_bins_subset)
        sd = sd(SD_bins_subset)
        #print(c(min,max,mean,sd))
        sample_size = reduce(SD_bins_subset,sum)
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
    list(SD_bins=SD_bins,temporal_threshold_seconds=match(0,res)*60)
}

make_segmented_trajectory <- function (trajectory, temporal_threshold_seconds=FALSE, do_plot=FALSE)
{
    # some preprocessing
    SDs <<- calc_SDs(trajectory)

    if (temporal_threshold_seconds == FALSE)
    {
        print("adaptive ATS")
        res <- calc_modified_thompson_tau()
        temporal_threshold_seconds <<- res$temporal_threshold_seconds
        if (do_plot)
        {
            plot(res$SD_bins, type="l")
            abline(v=temporal_threshold_seconds/60, col="blue")
        }
    }
    
    #print(paste("temporal threshold in seconds:",temporal_threshold_seconds))

    segmented_trajectory = list()
    n = nrow(trajectory)
    i = 1
    while(i <= n)
    {
        e = segmented_trajectory_get_e(trajectory,i,temporal_threshold_seconds)
        segmented_trajectory <- append(segmented_trajectory, list(trajectory[i:e,]))
        i = e + 1
    }
    segmented_trajectory
}

get_split_points <- function (segmented_trajectory)
{
    l = lapply(segmented_trajectory,function(segment){
        tail(segment, n=1)
    })
    rbindlist(l)
}

radius <- function (cluster, lon_mean, lat_mean)
{
    dmax = 0
    for(i in 1:nrow(cluster)) {
        d = haversine(cluster[i,][['lat']],cluster[i,][['lon']], lat_mean, lon_mean)
        if(d > dmax)
            dmax = d
    }
    # always add the spatial_threshold_meter as a minimum
    # e.g. if not, in case of 1 stop point the radius would be zero
    dmax + spatial_threshold_meter/2
}

time_adjust <- function (t,t_next)
{
    # if going from stop -> track then phone wakeup might take some time
    # take x minutes that the user was already in transport
    in_transport_seconds = 5*60

    delta_t = calc_time_diff(t_next,t)
    if(delta_t > temporal_threshold_seconds)
    {
        if(t + temporal_threshold_seconds > t_next - in_transport_seconds)
        {
            t = t + temporal_threshold_seconds
        }
        else
        {
            t = t_next - in_transport_seconds
        }
    }
    else
    {
        t = t_next
    }
    t
}

# for a stop: add temporal_threshold_seconds at the end
# for a track: add temporal_threshold_seconds at the front
make_cluster <- function(trajectory,start_index,stop_index,event)
{
    df = trajectory[start_index:stop_index,]
    lon_mean = mean(df$lon)
    lat_mean = mean(df$lat)

    n = nrow(trajectory)

    # time_start calculation
    if(event == 'stop')
    {
        time_start = trajectory[start_index,]$timestamp

        t_end = trajectory[stop_index,]$timestamp
        if(stop_index < n)
        {
            t_next = trajectory[stop_index+1,]$timestamp
            time_end = time_adjust(t_end,t_next)
        }
        else
        {
            # if last index then add temporal threshold to the stop (min. stop cluster duration)
            time_end = t_end + temporal_threshold_seconds
        }
    }
    else if(event == 'track')
    {
        t_start = trajectory[start_index,]$timestamp
        t_next = trajectory[start_index+1,]$timestamp
        time_start = time_adjust(t_start,t_next)

        time_end = trajectory[stop_index,]$timestamp
    }

    data.frame('index_start'=start_index,
                'index_end'=stop_index,
                'lon'=c(lon_mean),
                'lat'=c(lat_mean),
                'time_start'=time_start,
                'time_end'=time_end,
                'duration'=c(calc_time_diff(time_end,time_start)),
                'radius'=c(radius(df,lon_mean,lat_mean)),
                'event'=event)
}

calc_distances_split_points <- function (trajectory, split_points)
{
    distances = list()
    n = nrow(split_points)
    i = 2
    distances[1] = 0
    while(i <= n)
    {
        prev_sp = split_points[i-1,]
        sp = split_points[i,]
        distances[i] = sum(as.numeric(trajectory[(1+prev_sp$index):sp$index,]$d_prev))
        i = i + 1
    }
    as.numeric(distances)
}

merge_with_stdbscan <- function (split_points)
{
    source('stdbscan.R')
    res = stdbscan(split_points$lon,split_points$lat,split_points$timestamp,50,180,1)
    clusters = data.table()
    cur_c = 0
    timestamp = 0
    start_index = 0
    cur_index = 0
    for (i in 1:nrow(split_points))
    {
        sp = split_points[i,]

        # new cluster?
        c = res$cluster[i]
        if(c != cur_c)
        {
            if(cur_index != 0)
            {
                clusters <- rbind(clusters,make_cluster(trajectory,start_index,cur_index,'stop'))
            }
            start_index = sp$index
            cur_c = c
        }

        cur_index = sp$index
        timestamp = sp$timestamp
    }

    clusters <- rbind(clusters,make_cluster(trajectory,start_index,cur_index,'stop'))
}

splitpoints_to_stopclusters <- function (trajectory, split_points, minPts=1, eps_cl=0.01)
{
    nrows = nrow(split_points)
    if(nrows == 0)
    {
        return(data.table())
    }
    else if(nrows == 1)
    {
        return(make_cluster(trajectory,1,1,'stop'))
    }

    x <- cbind(x = split_points$lon, y = split_points$lat)
    res <- dbscan::optics(x, minPts = minPts)
    res <- dbscan::extractDBSCAN(res, eps_cl = eps_cl)

    clusters = data.table()
    cur_c = -1
    timestamp = 0
    start_index = 0
    cur_index = 0
    for (i in 1:nrows)
    {
        sp = split_points[i,]

        # new cluster?
        c = res$cluster[i]
        # start new cluster when:
        # - cluster index is different, or
        # - distance difference is too big between 2 stop points
        if(c != cur_c || sp$d_prev2 > 500)
        {
            if(cur_index != 0)
            {
                clusters <- rbind(clusters,make_cluster(trajectory,start_index,cur_index,'stop'))
            }
            start_index = sp$index
            cur_c = c
        }

        cur_index = sp$index
        timestamp = sp$timestamp
    }

    clusters <- rbind(clusters,make_cluster(trajectory,start_index,cur_index,'stop'))
}

add_tracks <- function(trajectory,stop_clusters)
{
    clusters = data.table()
    prev_index_start = 0
    prev_index_end = 0
    for(i in 1:nrow(stop_clusters))
    {
        index_start = stop_clusters[i,]$index_start
        index_end = stop_clusters[i,]$index_end

        if(!prev_index_end)
        {
            if(index_start != 1)
            {
                # start with a track if there are tracking points before the first stop cluster
                clusters <- rbind(clusters,make_cluster(trajectory,1,index_start,'track'))
            }
        }
        else
        {
            # 2 possibilities depending on time between 2 stops:
            # - almost no time in between -> merge stop clusters
            #   (regardless of inbetween tracking points, and also OPTICS sometimes makes different stop cluster because of density)
            # - else create a track in between the 2 stop clusters

            t_end_prev = trajectory[prev_index_end,]$timestamp
            t_start = trajectory[index_start,]$timestamp

            if(calc_time_diff(t_start,t_end_prev) > temporal_threshold_seconds)
            {
                # add track
                clusters <- rbind(clusters,make_cluster(trajectory,prev_index_end,index_start,'track'))
            }
            else
            {
                # merge 2 stop clusters by removing the previous cluster and move the index_start to the start of the previous cluster
                clusters = head(clusters,-1)
                index_start = prev_index_start
            }
        }
        clusters <- rbind(clusters,make_cluster(trajectory,index_start,index_end,'stop'))

        prev_index_start = index_start
        prev_index_end = index_end
    }

    # there are tracking points after the last stop cluster => make track cluster
    if(nrow(trajectory) > prev_index_end)
    {
        clusters <- rbind(clusters,make_cluster(trajectory,prev_index_end+1,nrow(trajectory),'track'))
    }

    clusters
}

ATS_OPTICS <- function (trajectory,temporal_threshold_seconds=180,spatial_threshold_meter=50)
{
    spatial_threshold_meter <<- spatial_threshold_meter
    temporal_threshold_seconds <<- temporal_threshold_seconds
    trajectory <- tibble::rowid_to_column(trajectory,'index')

    # calculate distances: 'd' at index i is distance between i and i+1
    start_time <- Sys.time()
    col_lon <- trajectory$lon
    col_lat <- trajectory$lat
    trajectory = cbind(trajectory,d_prev=calc_distances(col_lon,col_lat))
    end_time <- Sys.time()
    print(paste("Trajectory distances:",end_time-start_time))

    start_time <- Sys.time()
    trajectory = cbind(trajectory,data.table(t_prev=calc_time_diffs(trajectory$timestamp)))
    end_time <- Sys.time()
    print(paste("Trajectory timediffs:",end_time-start_time))

    start_time <- Sys.time()
    segmented_trajectory <- make_segmented_trajectory(trajectory,temporal_threshold_seconds)
    end_time <- Sys.time()
    print(paste("Segmented trajectory:",end_time-start_time))

    start_time <- Sys.time()
    split_points <- get_split_points(segmented_trajectory)
    split_points = cbind(split_points,
                            data.table(d_prev2=calc_distances_split_points(trajectory,split_points)),
                            data.table(t_prev2=calc_time_diffs(split_points$timestamp)))
    end_time <- Sys.time()
    print(paste("Split points with diffs:",end_time-start_time))

    start_time <- Sys.time()
    stop_clusters <- splitpoints_to_stopclusters(trajectory,split_points)
    end_time <- Sys.time()
    print(paste("Stop clusters:",end_time-start_time))

    start_time <- Sys.time()
    clusters <- add_tracks(trajectory,stop_clusters)
    end_time <- Sys.time()
    print(paste("With tracks:",end_time-start_time))

    clusters
    #list(split_points=split_points,clusters=clusters)
}
