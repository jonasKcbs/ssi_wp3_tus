library(data.table)
library(dplyr)
library(argparse)

library(ATSOPTICS)

read_csv_geopoints <- function (filename)
{
    # read and prepare data
    data <- read.csv(filename, header=TRUE, sep=",")
    colnames(data) <- c("id","t","lon","lat","acc","metadata")
    data
}

read_csv_geolocations <- function (filename)
{
    # read and prepare data
    data <- read.csv(filename, header=TRUE, sep=",")
    colnames(data) <- c("id","lon","lat","radius","metadata")
    data
}

write_csv_data <- function (filename, data, row_names=FALSE)
{
    write.csv(x=data, file=filename, row.names=row_names)
}

main <- function ()
{
    parser <- ArgumentParser(description='ATS OPTICS algorithm')
    parser$add_argument('path_geopoints',   type="character", help='path to geopoints file (input)')
    parser$add_argument('path_geolocations',type="character", help='path to geolocations file (input)')
    parser$add_argument('path_geoclusters', type="character", help='path to cluster file (output)')
    parser$add_argument('path_geomapping',  type="character", help='path to mapping file (output)')
    parser$add_argument('-a', type="integer", dest="acc",                   default=100,    help='minimal accuray in meters (preprocessing)')
    parser$add_argument('-t', type="integer", dest="temporal_threshold",    default=180,    help='temporal threshold in seconds (ATS), use 0 for Thompson Tau')
    parser$add_argument('-s', type="integer", dest="spatial_threshold",     default=50,     help='spatial threshold in meter (ATS)')
    parser$add_argument('-n', type="integer", dest="new_cluster_meter",           default=500,    help='start new cluster threshold in meters (postprocessing)')
    parser$add_argument('-m', type="integer", dest="new_cluster_seconds",           default=86400,  help='start new cluster threshold in seconds (postprocessing)')
    args <- parser$parse_args(commandArgs(trailingOnly=TRUE)) 

    path_geopoints <- args$path_geopoints
    path_geolocations <- args$path_geolocations
    path_clusters <- args$path_geoclusters
    path_mapping <- args$path_geomapping

    temporal_threshold_seconds <- args$temporal_threshold
    spatial_threshold_meter <- args$spatial_threshold
    accuracy <- args$acc
    new_cluster_threshold_meter <- args$new_cluster_meter
    new_cluster_threshold_seconds <- args$new_cluster_seconds

    # read and prepare data
    geopoints <- read_csv_geopoints(path_geopoints)
    geolocations <- read_csv_geolocations(path_geolocations)
    # filter data on accuracy
    geopoints <- geopoints %>% filter(acc < accuracy)

    ## ATS
    trajectory <- data.table(timestamp = geopoints$t, lon = geopoints$lon, lat = geopoints$lat)
    locations <- data.table(lon = geolocations$lon, lat = geolocations$lat, radius = geolocations$radius)
    ats <- ATS_OPTICS(trajectory, locations,
        temporal_threshold_seconds=temporal_threshold_seconds,
        spatial_threshold_meter=spatial_threshold_meter,
        new_cluster_threshold_meter=new_cluster_threshold_meter,
	new_cluster_threshold_seconds=new_cluster_threshold_seconds)
    ats_clusters <- ats$clusters
    split_points <- ats$split_points
    print(ats_clusters)
    write_csv_data(path_clusters, ats_clusters)

    mapping <- add_cluster_id_to_trajectory(geopoints,ats_clusters)
    write_csv_data(path_mapping, mapping)
    #print(ats_clusters)
    #print(mapping)

}

main()
