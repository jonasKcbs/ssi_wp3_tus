library(data.table)

source('ATS_OPTICS.R')

read_csv_data <- function (filename)
{
    # read and prepare data
    data <- read.csv(filename, header=TRUE, sep=",")
    colnames(data) <- c("id","t","lon","lat","acc","metadata")
    data
}

write_csv_data <- function (filename, data, row_names=FALSE)
{
    write.csv(x=data, file=filename, row.names=row_names)
}

main <- function ()
{
    args = commandArgs(trailingOnly=TRUE)
    if (length(args)!=4) {
        stop("Usage: process.R <IN_geopoints.csv> <IN_geolocations.csv> <OUT_clusters.csv> <OUT_geopoints_with_cluster_id>", call.=FALSE)
    }
    path_geopoints = args[1]
    path_geolocations = args[2]
    path_clusters = args[3]
    path_mapping = args[4]

    # read and prepare data
    data <- read_csv_data(path_geopoints)

    print(data)

    # filter data on accuracy
    data <- data %>% filter(acc < 100) # TODO parameter

    ## ATS
    trajectory <- data.table(timestamp = data$t, lon = data$lon, lat = data$lat)
    ats_clusters <- ATS_OPTICS(trajectory,temporal_threshold_seconds=180,spatial_threshold_meter=50,new_cluster_threshold_meter=500) # TODO parameters
    print(ats_clusters)
    write_csv_data(path_clusters, ats_clusters)

    mapping <- add_cluster_id_to_trajectory(data,ats_clusters)
    write_csv_data(path_mapping, mapping)
    #print(ats_clusters)
    #print(mapping)

}

main()
