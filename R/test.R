library(osmdata)
library(tidyverse)
library(sf)
library(leaflet)
library(leaflet.extras2)
library(sp)
library(data.table)
library(htmltools)

source('MOTUS-data.R')

Sys.setenv("ATS_OPTICS_CPP_FILE"="ATS_OPTICS/ATS_OPTICS.cpp")
source('ATS_OPTICS/ATS_OPTICS.R')

spatial_lines <- function(line_coord) {
  line <- Line(line_coord)
  spatial_lines <- SpatialLines(list(Lines(list(line),'route')))
}

#data <- read_motus_data("combined.csv")

#data <- read_motus_data("tmp/tracking_points_2021-12-30.csv")
#data <- read_motus_data("tmp/tracking_points_2022-01-08.csv")
#data <- read_motus_data("tmp/tracking_points_2021-08-26.csv")

#data <- read_motus_data("tmp/tracking_points_2021-09-26.csv")
#data <- read_motus_data("tmp/tracking_points_2021-08-29.csv")

# read and prepare data
data <- read_motus_data("tmp/tracking_points_2021-09-26.csv")
#data <- read_motus_data("tmp/tracking_points_2021-06-10.csv")
#data <- read_motus_data("tmp/tracking_points_2021-06-11.csv")
#data <- read_motus_data("tmp/tracking_points_2021-06-12.csv")
#data <- read_motus_data("tmp/tracking_points_2021-06-16.csv")
#data <- read_motus_data("tmp/tracking_points_2021-06-22.csv")

# has a 0s track
#data <- read_motus_data("tmp/tracking_points_2021-09-20.csv")

#data <- read_motus_data("tmp/tracking_points_2021-07-01.csv")
#data <- read_motus_data("tmp/tracking_points_2021-06-30.csv")
#data <- read_motus_data("tmp/tracking_points_2021-07-02.csv")

# has also short track (ikea)
#data <- read_motus_data("tmp/tracking_points_2021-08-25.csv")
#data <- read_motus_data("tmp/tracking_points_2021-08-26.csv")
# also here merge possible
#data <- read_motus_data("tmp/tracking_points_2021-08-27.csv")
# lots of zero's
#data <- read_motus_data("tmp/tracking_points_2021-08-28.csv")

# important for starting new cluster when distance between 2 stop points is big
#data <- read_motus_data("tmp/tracking_points_2022-01-15.csv")

# svalbard
#data <- read_motus_data("tmp/tracking_points_2022-06-06.csv")
#data <- read_motus_data("tmp/tracking_points_2022-06-08.csv")
#data <- read_motus_data("tmp/tracking_points_2022-06-09.csv")
#data <- read_motus_data("tmp/tracking_points_2022-06-12.csv")

data <- data %>% filter(accuracy < 100)

print(data)

#
data <- setDT(data)

# ATS
#Rprof()
print("===")
print("ATS")
print("===")
trajectory <- data.table(lon = data$lon, lat = data$lat, timestamp = data$timestamp, accuracy = data$accuracy)
start_time <- Sys.time()
locs = data.table()
locations <- data.table(id = numeric(), lon = numeric(), lat = numeric(), radius = numeric(), metadata = NULL)
ats_clusters <- ATS_OPTICS(trajectory,locations=locations,temporal_threshold_seconds=0,spatial_threshold_meter=50,new_cluster_threshold_meter=500)
trajectory <- add_cluster_id_to_trajectory(trajectory,ats_clusters)
end_time <- Sys.time()
print("performance:")
print(end_time -start_time)
print(ats_clusters)
print(trajectory)
#print(tbl_df(trajectory),n=40)
ats_clusters <- ats_clusters %>% filter(event == 'stop')
#ats_clusters = ats$clusters
#split_points = ats$split_points
ats_contents <- paste0('<strong>',
                'Lon: ', ats_clusters$lon, '<br />',
                'Lat: ', ats_clusters$lat, '<br />',
                'Duration: ', ats_clusters$duration, '<br />',
                'Radius: ', ats_clusters$radius, '<br />',
                'StartTime: ', ats_clusters$time_start, '<br />',
                'EndTime: ', ats_clusters$time_end,
              '</strong>') %>% lapply(htmltools::HTML)

#Rprof(NULL)
#segmented_trajectory <- make_segmented_trajectory(trajectory,180)
#split_points <- get_split_points(segmented_trajectory)

## save first row
first_row = data[1,]

trackingpoints <- data.table(lon = data$lon, lat = data$lat, accuracy = data$accuracy, timestamp = data$timestamp)
contents <- paste0('<strong>',
                'Lon: ', trackingpoints$lon, '<br />',
                'Lat: ', trackingpoints$lat, '<br />',
                'Timestamp: ', trackingpoints$timestamp, '<br />',
                'Accuracy: ', trackingpoints$accuracy,
              '</strong>') %>% lapply(htmltools::HTML)

track <- cbind(trackingpoints$lon,trackingpoints$lat)
f = length(track)/4
track_spatial_lines = spatial_lines(track)

# create map
mymap <- leaflet() %>%
  addTiles() %>%
  addCircleMarkers(data = trackingpoints, radius = 4, color = 'blue', label = contents, group="tracking points") %>%
  addArrowhead(data = track_spatial_lines, color = 'blue', group="tracking points connected", options = arrowheadOptions(
      yawn = 40,
      frequency = f,
      proportionalToTal = TRUE,
    )) %>%
  addCircles(data = ats_clusters, radius = ats_clusters$radius, color='red', label = ats_contents ,group="ATS") %>%
  #addCircleMarkers(data = split_points, radius = 4, color = 'red', group="splits") %>%
  addLayersControl(
    #baseGroups = c("OSM"),
    overlayGroups = c("tracking points", "tracking points connected", "splits", "ATS"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  hideGroup(c("splits")) %>%
  setView(lng = first_row$lon, lat = first_row$lat, zoom = 15)
