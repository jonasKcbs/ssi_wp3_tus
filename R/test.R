library(osmdata)
library(tidyverse)
library(sf)
library(leaflet)
library(leaflet.extras2)
library(sp)
library(data.table)
library(htmltools)
library(uuid)

options(width=200)
date = "2024-08-17"


Sys.setenv("ATS_OPTICS_CPP_FILE"="ATS_OPTICS/ATS_OPTICS.cpp")
source('ATS_OPTICS/ATS_OPTICS.R')

save_trips <- function(ats_clusters,trajectory) {
    indices_start = ats_clusters$index_start
    indices_end = ats_clusters$index_end
    events = ats_clusters$event
    if(nrow(ats_clusters) < 3)
      return;
    for(i in 1:(nrow(ats_clusters)-2))
    {
        event <- events[i]
        # track cluster must have tracking points
        if(event == "stop" && indices_start[i+1] != -1 && indices_end[i+1] != -1) {
          start = indices_start[i]
          end = indices_end[i+2]
          write.csv(trajectory[start:end],paste0("trips/",date,"-",i,".csv"))
        }
    }
}

spatial_lines <- function(line_coord) {
  line <- Line(line_coord)
  spatial_lines <- SpatialLines(list(Lines(list(line),'route')))
}

source('hbits/MOTUS-data2.R')
#data <- read_motus_data2(paste0("hbits/tmp/mts_respondentgeotrackingpoints_",date,".csv"))
#data <- data %>% filter(accuracy < 100)

source('hbits/ssi_open_geodata.R')
data <- read_ssi_open_geodata("hbits/tmp/ssi_open_geodata-Utrecht2-2024-06-08.csv")

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
ats <- ATS_OPTICS(trajectory,locations=locations,temporal_threshold_seconds=180,spatial_threshold_meter=50,new_cluster_threshold_meter=500)
ats_clusters <- ats$clusters
split_points <- ats$split_points
trajectory <- add_cluster_id_to_trajectory(trajectory,ats_clusters)
#save_trips(ats_clusters,trajectory)
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
  addCircleMarkers(data = split_points, radius = 4, color = 'red', group="splits") %>%
  addLayersControl(
    #baseGroups = c("OSM"),
    overlayGroups = c("tracking points", "tracking points connected", "splits", "ATS"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  hideGroup(c("splits")) %>%
  setView(lng = first_row$lon, lat = first_row$lat, zoom = 15)
