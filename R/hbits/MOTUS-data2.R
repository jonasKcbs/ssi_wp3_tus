
read_motus_data2 <- function (filename)
{
    # read and prepare data
    data <- read.csv(filename, header=TRUE, sep=",")
    colnames(data) <- c("id","guid","respondentflowstate_id","respondentlink_id","geofence_id","posted","created","latitude","longitude","accuracy","speed","type","activity","activity_confidence","data")
    data <- data %>% select(-one_of("id","guid","respondentflowstate_id","respondentlink_id","geofence_id","posted","speed","type","activity","activity_confidence","data"))
    data["timestamp"] <- lapply(data["created"],strptime,format="%Y-%m-%d %H:%M:%S")
    data["timestamp"] <- lapply(data["timestamp"],as.POSIXct)
    data["timestamp"] <- lapply(data["timestamp"],as.numeric)
    data
}