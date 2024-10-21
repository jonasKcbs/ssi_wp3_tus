
read_ssi_open_geodata <- function (filename)
{
    # read and prepare data
    data <- read.csv(filename, header=TRUE, sep=",")
    colnames(data) <- c("user_id","timestamp","latitude","longitude","stop","mode")
    data <- data %>% select(-one_of("stop","mode"))
    data["timestamp"] <- lapply(data["timestamp"],strptime,format="%Y-%m-%dT%H:%M:%SZ")
    data["timestamp"] <- lapply(data["timestamp"],as.POSIXct)
    data["timestamp"] <- lapply(data["timestamp"],as.numeric)
    data
}