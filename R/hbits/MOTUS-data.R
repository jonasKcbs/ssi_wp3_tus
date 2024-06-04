
read_motus_data <- function (filename)
{
    # read and prepare data
    data <- read.csv(filename, header=TRUE, sep=",")
    colnames(data) <- c("id","uuid","tracker_id","timestamp","lon","lat","accuracy","speed","confidence","activity")
    data <- data %>% select(-one_of('id','uuid','tracker_id','speed','confidence','activity'))
    data["timestamp"] <- lapply(data["timestamp"],strptime,format="%Y-%m-%d %H:%M:%S")
    data["timestamp"] <- lapply(data["timestamp"],as.POSIXct)
    data["timestamp"] <- lapply(data["timestamp"],as.POSIXct)
    data["timestamp"] <- lapply(data["timestamp"],as.numeric)
    data
}