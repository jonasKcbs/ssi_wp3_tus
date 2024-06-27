library(stringr)
library(data.table)

mydata <- read.csv('tracking_points.csv', header=TRUE, sep=';')

start = 0
prev_day = ""
for(i in 1:nrow(mydata))
{
    row = mydata[i,]
    day = str_split(row$created_at," ")[[1]][1]
    if(prev_day != day)
    {
        if(start != 0)
        {
            filename = paste0('tmp/tracking_points_',str_split(prev_day," ")[[1]][1],'.csv')
            fwrite(mydata[start:i-1,],filename)
        }
        start = i+1
    }
    prev_day = day
}
