library(stringr)
library(data.table)

mydata <- read.csv('tracking_points.csv', header=TRUE, sep=';')

start = 0
prev_day = ""
day_count = 0
filename = "combined.csv"
for(i in 1:nrow(mydata))
{
    if(day_count > 50)
        break
    row = mydata[i,]
    day = str_split(row$created_at," ")[[1]][1]
    if(prev_day != day)
    {
        day_count <- day_count + 1
        if(start != 0)
        {
            fwrite(mydata[start:i-1,],filename,append=TRUE)
        }
        start = i+1
    }
    prev_day = day
}
