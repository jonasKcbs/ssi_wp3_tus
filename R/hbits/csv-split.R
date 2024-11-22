library(stringr)
library(data.table)

mydata <- read.csv('mts_respondentgeotrackingpoints.csv', header=TRUE, sep=',')

start = 0
prev_day = ""
day = ""
for(i in 1:nrow(mydata))
{
    row = mydata[i,]
    day = str_split(row$created," ")[[1]][1]
    if(prev_day != day)
    {
        if(start != 0)
        {
            filename = paste0('tmp/mts_respondentgeotrackingpoints_',str_split(prev_day," ")[[1]][1],'.csv')
            fwrite(mydata[start:i-1,],filename)
        }
        start = i+1
    }
    prev_day = day
}

filename = paste0('tmp/mts_respondentgeotrackingpoints_',str_split(day," ")[[1]][1],'.csv')
fwrite(mydata[start:nrow(mydata),],filename)
