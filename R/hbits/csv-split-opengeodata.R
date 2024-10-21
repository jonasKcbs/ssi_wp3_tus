library(stringr)
library(data.table)

mydata <- read.csv('ssi_open_geodata.csv', header=TRUE, sep=',')

start = 0
prev_day = ""
for(i in 1:nrow(mydata))
{
    row = mydata[i,]
    respondent = row$user_id
    day = format(as.Date(row$timestamp),"%Y-%m-%d")
    if(prev_day != day)
    {
        if(start != 0)
        {
            filename = paste0('tmp/ssi_open_geodata-',respondent,'-',str_split(prev_day," ")[[1]][1],'.csv')
            fwrite(mydata[start:i-1,],filename)
        }
        start = i+1
    }
    prev_day = day
}
