The open geolocation dataset was collected under the Smart Survey Implementation project 
with project numer 101119594, funded by Eurostat.

The data was collected in the Netherlands in the summer of 2024 by employees of CBS 
(Statistics Netherlands) and students from Utrecht University. Three users 
collected geolocation data using the CBS app on android devices and kept track
of their movement in diaries. One of the users (UtrechtX) collected data using
three different accounts and/or phones sometimes on the same journey (Utrecht1, Utrecht2 and Utrecht12). 

Variables in the dataset:
- user_id: identifier of the user
- timestamp: day, hour, minute and second of the recorded data point (UTC timezone)
- latitude: together with longitude this is the observed coordinate of the data point
- longitude: together with latitude this is the observed coordinate of the data point
- stop: indicator of whether the user was moving (FALSE) or not (TRUE) according to the diary
- mode: classification of the transportation mode according to the diary

- test