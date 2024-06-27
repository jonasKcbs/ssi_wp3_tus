import pandas as pd
import csv
df = pd.read_csv("mts_respondentgeotrackingpoints.csv")
print(df.head())
# "id","guid","respondentflowstate_id","respondentlink_id","geofence_id","posted","created","latitude","longitude","accuracy","speed","type","activity","activity_confidence","data"
# "id";"uuid";"tracker_id";"created_at";"longitude";"latitude";"accuracy";"speed";"confidence";"activity"
df.columns = ['id','uuid','tracker_id','respondentlink_id','geofence_id','posted','created_at','latitude','longitude','accuracy','speed','type','activity','confidence','data']
print(df.head())
df.to_csv("tracking_points.csv",sep=';',index=False, columns=["id","uuid","tracker_id","created_at","longitude","latitude","accuracy","speed","confidence","activity"],quoting=csv.QUOTE_ALL)