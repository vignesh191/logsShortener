import json
import itertools
import distance
from itertools import groupby
MARGIN_OF_ERROR = 0.15 #any two strings that return a normalized levenshtein below this are so-called "similar"

final_output = {"eventTimeline": [], "executionId": "16101860340687685", "stageId": "2"}; #dictionary to be returned


logs_arr = json.load(open('logs.json')) #reading in the json logs array
sorted_arr = sorted(logs_arr, key= lambda i: i['Source']) #sorting arr by the "source" attribute

source_buckets = []
for i, group in groupby(sorted_arr, key= lambda i: i['Source']): #making buckets by "source" attribute
    source_buckets.append(list(group))

for bucket in source_buckets: #iterate over the different Sources' logs
    bucket_event_timeline = []
    for idx in range(len(bucket)): #compare each element's Event attribute with each other to determine similarity
        similar = []
        for idx2 in range(len(bucket)): 
            if(distance.levenshtein(bucket[idx]['Event'].split(" "), bucket[idx2]['Event'].split(" "), normalized=True) < MARGIN_OF_ERROR):
                similar.append(bucket[idx2])  #if two Events are similar via levenshtein, then group them in an array

        if len(similar) != 0 and similar not in bucket_event_timeline: #remove duplicates and empty arrays
            similar = sorted(similar, key= lambda i: i['Time']) # order the similar events by time
            bucket_event_timeline.append(similar)

    for event in bucket_event_timeline: #format each event group
        timeline_obj = {      
            "event": event[0]['Event'],
            "source_name": event[0]['Source'],
            "start_timestamp": event[0]['Time'],
            "end_timestamp": event[len(event) - 1]['Time'],
            "count": len(event)
        }
        final_output["eventTimeline"].append(timeline_obj) #add event group to final_output json


final_output["eventTimeline"] = sorted(final_output["eventTimeline"], key= lambda i: i['start_timestamp']) #sort events by time 


with open('output.json', 'w') as fp: #write/export python dictionary to a json file
    json.dump(final_output, fp)


#OVERALL RESULTS: Reduced logs from a length of 209, to 37 groups of logs that are "similar"