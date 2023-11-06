import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import csv
import re
import os
import haversine as hs
import ast

doiList = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/PLOSOne_confirmed_locations.csv')
dois = doiList['doi']
mordecai = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/locations_mordecai_2023.csv')
nltk = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/locations_nltk_2023.csv')
spacy_lg = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/locations_spacy-lg_2023.csv')
spacy_trf = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/locations_spacy-trf_2023.csv')
stanza = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/locations_stanza_2023.csv')
arcgispro = pd.read_csv('C:/Users/bgodfrey/Documents/GitHub/Placenames/2023/locations_arcgispro_2023.csv')


# Check coordinate location
#parsers = [mordecai, spacy_lg, spacy_trf, nltk, stanza, arcgispro]
parsers = [arcgispro]
#nltk, stanza, mordecai, spacy-trf, arcgispro have latitude out of range on same arctile (#25)



dict = {'DOI':[],'parser':[],'correctPlace':[],'accurates':[],'inaccurates':[]}

results = pd.DataFrame(dict)

def checkAccuracy(parser, sourceId, sourceLoc, text):

    loc2 = ast.literal_eval(sourceLoc)
    #loc1 = (44.71314,-63.7233)
    
    accurates = 0
    inaccurates = 0
    
    for index, row in parser.iterrows():
        if sourceId == row['doi']:
            if row['parser'] != 'mordecai' and type(row['coordinates']) is not float and "," in row['coordinates']:
                loc1 = ast.literal_eval(row['coordinates'])
                dist = hs.haversine(loc1,loc2)
                if dist > 161:
                    #print(sourceId, index, row['section'], row['parser'], "Distance: "+str(round(dist, 2))+" km, ", "Not Accurate")
                    inaccurates += 1
                else:
                    #print(sourceId, index, row['section'], row['parser'], "Distance: "+str(round(dist, 2))+" km, ", "Accurate")
                    accurates += 1
            elif 'mordecai' in row['parser'] and pd.notnull(row['geo.lat']):
                lat1 = str(row['geo.lat'])
                lon1 = str(row['geo.lon'])
                location = str(lat1)+","+str(lon1)
                loc1 = ast.literal_eval(location)
                dist = hs.haversine(loc1,loc2)
                if dist > 161:
                    #print(sourceId, index, row['section'], row['parser'], "Distance: "+str(round(dist, 2))+" km, ", "Not Accurate")
                    inaccurates += 1
                else:
                    #print(sourceId, index, row['section'], row['parser'], "Distance: "+str(round(dist, 2))+" km, ", "Accurate")
                    accurates += 1
            else:
                pass
        else:
            pass

    print("done")
    results.loc[len(results.index)] = [sourceId, row['parser'], text, accurates, inaccurates]
    print(sourceId, row['parser'], "Accurates: "+str(accurates))
    print(sourceId, row['parser'], "Inaccurates: "+str(inaccurates))


loop = 1

try:
    for parser in parsers:
        for index, row in doiList.iterrows():
            print("Article Row: "+str(loop))
            if pd.notnull(row['doi']) and pd.notnull(row['RE_Lat']):
                sourceId = row['doi']
                lat = row['RE_Lat']
                long = row['RE_Long']
                text = row['Coordinate Text']
                sourceLoc = str(lat)+","+str(long)
                loop +=1
                checkAccuracy(parser, sourceId, sourceLoc, text) 
            else:
                pass
except Exception as e:
    print(e)
    #for any exception to be catched
    print(type(e))
    #to know the type of exception.


results.to_csv('results_20230320.csv', sep=',', encoding='utf8')
