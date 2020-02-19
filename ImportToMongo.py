import pandas as pd

## START - read data from aLL Excel Sheets ##
data = pd.concat(pd.read_excel('D:/Docs/BDBA/information Systems/Group Project/Random Data/CityData v2.xlsx'
                      , sheet_name=None
                      , skiprows=1
                      , header=0
                      , skipfooter=1)
                  ,ignore_index = True)#combine data from all excel sheets into single dataFrame
## END - read data from all Excel Sheets ##

fn = lambda row: str(row.CaseID) + '_' + str(row.City).replace(' ','_') + '_' + str(row.Date) # define a function for the new column
col = data.apply(fn, axis=1) # get column data with an index
data = data.assign(custom_cityid=col.values) # assign values to column 'custom_cityid'

citiesDF = data.filter(['custom_cityid'
                          , 'Date'
                          , 'City'
                          , 'CityPopulation'
                          , 'UrbanPopulation(%of total population)'
                          , 'Density(persons/km2)'
                          , 'WasteGenerationrate(kg/person/day)'
                          , 'Avg GDP(US$/person/year)'
                          , 'CO2emission(capita)'
                          , 'Ecologicalfootprint(gha/capita)'
                          , 'LifeExpectancyboth(years)'
                          , 'AdultMortalityrate(probability of dying between the ages of 15 and 60 per 1000 adults)'], axis = 1)

wastemanagementDF = data.filter(['custom_cityid'
                                  , 'Extend of plastic waste separation at the municipality level'
                                  , 'Extend of paper waste separation at the municipality level'
                                  , 'Extend of metal waste separation at the municipality level'
                                  , 'Extend of glass waste separation at the municipality level'
                                  , 'Extend of organic waste separation at the municipality level'
                                  , 'Extend of battery separation at the municipality level'
                                  , 'Extend of medical waste separation at the healthcare centers'
                                  , 'Extend of electric and electronic waste separation at the municipality level'
                                  , 'Extend of waste dispersed in the city'
                                  , 'Extend of waste separation at the house level'
                                  , 'Extend of waste separation at the business level'
                                  , 'Hazardous waste being treated'
                                  , 'Frequency of waste collection at commercial sites (times/week)'
                                  , 'Frequency of waste collection at inner city (times/week)'
                                  , 'Frequency of waste collection at rural areas (times/week)'], axis = 1)

solidwasteDF = data.filter(['custom_cityid'
                               , 'Total Waste generated(KG)'
                               , 'food & organic waste'
                               , 'Metal Waste'
                               , 'Glass Waste'
                               , 'Other Waste'
                               , 'Paper Cardboard Waste'
                               , 'Plastic Waste'
                               , 'Rubber & Leather Waste'
                               , 'Wood Waste'
                               , 'Yard and Garden Green Waste'], axis = 1)

import pymongo
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

# connecting to desired database
# here "WasteManagement" is database name. if db does not exist it will create db automatically
db = client["WasteManagement"]

# connecting to a collection, where our data will be stored.
# if it does not exist. It will create automatically.
collectionCity = db["Cities"]
collectionwastemanagement = db["wastemanagementCollection"]
collectionSolidWaste = db["SolidWaste"]

# To write
collectionCity.delete_many({})  # Destroy the collection
# To avoid repetitions
collectionwastemanagement.delete_many({})
collectionSolidWaste.delete_many({})

collectionCity.insert_many(citiesDF.to_dict('records'))
collectionwastemanagement.insert_many(wastemanagementDF.to_dict('records'))
collectionSolidWaste.insert_many(solidwasteDF.to_dict('records'))
