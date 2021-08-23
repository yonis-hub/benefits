#dependency
import os 
import json
# import petpy
import urllib
import time
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base

#api key and db password
from config import key, secret, api_key 
import config
from pprint import pprint


# Connecting to the GIPHY API
def buildGiphyUrl(baseUrl, q, limit, offset, rating, lang ):
    
   
    query_url = baseUrl + 'api_key='+ api_key + '&q='+ q + '&limit='+ limit + '&offset=' + offset + '&rating=' + rating + '&lang='+ lang
    # print(query_url)

    return query_url



# giphy api call and save to giphy df 
def giphyResponse(query_url):
    with urllib.request.urlopen(query_url) as response:
        data = json.loads(response.read())
        
        #empty list
        gif_url = []
        slug = []
        gif_id = []
        trending_datetime= []

        #loop and append empty list
        for item in data['data']:
            
            gif_id.append(item['id'])
            slug.append(item['slug'])
            trending_datetime.append(item['trending_datetime'])
            gif_url.append(item['bitly_gif_url'])
        
            #create df for the list items (slug and gif_url)
            giphy_df = pd.DataFrame({'slug':slug,
                                    'gif_url':gif_url,'gif_id':gif_id,
                                    'trending_datetime':trending_datetime})

            #remove all the data with no trending dates by using drop dpulicates ('0000-00-00 00:00:00')
            giphy_df.drop_duplicates(subset ="trending_datetime",
                             keep = False, inplace = True)

            #reset index to gif_url
            giphy_df = giphy_df.set_index('gif_id')

        return giphy_df



# Send Giphy data to stagedDB
def sendToGiphyStage(df,stage_db):
    
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')
        
    # checking tables
    engine.table_names()
    
    try:
        df.to_sql(name=stage_db, con=engine, if_exists="append", index=True)
        #print("Data loaded successfully")
    
    except:
        print("Data has already been loaded to db")


# Filter Giphy data and insert into new DB
def updateGiphy(stage_db,final_db, species):
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    engine.execute("""INSERT INTO """ + final_db + """ 
        SELECT DISTINCT gif_id,slug, gif_url, """ + species + """, trending_datetime
        FROM """ + stage_db +  
        """ ON CONFLICT (gif_id) DO UPDATE SET 
            slug = EXCLUDED.slug, 
            gif_url = EXCLUDED.gif_url,
            trending_datetime = EXCLUDED.trending_datetime""")

#Data check
def getGiphySize(final_db):
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    table_count = engine.execute(
        """Select count(*) 
        FROM """ + final_db).fetchall()
    table_count = table_count[0][0]
    return table_count

#check data entries 
def collectGiphy(url, topic, limit, rating, lang, stage_db, final_db, animal):
    
    preNum = getGiphySize(final_db)

    apiNum = getGiphySize(stage_db)
    offset = 0
    while (getGiphySize(stage_db) - apiNum) < 200 and offset < 5000:

        apiMidNum = getGiphySize(stage_db)
        query_url = buildGiphyUrl(url, topic, limit, str(offset), rating, lang)
        giphy_df = giphyResponse(query_url)
        sendToGiphyStage(giphy_df, stage_db)
        updateGiphy(stage_db, final_db, animal)

        offset += (getGiphySize(stage_db) - apiMidNum)

    postNum = getGiphySize(final_db)
    postStageNum = getGiphySize(stage_db)

    print(str(postStageNum - apiNum) + ' stage entries added.')
    print(str(postNum - preNum) + ' entries added.')



# Connecting to the PETFINDER API 
def petfinderAPI(after, before, animal, dist, resNum, pageNum):
    # initialized
    pf = petpy.Petfinder(key=key, secret=secret)
    
    #params
    after_date = after
    before_date = before
    animal_type = animal
    status = 'adoptable'
    location = 'Miami, FL' 
    distance = dist
    results_per_page = resNum
    pages = pageNum
    
    #api call
    api_res = pf.animals(animal_type=animal_type, status=status, location=location, distance=distance,
                     after_date=after_date, before_date=before_date,
                     results_per_page=results_per_page, pages=pages, return_df=True)
    
    # filtered the data few columns 
    df = api_res[['id','species','age','gender','animal_id','contact.address.city','published_at']]
    #reset index to id
    new_df = df.set_index('id')
    
    #rename columns
    new_df = new_df.rename(columns={'contact.address.city':'city', 'published_at':'date_published'})
    
    return new_df

 #Send to stageing DB 
def sendToAnimalStage(df, dbName):
    #Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    try:
        df.to_sql(name=dbName, con=engine, if_exists="append", index=True)
        print("Data loaded successfully")
    
    except:
        print("Data has already been loaded to db")
   

#Update final db with correct data
def updateAdopt(stageDb, finalDb):
    #Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    # handle the any duplicates from staging db to final dog_db, cat_db 
    engine.execute(
        """INSERT INTO """ + finalDb +
        """ SELECT DISTINCT * FROM """ + stageDb +
        """ ON CONFLICT (id) DO UPDATE SET
            species = EXCLUDED.species,
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            animal_id = EXCLUDED.animal_id,
            city = EXCLUDED.city,
            date_published = EXCLUDED.date_published""")
   


def getDailyStats():
    
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    #check for connection to db and show all table names 
    engine.table_names()
    
    # reflect an existing database 
    Base = automap_base()

    # reflect the tables
    Base.prepare(engine, reflect=True)

    #---------Stats-----------
    stats = engine.execute("""
    SELECT CAST(CAST(date_published AS date) AS varchar) AS trending_post_date, COUNT(species), species, search_type FROM 
(
    SELECT species, date_published, 'adoption' as search_type 
    FROM cat_post_db
    WHERE date_published >= '2021-01-01' AND date_published < '2021/04/18'
    UNION
	
    SELECT species, date_published, 'adoption' as search_type 
    FROM dog_post_db
    WHERE date_published >= '2021-01-01' AND date_published < '2021/04/18'
	
	UNION
	
	SELECT species , trending_datetime as date_published, 'giphy' as search_type
    FROM dog_giphy_db
    WHERE trending_datetime >= '2021-01-01' AND trending_datetime < '2021/04/18'
	
	UNION
	
	SELECT species , trending_datetime as date_published, 'giphy' as search_type
    FROM cat_giphy_db
    WHERE trending_datetime >= '2021-01-01' AND trending_datetime < '2021/04/18'
) AS sub
GROUP BY CAST(date_published AS date), species, search_type
ORDER BY CAST(date_published AS date)
    """).fetchall()


    
    return stats

def outputStats():

   #list of data ranges 
    lst_dt = pd.date_range(start="2021-01-01",end="2021/04/18")

    date_list = []
    cat_post_list = []
    dog_post_list = []
    cat_giphy_list = []
    dog_giphy_list = []

    date_dict = {}

    for d in lst_dt:
        date_list.append(d.date())
        date_dict[str(d.date())] = [0, 0, 0, 0]

    #retrieve data func getDailyStats()
    daily_stats = getDailyStats()

    for column in daily_stats:
        stat_date = str(column[0])
        stat_count = column[1]
        stat_species = column[2]
        stat_search_type = column[3]
        
        if stat_search_type == 'adoption':
            if stat_species == 'Cat':
                date_dict[stat_date][0] = stat_count
            elif stat_species == 'Dog':
                date_dict[stat_date][1] = stat_count
        elif stat_search_type == 'giphy':
            if stat_species == 'Cat':
                date_dict[stat_date][2] = stat_count
            elif stat_species == 'Dog':
                date_dict[stat_date][3] = stat_count

    for d in date_dict.keys():
        counts = date_dict[d]
        cat_post_list.append(counts[0])
        dog_post_list.append(counts[1])
        cat_giphy_list.append(counts[2])
        dog_giphy_list.append(counts[3])
        

    date_list = [str(x) for x in date_list]

    #df containing all the queary data    
    daily_stats_df = pd.DataFrame({'date': date_list,
                            'cat_adoption': cat_post_list,
                            'dog_adoption':dog_post_list,
                            'cat_post':cat_giphy_list,
                            'dog_post':dog_giphy_list
                            })

    daily_stats_df = daily_stats_df.set_index('date') 

    #save to csv file or anyother types
    daily_stats_df.to_csv('stats_output.csv')
    

    




#param control

collectGiphy('http://api.giphy.com/v1/gifs/search?', 'cat', '300', 'g', 'en', 'stage_cat_giphy_db', 'cat_giphy_db', "'Cat'")
collectGiphy('http://api.giphy.com/v1/gifs/search?', 'dog', '300', 'g', 'en', 'stage_dog_giphy_db', 'dog_giphy_db', "'Dog'")

dogDf = petfinderAPI('2021-01-01', '2021-04-18', 'dog', '50', 1, 20)
catDf = petfinderAPI('2021-01-01', '2021-04-18', 'cat', '50', 1, 20)
sendToAnimalStage(catDf, 'stage_cat_db')
sendToAnimalStage(dogDf, 'stage_dog_db')
updateAdopt('stage_dog_db', 'dog_post_db')
updateAdopt('stage_cat_db', 'cat_post_db')

#stats
getDailyStats()

outputStats()




