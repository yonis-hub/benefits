#dependency
import os 
import json
import petpy
import urllib
import time
import pandas as pd
import matplotlib.pyplot as plt

#giphy api
import giphy_client
from giphy_client.rest import ApiException

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
def get_trending_giphy(l, o):
    #params
    api_instance = giphy_client.DefaultApi()
    api_key = config.api_key 
    limit = l
    #increase offset by 100 each run 
    offset = o
    
    
    #empty list
    gif_url = []
    slug = []
    gif_id = []
    trending_datetime= []

    try: 
        # trending endpoint
        api_response = api_instance.gifs_trending_get(api_key, limit=limit)
        #pprint(api_response.data)

        api_res = api_response.data

        # iterate over the api response 
        for item in api_res:
            #print(item)

            gif_id.append(item.id)
            slug.append(item.slug)
            trending_datetime.append(item.trending_datetime)
            gif_url.append(item.bitly_gif_url)
            

    except ApiException as e:
        print("Exception when calling DefaultApi->gifs_search_get: %s\n" % e)
        
    
    
    #create df for the list items (slug and gif_url)
    giphy_df = pd.DataFrame({'slug':slug,
                            'gif_url':gif_url,'gif_id':gif_id,
                            'trending_datetime':trending_datetime})
    
    #removing duplicate times with value of '0000-00-00 00:00:00'
    giphy_df.drop_duplicates(subset ="trending_datetime",
                     keep = False, inplace = True)
    
    #reset index to gif_url
    giphy_df = giphy_df.set_index('gif_id')

    return giphy_df

#Send Giphy to DB
def sendToGiphyStage(df):
    
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')
        
    # checking tables
    engine.table_names()
    
    try:
        df.to_sql(name="stage_giphy_db", con=engine, if_exists="append", index=True)
        #print("Data loaded successfully")
    
    except:
        print("Data has already been loaded to db")

# Filter Giphy data and insert into new DB
def updateGiphy():
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    engine.execute("""INSERT INTO giphy_db 
        SELECT DISTINCT gif_id, gif_url, slug, trending_datetime
        FROM stage_giphy_db 
        WHERE slug LIKE '%%cat%%' OR slug LIKE '%%dog%%' AND slug NOT LIKE '%%dogg%%' 
        ON CONFLICT (gif_id) DO UPDATE SET 
            slug = EXCLUDED.slug, 
            gif_url = EXCLUDED.gif_url, 
            trending_datetime = EXCLUDED.trending_datetime
    """)

#Data check
def getGiphySize():
    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    table_count = engine.execute(
        """Select count(*) 
        FROM giphy_db
        """).fetchall()
    table_count = table_count[0][0]
    return table_count

#while loop until the 
# off = 0
# while getGiphySize() < 200 and off < 5000:
    print("giphy_db size: " + str(getGiphySize()))
    print("offset: " + str(off))
    df = get_trending_giphy(100, off)
    sendToGiphyStage(df)
    updateGiphy()
    off += 100

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

    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    try:
        df.to_sql(name=dbName, con=engine, if_exists="append", index=True)
        print("Data loaded successfully")
    
    except:
        print("Data has already been loaded to db")
    return print("Data loaded successfully")

#Update final db with correct data
def updateAdopt(stageDb, finalDb):
    # Create a SQL Database connection
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
    
    # print("Data Inserted successfully")
    return 



def makeCharts():

    # Create a SQL Database connection
    connection_string = f"postgres:{config.password}@localhost:5432/pets_db"
    engine = create_engine(f'postgresql://{connection_string}')

    #check for connection to db and show all table names 
    engine.table_names()
    
    # reflect an existing database 
    Base = automap_base()

    # reflect the tables
    Base.prepare(engine, reflect=True)

    #view all of the tables with pkey 
    Base.classes.keys()
    
    # Save references to each table in the db
    cat_db = Base.classes.cat_db
    dog_db = Base.classes.dog_db
    giphy_db = Base.classes.giphy_db
    
    # Create(link) btw Python and DB
    session = Session(engine)
    
    #Inspector to explore the database and print the table names
    inspector = inspect(engine)
    inspector.get_table_names()
    
    # Use Inspector to print the column names and types
    cat_table = inspector.get_columns('cat_db')

    dog_table = inspector.get_columns('dog_db')

    giphy_table = inspector.get_columns('giphy_db')


     #---------Cat adoption stats-----------
    cat_adoption = engine.execute("""SELECT COUNT(species) as daily_vol, 
    cast(cast(date_published as date) as varchar) 
    FROM cat_db 
    Group by cast(date_published as date)
    """).fetchall()


    cat_adop_dates = [x[1] for x in cat_adoption]
    cat_adop_count = [x[0] for x in cat_adoption]

    pprint(f' Dates for CAT Adop {cat_adop_dates}')
    pprint(f' Count CAT Adop {cat_adop_count}')

 #---------Dog adoption stats-----------

    dog_adoption = engine.execute("""SELECT COUNT(species) as daily_vol, 
    cast(cast(date_published as date) as varchar) 
    FROM cat_db 
    Group by cast(date_published as date)
    """).fetchall()
    #list comprehension
    dog_adop_dates = [x[1] for x in dog_adoption]
    dog_adop_count = [x[0] for x in dog_adoption]

    pprint(f' Dates for DOG Adop {dog_adop_dates}')
    pprint(f' Count DOG Adop {dog_adop_count}')


 #---------giphy trends stats-----------
    
    # pet_trends = engine.execute("""
    # SELECT COUNT(*) as daily_vol, 
    # cast(cast(trending_datetime as date) as varchar) 
    # FROM giphy_db 
    # Group by cast(trending_datetime as date)
    # """).fetchall()

    # #list comprehension
    # giphy_adop_dates = [x[1] for x in pet_trends]
    # giphy_adop_count = [x[0] for x in pet_trends]

#     print(giphy_adop_dates)
#     print(giphy_adop_count)
    
    ###----------------Make Graphs----------
    
    # cat_adoption = pd.DataFrame({'Cat Adoption': cat_adop_count,
    #                          'Date Trending': cat_adop_dates})
    
    # dog_adoption = pd.DataFrame({'Dog Adoption': dog_adop_count,
    #                      'Date Trending': dog_adop_dates})
    
    # pet_post = pd.DataFrame({'Pet Post': giphy_adop_count,
    #                          'Date Trending': giphy_adop_dates})
    






#param control
# dogDf = petfinderAPI('2021-01-01', '2021-04-18', 'dog', '50', 1, 20)
# catDf = petfinderAPI('2021-01-01', '2021-04-18', 'cat', '50', 1, 20)
# sendToAnimalStage(catDf, 'stage_cat_db')
# sendToAnimalStage(dogDf, 'stage_dog_db')
# updateAdopt('stage_dog_db', 'dog_db')
# updateAdopt('stage_cat_db', 'cat_db')
makeCharts()
