-- initial data gathering and staging
CREATE TABLE stage_cat_db (
	id INT , 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR,
	date_published timestamp
);

CREATE TABLE stage_dog_db (
	id INT , 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR,
	date_published timestamp
);

-- create cat_db (petfinder api)
CREATE TABLE cat_post_db (
	id INT PRIMARY KEY, 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR,
	date_published timestamp
);


-- create dog_db (petfinder api)
CREATE TABLE dog_post_db (
	id INT PRIMARY KEY, 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR,
	date_published timestamp
);

-- handle the duplicates from staging area to final cat_db  
INSERT INTO cat_post_db 
SELECT DISTINCT * FROM stage_cat_db
ON CONFLICT (id) DO UPDATE SET
	species = EXCLUDED.species,
	age = EXCLUDED.age,
	gender = EXCLUDED.gender,
	animal_id = EXCLUDED.animal_id,
	city = EXCLUDED.city,
	date_published = EXCLUDED.date_published

-- handle the duplicates from staging area to final dog_db  
INSERT INTO dog_post_db 
SELECT DISTINCT * FROM stage_dog_db
ON CONFLICT (id) DO UPDATE SET
	species = EXCLUDED.species,
	age = EXCLUDED.age,
	gender = EXCLUDED.gender,
	animal_id = EXCLUDED.animal_id,
	city = EXCLUDED.city,
	date_published = EXCLUDED.date_published

-- query for select dog adoption data
SELECT COUNT(species) as daily_vol, 
    cast(cast(date_published as date) as varchar) 
    FROM dog_post_db 
	WHERE 
		date_published >= '2021-01-01' AND date_published < '2021/04/18'
    Group by cast(date_published as date)

-- query for select cat adoption data
SELECT COUNT(species) as daily_vol, 
    cast(cast(date_published as date) as varchar) 
    FROM cat_post_db 
	WHERE 
		date_published >= '2021-01-01' AND date_published < '2021/04/18'
    Group by cast(date_published as date)





--union all the datas 
SELECT  CAST(CAST(date_published AS date) AS varchar) AS trending_post_date, COUNT(species), species, search_type
FROM 
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


--giphy db
select * from stage_cat_giphy_db
select * from stage_dog_giphy_db
select * from dog_giphy_db
select * from cat_giphy_db
--petfinder db
select * from stage_cat_db
select * from stage_dog_db
select * from dog_post_db
select * from cat_post_db