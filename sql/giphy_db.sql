
-- initial data gathering and staging
CREATE TABLE stage_cat_giphy_db (
	gif_url VARCHAR (255) , 
	slug VARCHAR (255),
	gif_id VARCHAR (255),
	trending_datetime timestamp
);

CREATE TABLE stage_dog_giphy_db (
	gif_url VARCHAR (255) , 
	slug VARCHAR (255),
	gif_id VARCHAR (255),
	trending_datetime timestamp
);


-- main db giphy 
CREATE TABLE dog_giphy_db (
	gif_id VARCHAR (255) PRIMARY KEY, 
	slug VARCHAR (255),
	gif_url VARCHAR (255),
	species VARCHAR (10),
	trending_datetime timestamp
);
CREATE TABLE cat_giphy_db (
	gif_id VARCHAR (255) PRIMARY KEY, 
	slug VARCHAR (255),
	gif_url VARCHAR (255),
	species VARCHAR (10),
	trending_datetime timestamp
);


--filter on cats and dogs
SELECT DISTINCT(gif_id), gif_url, slug, trending_datetime FROM stage_giphy_db
WHERE slug LIKE '%cat%' 
	OR slug LIKE '%dog';


-- handle the duplicates from staging area to final giphy_db  
INSERT INTO giphy_db 
SELECT DISTINCT(gif_id), 'Dog', gif_url, slug, trending_datetime FROM stage_giphy_db
WHERE slug LIKE '%cat%' OR slug LIKE '%dog'
ON CONFLICT (gif_id) DO UPDATE SET
	slug = EXCLUDED.slug,
	gif_url = EXCLUDED.gif_url,
	trending_datetime = EXCLUDED.trending_datetime
	