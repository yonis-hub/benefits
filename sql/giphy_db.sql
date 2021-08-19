-- initial data gathering and staging
create table stage_giphy_db (
	gif_url varchar (255) , 
	slug varchar (255)
);

-- main db giphy 
create table giphy_db (
	gif_url varchar (255) primary key, 
	slug varchar (255)
);


--filter on cats
select distinct(slug), gif_url from stage_giphy_db
where slug like '%cat%'

--filter on dogs
select slug, gif_url from stage_giphy_db
where slug like '%dog%'


-- handle the duplicates from staging area to final cat_db  
INSERT INTO giphy_db 
SELECT DISTINCT * FROM stage_giphy_db
ON CONFLICT (gif_url) DO UPDATE SET
	slug = EXCLUDED.slug,
	