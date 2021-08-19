-- initial data gathering and staging
create table stage_cat_db (
	id int , 
	species varchar(4),
	age varchar(10), 
	gender varchar(6),
	animal_id int,
	city varchar
);

create table stage_dog_db (
	id int , 
	species varchar(4),
	age varchar(10), 
	gender varchar(6),
	animal_id int,
	city varchar
);


select * from stage_cat_db;


-- create cat_db (petfinder api)
create table cat_db (
	id int primary key, 
	species varchar(4),
	age varchar(10), 
	gender varchar(6),
	animal_id int,
	city varchar
);


-- create dog_db (petfinder api)
create table dog_db (
	id int primary key, 
	species varchar(4),
	age varchar(10), 
	gender varchar(6),
	animal_id int,
	city varchar
);

-- handle the duplicates from staging area to final cat_db  
INSERT INTO cat_db 
SELECT DISTINCT * FROM stage_cat_db
ON CONFLICT (id) DO UPDATE SET
	species = EXCLUDED.species,
	age = EXCLUDED.age,
	gender = EXCLUDED.gender,
	animal_id = EXCLUDED.animal_id,
	city = EXCLUDED.city

-- handle the duplicates from staging area to final dog_db  
INSERT INTO dog_db 
SELECT DISTINCT * FROM stage_dog_db
ON CONFLICT (id) DO UPDATE SET
	species = EXCLUDED.species,
	age = EXCLUDED.age,
	gender = EXCLUDED.gender,
	animal_id = EXCLUDED.animal_id,
	city = EXCLUDED.city



-- INSERT INTO dog_db 
-- SELECT DISTINCT * FROM new_dog_df
-- ON CONFLICT (id) DO UPDATE SET
-- 	species = EXCLUDED.species,
-- 	age = EXCLUDED.age,
-- 	gender = EXCLUDED.gender,
-- 	animal_id = EXCLUDED.animal_id,
-- 	city = EXCLUDED.city
