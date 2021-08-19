-- initial data gathering and staging
CREATE TABLE stage_cat_db (
	id INT , 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR
);

CREATE TABLE stage_dog_db (
	id INT , 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR
);

-- create cat_db (petfinder api)
CREATE TABLE cat_db (
	id INT PRIMARY KEY, 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR
);


-- create dog_db (petfinder api)
CREATE TABLE dog_db (
	id INT PRIMARY KEY, 
	species VARCHAR(4),
	age VARCHAR(10), 
	gender VARCHAR(6),
	animal_id INT,
	city VARCHAR
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
