DROP TABLE IF EXISTS cwei.movie;

CREATE TABLE cwei.movie(
    user_id STRING,
    movie_id STRING,
    rating FLOAT,
    timestamp STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "movie" INTO TABLE cwei.movie;


WITH movie_ids AS (SELECT movie_id FROM cwei.movie
WHERE user_id = '244' and rating > 3),    
similar_users AS (
SELECT user_id FROM cwei.movie as a, movie_ids as b
WHERE b.movie_id = a.movie_id and rating > 3 LIMIT 10) 
SELECT movie_id FROM cwei.movie a, similar_users b
WHERE a.user_id = b.user_id AND rating > 3 AND a.user_id <> '244'  
LIMIT 10; 
