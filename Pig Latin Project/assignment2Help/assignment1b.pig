movies = load 'piginput/movies.csv' using PigStorage(',') as (color:chararray,director_name:chararray,num_critic_for_reviews:int,duration:int,director_facebook_likes:int,actor_3_facebook_likes:int,actor_2_name:chararray,actor_1_facebook_likes:int,gross:int,genres:chararray,actor_1_name:chararray,movie_title:chararray,num_voted_users:int,cast_total_facebook_likes:int,actor_3_name:chararray,facenumber_in_poster:int,movie_imdb_link:chararray,num_user_for_reviews:int,language:chararray,country:chararray,content_rating:chararray,budget:long,title_year:int,actor_2_facebook_likes:int,imdb_score:float,aspect_ratio:chararray,movie_facebook_likes:int);
groupedmovies = group movies by director_name; 
moviesavg = foreach groupedmovies generate group, AVG(movies.imdb_score) as imdb_avg;
sortedavg = order moviesavg by imdb_avg DESC;
dump sortedavg;

