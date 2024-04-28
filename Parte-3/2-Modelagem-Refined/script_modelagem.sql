USE desafio;

START TRANSACTION;

CREATE TABLE dim_genre (
	id INT PRIMARY KEY AUTO_INCREMENT,
    genre VARCHAR(50) NOT NULL,
    num_movies INT
);

CREATE TABLE dim_date (
	id INT PRIMARY KEY AUTO_INCREMENT,
	`date` DATE NOT NULL,
    `year` YEAR NOT NULL,
    `month` INT NOT NULL,
    `day` INT NOT NULL,
    `quarter` CHAR(2) NOT NULL
);

CREATE TABLE dim_movie (
	id VARCHAR(100) PRIMARY KEY,
    main_title VARCHAR(255) NOT NULL,
    original_title VARCHAR(255) NOT NULL,
    release_date DATE NOT NULL,
    genres VARCHAR(255) NOT NULL
);

CREATE TABLE dim_actor (
	id INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    num_movies INT
);

CREATE TABLE fact_movie_actor (
    id_movie VARCHAR(100),
    id_actor INT,
    id_date INT,
    id_genre INT,
    runtime INT NOT NULL,
    revenue INT NOT NULL,
    budget INT NOT NULL,
    actor_role VARCHAR(255),
    popularity DOUBLE NOT NULL,
    imdb_rating DOUBLE NOT NULL,
    tmdb_rating DOUBLE NOT NULL,
    imdb_vote_count INT NOT NULL,
    tmdb_vote_count INT NOT NULL,
    PRIMARY KEY (id_movie, id_actor, id_date, id_genre),
    FOREIGN KEY (id_movie) REFERENCES dim_movie(id),
    FOREIGN KEY (id_actor) REFERENCES dim_actor(id),
    FOREIGN KEY (id_date) REFERENCES dim_date(id),
    FOREIGN KEY (id_genre) REFERENCES dim_genre(id)
);

COMMIT;