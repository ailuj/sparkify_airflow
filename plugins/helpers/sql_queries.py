class SqlQueries:
    songplay_table_insert = ("""CREATE TABLE IF NOT EXISTS public.songplays(
                                songplay_id varchar(35) not null, 
                                start_time timestamp not null, 
                                user_id int not null, 
                                level varchar(50), 
                                song_id varchar(50), 
                                artist_id varchar(50), 
                                session_id int, 
                                location varchar(200), 
                                user_agent varchar(200),
                                CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id));
                            INSERT INTO public.songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id,                                  location, user_agent)
                                SELECT
                                    md5(events.sessionid || events.start_time) songplay_id,
                                    events.start_time, 
                                    events.userid, 
                                    events.level, 
                                    songs.song_id, 
                                    songs.artist_id, 
                                    events.sessionid, 
                                    events.location, 
                                    events.useragent
                                    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                                FROM staging_events
                                WHERE page='NextSong') events
                                LEFT JOIN staging_songs songs
                                ON events.song = songs.title
                                    AND events.artist = songs.artist_name
                                    AND events.length = songs.duration""")

    user_table_insert = ("""CREATE TABLE IF NOT EXISTS public.users(
                                user_id int NOT NULL, 
                                first_name varchar(200), 
                                last_name varchar(200), 
                                gender varchar(50), 
                                level varchar(50),
                                CONSTRAINT users_pkey PRIMARY KEY (user_id));
                            INSERT INTO public.users (user_id, first_name, last_name, gender, level)
                                SELECT distinct userid, 
                                        firstname, 
                                        lastname, 
                                        gender, 
                                        level
                                FROM staging_events
                                WHERE page='NextSong'""")

    song_table_insert = ("""CREATE TABLE IF NOT EXISTS public.songs(
                                song_id varchar(50) NOT NULL, 
                                title varchar(256), 
                                artist_id varchar(50) NOT NULL, 
                                year int, 
                                duration numeric(18,0),
                                CONSTRAINT songs_pkey PRIMARY KEY (song_id));
                            INSERT INTO public.songs (song_id, title, artist_id, year, duration)
                                SELECT distinct song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration
                                FROM staging_songs""")

    artist_table_insert = ("""CREATE TABLE IF NOT EXISTS public.artists(
                                artist_id varchar(50) NOT NULL, 
                                name varchar(256), 
                                location varchar(256), 
                                latitude numeric(18,0), 
                                longitude numeric(18,0),
                                CONSTRAINT artists_pkey PRIMARY KEY (artist_id));
                              INSERT INTO public.artists (artist_id, name, location, latitude, longitude)
                                SELECT distinct artist_id, 
                                        artist_name, 
                                        artist_location, 
                                        artist_latitude, 
                                        artist_longitude
                                FROM staging_songs""")

    time_table_insert = ("""CREATE TABLE IF NOT EXISTS public.time(
                                start_time timestamp NOT NULL, 
                                hour int, 
                                day int, 
                                week int, 
                                month int, 
                                year int, 
                                weekday int,
                                CONSTRAINT times_pkey PRIMARY KEY (start_time));
                            INSERT INTO public.time (start_time, hour, day, week, month, year, weekday)
                                SELECT start_time, 
                                        extract(hour from start_time), 
                                        extract(day from start_time), 
                                        extract(week from start_time), 
                                        extract(month from start_time), 
                                        extract(year from start_time), 
                                        extract(dayofweek from start_time)
                                FROM songplays""")
    
    staging_events_create = ("""CREATE TABLE IF NOT EXISTS public.staging_events (
                        artist varchar(256),
                        auth varchar(50),
                        firstname varchar(200),
                        gender varchar(50),
                        iteminsession int4,
                        lastname varchar(200),
                        length numeric(18,0),
                        level varchar(50),
                        location varchar(200),
                        method varchar(50),
                        page varchar(50),
                        registration numeric(18,0),
                        sessionid int4,
                        song varchar(256),
                        status int4,
                        ts int8,
                        useragent varchar(256),
                        userid int4
                    );""")
    
    staging_songs_create = ("""CREATE TABLE IF NOT EXISTS public.staging_songs (
                            num_songs int4,
                            artist_id varchar(100),
                            artist_name varchar(256),
                            artist_latitude numeric(18,0),
                            artist_longitude numeric(18,0),
                            artist_location varchar(256),
                            song_id varchar(100),
                            title varchar(256),
                            duration numeric(18,0),
                            year int4
                        );
    """)