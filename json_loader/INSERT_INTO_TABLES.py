import json
import psycopg

root_database_name = "project_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

conn = psycopg.connect(dbname=root_database_name, user=db_username, password=db_password, host=db_host, port=db_port)

# Create a cursor
cur = conn.cursor()

#reset database
cur.execute(""" DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            """)

#create managers table
cur.execute(""" CREATE TABLE managers (
                manager_id INT,
                name VARCHAR(100),
                nickname VARCHAR(100),
                dob DATE,
                country VARCHAR(75),
                PRIMARY KEY (manager_id)
            )
            """)
with open("dataset\managers.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for manager in data:
        keys = manager.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [manager[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO managers ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create stadium table
cur.execute(""" CREATE TABLE stadiums (
                stadium_id INT,
                name VARCHAR(100),
                country VARCHAR(75),
                PRIMARY KEY (stadium_id)
            )
            """)
with open("dataset\stadiums.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for stadium in data:
        keys = stadium.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [stadium[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO stadiums ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create referee table
cur.execute(""" CREATE TABLE referees (
                referee_id INT,
                name VARCHAR(100),
                country VARCHAR(75),
                PRIMARY KEY (referee_id)
            )
            """)
with open("dataset\\referees.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for referee in data:
        keys = referee.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [referee[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO referees ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create competition table
cur.execute(""" CREATE TABLE competitions (
                competition_id INT,
                season_id INT,
                competition_name VARCHAR(100),
                season_name VARCHAR(100),
                country VARCHAR(75),
                gender VARCHAR(10),
                youth BOOLEAN,
                international BOOLEAN,
                PRIMARY KEY (competition_id, season_id)
            )
            """)
with open("dataset\competitions.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for competition in data:
        keys = competition.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [competition[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO competitions ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create teams table
cur.execute(""" CREATE TABLE teams (
                team_id INT,
                team_name VARCHAR(50),
                team_gender VARCHAR(10),
                country VARCHAR(75),
                PRIMARY KEY (team_id)
            )
            """)
with open("dataset\\teams.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for lineup in data:
        keys = lineup.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [lineup[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO teams ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create players table
cur.execute(""" CREATE TABLE players (
                player_id INT,
                player_name VARCHAR(100),
                player_nickname VARCHAR(100),
                country VARCHAR(75),
                PRIMARY KEY (player_id)
            )
            """)
with open("dataset\players.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for player in data:
        keys = player.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [player[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO players ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create matches table
cur.execute(""" CREATE TABLE matches (
                match_id INT,
                competition_id INT,
                season_id INT,
                stadium_id INT,
                referee_id INT,
                home_team_id INT,
                away_team_id INT,
                match_date DATE,
                kick_off TIME,
                home_score INT,
                away_score INT,
                match_week INT,
                competition_stage VARCHAR(100),
            
                PRIMARY KEY (match_id),
                FOREIGN KEY (competition_id, season_id)
                    REFERENCES competitions(competition_id, season_id),
                FOREIGN KEY (stadium_id)
                    REFERENCES stadiums(stadium_id),
                FOREIGN KEY (referee_id)
                    REFERENCES referees(referee_id),
                FOREIGN KEY (home_team_id)
                    REFERENCES teams(team_id),
                FOREIGN KEY (away_team_id)
                    REFERENCES teams(team_id)
            )
            """)
with open("dataset\matches.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        keys = match.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [match[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO matches ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create team_managers table
cur.execute(""" CREATE TABLE team_managers (
                manager_id INT,
                match_id INT,
                managing_home_team BOOLEAN,
            
                FOREIGN KEY (manager_id)
                    REFERENCES managers(manager_id),
                FOREIGN KEY (match_id)
                    REFERENCES matches(match_id)
            )
            """)
with open("dataset\\team_managers.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for team_manager in data:
        keys = team_manager.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [team_manager[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO team_managers ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create played_in table
cur.execute(""" CREATE TABLE played_in (
                player_id INT,
                team_id INT,
                match_id INT,
                jersey_number INT,
            
                FOREIGN KEY (player_id)
                    REFERENCES players(player_id),
                FOREIGN KEY (match_id)
                    REFERENCES matches(match_id),
                FOREIGN KEY (team_id)
                    REFERENCES teams(team_id)
            )
            """)
with open("dataset\played_in.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for participation in data:
        keys = participation.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [participation[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO played_in ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create card table
#time_issued is in mm:ss, and not a time. VARCHAR is used instead
cur.execute(""" CREATE TABLE cards (
                player_id INT,
                team_id INT,
                match_id INT,
                card_id INT,
                period INT,
                reason VARCHAR(30),
                card_type VARCHAR(20),
                time_issued VARCHAR(6),

                PRIMARY KEY (card_id),
                FOREIGN KEY (player_id)
                    REFERENCES players(player_id),
                FOREIGN KEY (match_id)
                    REFERENCES matches(match_id),
                FOREIGN KEY (team_id)
                    REFERENCES teams(team_id)
            )
            """)
with open("dataset\cards.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for card in data:
        keys = card.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [card[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO cards ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create position table
cur.execute(""" CREATE TABLE positions (
                player_id INT,
                team_id INT,
                match_id INT,
                position_id INT,
                from_time VARCHAR(8),
                to_time VARCHAR(8),
                from_period INT,
                to_period INT,
                start_reason VARCHAR(60),
                end_reason VARCHAR(60),
                position VARCHAR(30),

                PRIMARY KEY (position_id),
                FOREIGN KEY (player_id)
                    REFERENCES players(player_id),
                FOREIGN KEY (match_id)
                    REFERENCES matches(match_id),
                FOREIGN KEY (team_id)
                    REFERENCES teams(team_id)
            )
            """)
with open("dataset\positions.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for position in data:
        keys = position.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [position[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO positions ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)


#create events table
cur.execute(""" CREATE TABLE events (
                event_id VARCHAR(50),
                possession_team_id INT,
                event_team_id INT,
                match_id INT,
                type VARCHAR(20),
                index INT,
                minute INT,
                second INT,
                period INT,
                timestamp TIME,
                possession INT,
                play_pattern VARCHAR(50),

                PRIMARY KEY (event_id),
                FOREIGN KEY (possession_team_id)
                    REFERENCES teams(team_id),
                FOREIGN KEY (event_team_id)
                    REFERENCES teams(team_id),
                FOREIGN KEY (match_id)
                    REFERENCES matches(match_id)
            )
            """)
with open("dataset\events.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO events ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)


#create starting XI table
cur.execute(""" CREATE TABLE starting_xi (
                event_id VARCHAR(50),
                formation INT,
                player_id_1 INT,
                player_id_2 INT,
                player_id_3 INT,
                player_id_4 INT,
                player_id_5 INT,
                player_id_6 INT,
                player_id_7 INT,
                player_id_8 INT,
                player_id_9 INT,
                player_id_10 INT,
                player_id_11 INT,
            
                player_jersey_number_1 INT,
                player_jersey_number_2 INT,
                player_jersey_number_3 INT,
                player_jersey_number_4 INT,
                player_jersey_number_5 INT,
                player_jersey_number_6 INT,
                player_jersey_number_7 INT,
                player_jersey_number_8 INT,
                player_jersey_number_9 INT,
                player_jersey_number_10 INT,
                player_jersey_number_11 INT,
            
                player_position_1 VARCHAR(40),
                player_position_2 VARCHAR(40),
                player_position_3 VARCHAR(40),
                player_position_4 VARCHAR(40),
                player_position_5 VARCHAR(40),
                player_position_6 VARCHAR(40),
                player_position_7 VARCHAR(40),
                player_position_8 VARCHAR(40),
                player_position_9 VARCHAR(40),
                player_position_10 VARCHAR(40),
                player_position_11 VARCHAR(40),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (player_id_1)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_2)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_3)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_4)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_5)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_6)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_7)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_8)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_9)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_10)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_11)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Starting XI.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO starting_xi ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create tactical shift table
cur.execute(""" CREATE TABLE tactical_shift (
                event_id VARCHAR(50),
                formation INT,
                player_id_1 INT,
                player_id_2 INT,
                player_id_3 INT,
                player_id_4 INT,
                player_id_5 INT,
                player_id_6 INT,
                player_id_7 INT,
                player_id_8 INT,
                player_id_9 INT,
                player_id_10 INT,
                player_id_11 INT,
            
                player_jersey_number_1 INT,
                player_jersey_number_2 INT,
                player_jersey_number_3 INT,
                player_jersey_number_4 INT,
                player_jersey_number_5 INT,
                player_jersey_number_6 INT,
                player_jersey_number_7 INT,
                player_jersey_number_8 INT,
                player_jersey_number_9 INT,
                player_jersey_number_10 INT,
                player_jersey_number_11 INT,
            
                player_position_1 VARCHAR(40),
                player_position_2 VARCHAR(40),
                player_position_3 VARCHAR(40),
                player_position_4 VARCHAR(40),
                player_position_5 VARCHAR(40),
                player_position_6 VARCHAR(40),
                player_position_7 VARCHAR(40),
                player_position_8 VARCHAR(40),
                player_position_9 VARCHAR(40),
                player_position_10 VARCHAR(40),
                player_position_11 VARCHAR(40),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (player_id_1)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_2)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_3)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_4)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_5)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_6)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_7)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_8)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_9)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_10)
                    REFERENCES players(player_id),
                FOREIGN KEY (player_id_11)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Tactical Shift.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO tactical_shift ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create referee ball-drop table
cur.execute(""" CREATE TABLE referee_ball_drop (
                event_id VARCHAR(50),
                location_x FLOAT4,
                location_y FLOAT4,
                off_camera BOOLEAN,
                
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
            )
            """)
with open("dataset\sorted_events\Referee Ball-Drop.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO referee_ball_drop ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create half_start table
cur.execute(""" CREATE TABLE half_start (
                event_id VARCHAR(50),
                duration FLOAT4,
                late_video_start BOOLEAN,
                
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
            )
            """)
with open("dataset\sorted_events\Half Start.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO half_start ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create half_end table
cur.execute(""" CREATE TABLE half_end (
                event_id VARCHAR(50),
                under_pressure BOOLEAN,
                
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
            )
            """)
with open("dataset\sorted_events\Half End.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO half_end ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create shot table
cur.execute(""" CREATE TABLE shots (
                event_id VARCHAR(50),
                initiating_player INT,
                key_pass_id VARCHAR(50),
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                under_pressure BOOLEAN,
                out BOOLEAN,
                off_camera BOOLEAN,
                end_location_x FLOAT4,
                end_location_y FLOAT4,
                technique VARCHAR(30),
                aerial_won BOOLEAN,
                deflected BOOLEAN,
                xg_score FLOAT8,
                outcome VARCHAR(20),
                body_part VARCHAR(30),
                type VARCHAR(20),
                one_on_one BOOLEAN,
                saved_off_target BOOLEAN,
                first_time BOOLEAN,
                saved_to_post BOOLEAN,
                redirect BOOLEAN,
                open_goal BOOLEAN,
                follows_dribble BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (key_pass_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Shot.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO shots ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create pass table
cur.execute(""" CREATE TABLE passes (
                event_id VARCHAR(50),
                initiating_player INT,
                assisted_shot_event_id VARCHAR(50),
                receiving_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                under_pressure BOOLEAN,
                counterpress BOOLEAN,
                out BOOLEAN,
                off_camera BOOLEAN,
                length FLOAT4,
                height VARCHAR(20),
                angle FLOAT4,
                end_location_x FLOAT4,
                end_location_y FLOAT4,
                through_ball BOOLEAN,
                shot_assist BOOLEAN,
                inswinging BOOLEAN,
                outswinging BOOLEAN,
                technique VARCHAR(30),
                aerial_won BOOLEAN,
                miscommunication BOOLEAN,
                cut_back BOOLEAN,
                no_touch BOOLEAN,
                deflected BOOLEAN,
                outcome VARCHAR(20),
                switch BOOLEAN,
                straight BOOLEAN,
                body_part VARCHAR(30),
                crossing BOOLEAN,
                type VARCHAR(20),
                goal_assist BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (assisted_shot_event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id),
                FOREIGN KEY (receiving_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Pass.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO passes ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create substitution table
cur.execute(""" CREATE TABLE substitutions (
                event_id VARCHAR(50),
                initiating_player INT,
                replacement_player INT,
                position VARCHAR(30),
                off_camera BOOLEAN,
                outcome VARCHAR(20),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id),
                FOREIGN KEY (replacement_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Substitution.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO substitutions ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create error table
cur.execute(""" CREATE TABLE errors (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                off_camera BOOLEAN,
                location_x FLOAT4,
                location_y FLOAT4,
                under_pressure BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Error.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO errors ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create fifty_fifty table
cur.execute(""" CREATE TABLE fifty_fifties (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                counterpress BOOLEAN,
                out BOOLEAN,
                under_pressure BOOLEAN,
                outcome VARCHAR(50),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Fifty-Fifty.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO fifty_fifties ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create dribble table
cur.execute(""" CREATE TABLE dribbles (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                out BOOLEAN,
                overrun BOOLEAN,
                no_touch BOOLEAN,
                nutmeg BOOLEAN,
                completed BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Dribble.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO dribbles ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create foul_commited table
cur.execute(""" CREATE TABLE fouls_committed (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                counterpress BOOLEAN,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                type VARCHAR(30),
                offensive BOOLEAN,
                card_type VARCHAR(20),
                advantage BOOLEAN,
                penalty BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Foul Committed.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO fouls_committed ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create interception table
cur.execute(""" CREATE TABLE interceptions (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                counterpress BOOLEAN,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                outcome VARCHAR(30),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Interception.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO interceptions ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create player_on table
cur.execute(""" CREATE TABLE players_on (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                off_camera BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Player On.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO players_on ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create player_off table
cur.execute(""" CREATE TABLE players_off (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                off_camera BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Player Off.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO players_off ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create dispossessed table
cur.execute(""" CREATE TABLE depossessions (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Dispossessed.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO depossessions ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create ball_recovery table
cur.execute(""" CREATE TABLE ball_recoveries (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                out BOOLEAN,
                recovery_failure BOOLEAN,
                offensive BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Ball Recovery.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO ball_recoveries ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create own_goal_against table
cur.execute(""" CREATE TABLE own_goals_against (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Own Goal Against.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO own_goals_against ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create miscontrol table
cur.execute(""" CREATE TABLE miscontrols (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                out BOOLEAN,
                aerial_won BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Miscontrol.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO miscontrols ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create goal_keeper table
cur.execute(""" CREATE TABLE goal_keeper_events (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                end_location_x FLOAT4,
                end_location_y FLOAT4,
                under_pressure BOOLEAN,
                off_camera BOOLEAN,
                out BOOLEAN,
                success_in_play BOOLEAN,
                punched_out BOOLEAN,
                lost_out BOOLEAN,
                outcome VARCHAR(30),
                technique VARCHAR(30),
                shot_saved_to_post BOOLEAN,
                shot_saved_off_target BOOLEAN,
                lost_in_play BOOLEAN,
                type VARCHAR(30),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Goal Keeper.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO goal_keeper_events ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create foul_won table
cur.execute(""" CREATE TABLE fouls_won (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                under_pressure BOOLEAN,
                off_camera BOOLEAN,
                defensive BOOLEAN,
                advantage BOOLEAN,
                penalty BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Foul Won.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO fouls_won ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create ball_receipt table
cur.execute(""" CREATE TABLE ball_receipts (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                under_pressure BOOLEAN,
                complete BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Ball Receipt.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO ball_receipts ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create clearance table
cur.execute(""" CREATE TABLE clearances (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                out BOOLEAN,
                aerial_won BOOLEAN,
                body_part VARCHAR(20),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Clearance.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO clearances ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create duel table
cur.execute(""" CREATE TABLE duels (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                counterpress BOOLEAN,
                outcome VARCHAR(30),
                type VARCHAR(20),

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Duel.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO duels ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create injury_stoppage table
cur.execute(""" CREATE TABLE injury_stoppages (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                outcome VARCHAR(30),
                in_chain BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Injury Stoppage.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO injury_stoppages ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create bad_behaviour table
cur.execute(""" CREATE TABLE bad_behaviours (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                card_type VARCHAR(20),
                duration FLOAT4,
                off_camera BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Bad Behaviour.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO bad_behaviours ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create dribbled_past table
cur.execute(""" CREATE TABLE dribbles_past (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                counterpress BOOLEAN,
                off_camera BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Dribbled Past.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO dribbles_past ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create block table
cur.execute(""" CREATE TABLE blocks (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                counterpress BOOLEAN,
                under_pressure BOOLEAN,
                out BOOLEAN,
                off_camera BOOLEAN,
                deflection BOOLEAN,
                offensive BOOLEAN,
                save BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Block.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO blocks ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create own_goal_for table
cur.execute(""" CREATE TABLE own_goals_for (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Own Goal For.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO own_goals_for ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create shield table
cur.execute(""" CREATE TABLE shields (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                under_pressure BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Shield.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO shields ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create pressure table
cur.execute(""" CREATE TABLE pressures (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                off_camera BOOLEAN,
                under_pressure BOOLEAN,
                counterpress BOOLEAN,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Pressure.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO pressures ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create Carry table
cur.execute(""" CREATE TABLE carries (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,
                duration FLOAT4,
                under_pressure BOOLEAN,
                end_location_x FLOAT4,
                end_location_y FLOAT4,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Carry.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO carries ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

#create Offside table
cur.execute(""" CREATE TABLE offsides (
                event_id VARCHAR(50),
                initiating_player INT,
                position VARCHAR(30),
                location_x FLOAT4,
                location_y FLOAT4,

                FOREIGN KEY (event_id)
                    REFERENCES events(event_id),
                FOREIGN KEY (initiating_player)
                    REFERENCES players(player_id)
            )
            """)
with open("dataset\sorted_events\Offside.json", "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        keys = event.keys()
        placeholders = ','.join(['%s' for _ in range(len(keys))])
        values = [event[key] for key in keys]
        columns = ','.join(keys)
        query = f"INSERT INTO offsides ({columns}) VALUES ({placeholders})"
        cur.execute(query, values)

conn.commit()
cur.close()
conn.close()
