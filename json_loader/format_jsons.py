import os
import json
import shutil

#This code assumes that the raw data from Statbomb is available, with at least the events, lineups, and matches sub folders
#this data should be stored in a folder named data
#also assumes you are in the Repo folder
if os.path.exists('dataset'):
    shutil.rmtree('dataset')
os.mkdir("dataset")
os.mkdir(os.path.join("dataset", "sorted_events"))

uniqueCompetitions = {}

#move the base competitions to a filtered version
with open(os.path.join("data", "competitions.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    filtered_competitions = []
    for competition in data:
        if ((competition["competition_name"] == "La Liga" and competition["season_name"] == "2018/2019")
        or (competition["competition_name"] == "La Liga" and competition["season_name"] == "2019/2020")
        or (competition["competition_name"] == "La Liga" and competition["season_name"] == "2020/2021")
        or (competition["competition_name"] == "Premier League" and competition["season_name"] == "2003/2004")):
            del competition["match_updated"]
            del competition["match_updated_360"]
            del competition["match_available_360"]
            del competition["match_available"]
            competition["gender"] = competition["competition_gender"]
            del competition["competition_gender"]
            competition["youth"] = competition["competition_youth"]
            del competition["competition_youth"]
            competition["international"] = competition["competition_international"]
            del competition["competition_international"]
            competition["country"] = competition["country_name"]
            del competition["country_name"]
            filtered_competitions.append(competition)
            if not competition["competition_id"] in uniqueCompetitions.keys():
                uniqueCompetitions[competition["competition_id"]] = [competition["season_id"]]
            else:
                uniqueCompetitions[competition["competition_id"]].append(competition["season_id"])
        else:
            del competition
    with open(os.path.join("dataset", "competitions.json"), "w") as jsonFile:
        json.dump(filtered_competitions, jsonFile, indent=4)
#competitions.json now formatted correctly

allMatches = []

#Remove unused matches
for comp_id in uniqueCompetitions.keys():
    for sea_id in uniqueCompetitions[comp_id]:
        with open(os.path.join("data","matches", str(comp_id), str(sea_id)+".json"), "r", encoding="utf8") as jsonFile:
            data = json.load(jsonFile)
            for match in data:
                allMatches.append(match)
        with open(os.path.join("dataset","matches.json"), "w") as jsonFile:
            json.dump(allMatches, jsonFile, indent=4)
#unused matches obtained
#first pass of filtering
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        del match["home_team"]["home_team_group"]
        match["home_team"]["country"] = match["home_team"]["country"]["name"]

        del match["away_team"]["away_team_group"]
        match["away_team"]["country"] = match["away_team"]["country"]["name"]

        match["competition_id"] = match["competition"]["competition_id"]
        del match["competition"]

        match["season_id"] = match["season"]["season_id"]
        del match["season"]

        match["competition_stage"] = match["competition_stage"]["name"]

        del match["match_status"]
        del match["match_status_360"]
        del match["last_updated"]
        del match["last_updated_360"]
        del match["metadata"]
with open(os.path.join("dataset","matches.json"), "w") as jsonFile:
    json.dump(data, jsonFile, indent=4)


#find all used stadiums
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    allStadiums = []
    knownStadiumIds = set()
    data = json.load(jsonFile)
    for match in data:
        if "stadium" in match.keys() and match["stadium"]["id"] not in knownStadiumIds:
            curStadium = {}
            curStadium["stadium_id"] = match["stadium"]["id"]
            curStadium["name"] = match["stadium"]["name"]
            curStadium["country"] = match["stadium"]["country"]["name"]
            allStadiums.append(curStadium)
            knownStadiumIds.add(match["stadium"]["id"])
            del match["stadium"]
    with open(os.path.join("dataset","stadiums.json"), "w") as jsonFile:
        json.dump(allStadiums, jsonFile, indent=4)

#replace stadiums in matches with id
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        if "stadium" in match.keys():
            match["stadium_id"] = match["stadium"]["id"]
            del match["stadium"]
        else:
            match["stadium_id"] = None
    with open(os.path.join("dataset","matches.json"), "w") as jsonFile:
        json.dump(data, jsonFile, indent=4)

#find all teams
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allTeams = []
    uniqueTeamIds = set()
    for match in data:
        curTeam = {}
        curTeam["team_id"] = match["home_team"]["home_team_id"]
        curTeam["team_name"] = match["home_team"]["home_team_name"]
        curTeam["team_gender"] = match["home_team"]["home_team_gender"]
        curTeam["country"] = match["home_team"]["country"]
        if not curTeam["team_id"] in uniqueTeamIds:
            allTeams.append(curTeam)
            uniqueTeamIds.add(curTeam["team_id"])
        curTeam = {}
        curTeam["team_id"] = match["away_team"]["away_team_id"]
        curTeam["team_name"] = match["away_team"]["away_team_name"]
        curTeam["team_gender"] = match["away_team"]["away_team_gender"]
        curTeam["country"] = match["away_team"]["country"]
        if not curTeam["team_id"] in uniqueTeamIds:
            allTeams.append(curTeam)
            uniqueTeamIds.add(curTeam["team_id"])
    with open(os.path.join("dataset","teams.json"), "w") as jsonFile:
        json.dump(allTeams, jsonFile, indent=4)

#find all managers
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allManagers = []
    uniqueManagerIds = set()
    for match in data:
        if "managers" in match["home_team"].keys():
            for manager in match["home_team"]["managers"]:
                curMan = {}
                curMan["manager_id"] = manager["id"]
                curMan["name"] = manager["name"]
                curMan["nickname"] = manager["nickname"]
                curMan["dob"] = manager["dob"]
                curMan["country"] = manager["country"]["name"]
                if not curMan["manager_id"] in uniqueManagerIds:
                    allManagers.append(curMan)
                    uniqueManagerIds.add(curMan["manager_id"])
        if "managers" in match["away_team"].keys():
            for manager in match["away_team"]["managers"]:
                curMan = {}
                curMan["manager_id"] = manager["id"]
                curMan["name"] = manager["name"]
                curMan["nickname"] = manager["nickname"]
                curMan["dob"] = manager["dob"]
                curMan["country"] = manager["country"]["name"]
                if not curMan["manager_id"] in uniqueManagerIds:
                    allManagers.append(curMan)
                    uniqueManagerIds.add(curMan["manager_id"])
    with open(os.path.join("dataset","managers.json"), "w") as jsonFile:
        json.dump(allManagers, jsonFile, indent=4)

#remove team info from matches
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        match["home_team_id"] = match["home_team"]["home_team_id"]
        match["away_team_id"] = match["away_team"]["away_team_id"]

        if "managers" in match["home_team"].keys():
            match["home_team_managers"] = match["home_team"]["managers"]
        else:
            match["home_team_managers"] = []

        if "managers" in match["away_team"].keys():
            match["away_team_managers"] = match["away_team"]["managers"]
        else:
            match["away_team_managers"] = []

        curManagers = []
        for manager in match["home_team_managers"]:
            curManagers.append(manager["id"])
        match["home_team_managers"] = curManagers
        curManagers = []
        for manager in match["away_team_managers"]:
            curManagers.append(manager["id"])
        match["away_team_managers"] = curManagers

        del match["home_team"]
        del match["away_team"]
    with open(os.path.join("dataset","matches.json"), "w") as jsonFile:
        json.dump(data, jsonFile, indent=4)

#find the managers in each match
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    team_managers = []
    for match in data:
        match_id = match["match_id"]

        home_team_id = match["home_team_id"]
        for manager in match["home_team_managers"]:
            curManRel = {}
            curManRel["match_id"] = match_id
            curManRel["manager_id"] = manager
            curManRel["managing_home_team"] = True
            team_managers.append(curManRel)

        away_team_id = match["away_team_id"]
        for manager in match["away_team_managers"]:
            curManRel = {}
            curManRel["match_id"] = match_id
            curManRel["manager_id"] = manager
            curManRel["managing_home_team"] = False
            team_managers.append(curManRel)
    with open(os.path.join("dataset","team_managers.json"), "w") as jsonFile:
        json.dump(team_managers, jsonFile, indent=4)

#delete the manager ids in matches
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        del match["home_team_managers"]
        del match["away_team_managers"]
    with open(os.path.join("dataset","matches.json"), "w") as jsonFile:
        json.dump(data, jsonFile, indent=4)

#find all referees
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allReferees = []
    uniqueRefereeIds = set()
    for match in data:
        if "referee" in match.keys():
            curRef = {}
            curRef["referee_id"] = match["referee"]["id"]
            curRef["name"] = match["referee"]["name"]
            curRef["country"] = match["referee"]["country"]["name"]
            if not curRef["referee_id"] in uniqueRefereeIds:
                allReferees.append(curRef)
                uniqueRefereeIds.add(curRef["referee_id"])
    with open(os.path.join("dataset","referees.json"), "w") as jsonFile:
        json.dump(allReferees, jsonFile, indent=4)

#delete the manager ids in matches
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        if "referee" in match.keys():
            del match["referee"]
    with open(os.path.join("dataset","matches.json"), "w") as jsonFile:
        json.dump(data, jsonFile, indent=4)

#find match ids for later
matchIds = []
with open(os.path.join("dataset","matches.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for match in data:
        matchIds.append(match["match_id"])

#find all relevant lineups
allLineups = []
for match_id in matchIds:
    relevantLineupJSON = os.path.join("data","lineups", str(match_id) + ".json")
    with open(relevantLineupJSON, "r", encoding="utf8") as jsonFile:
        data = json.load(jsonFile)
        for lineup in data:
            lineup["match_id"] = match_id
            allLineups.append(lineup)
with open(os.path.join("dataset","lineups.json"), "w", encoding="utf8") as jsonFile:
    json.dump(allLineups, jsonFile, indent=4)

#start to split the lineup into cards, positions, players, and teams those players have played in
#find all cards
with open(os.path.join("dataset","lineups.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allCards = []
    for lineup in data:
        team_id = lineup["team_id"]
        match_id = lineup["match_id"]
        for player in lineup["lineup"]:
            player_id = player["player_id"]
            for card in player["cards"]:
                curCard = {}
                curCard["time_issued"] = card["time"]
                curCard["card_type"] = card["card_type"]
                curCard["reason"] = card["reason"]
                curCard["period"] = card["period"]
                curCard["player_id"] = player_id
                curCard["team_id"] = team_id
                curCard["match_id"] = match_id
                allCards.append(curCard)
    cardId = 1
    for card in allCards:
        card["card_id"] = cardId
        cardId += 1
    with open(os.path.join("dataset","cards.json"), "w") as jsonFile:
        json.dump(allCards, jsonFile, indent=4)

#find all positions
with open(os.path.join("dataset","lineups.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allPositions = []
    for lineup in data:
        team_id = lineup["team_id"]
        match_id = lineup["match_id"]
        for player in lineup["lineup"]:
            player_id = player["player_id"]
            for position in player["positions"]:
                curPosition = {}
                curPosition["position"] = position["position"]
                curPosition["from_time"] = position["from"]
                curPosition["to_time"] = position["to"]
                curPosition["from_period"] = position["from_period"]
                curPosition["to_period"] = position["to_period"]
                curPosition["start_reason"] = position["start_reason"]
                curPosition["end_reason"] = position["end_reason"]
                curPosition["player_id"] = player_id
                curPosition["team_id"] = team_id
                curPosition["match_id"] = match_id
                allPositions.append(curPosition)
    positionId = 1
    for position in allPositions:
        position["position_id"] = positionId
        positionId += 1
    with open(os.path.join("dataset","positions.json"), "w") as jsonFile:
        json.dump(allPositions, jsonFile, indent=4)

#find all players
with open(os.path.join("dataset","lineups.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allPlayers = []
    uniquePlayerIDs = set()
    for lineup in data:
        for player in lineup["lineup"]:
            if not player["player_id"] in uniquePlayerIDs:
                curPlayer = {}
                curPlayer["player_id"] = player["player_id"]
                curPlayer["player_name"] = player["player_name"]
                curPlayer["player_nickname"] = player["player_nickname"]
                curPlayer["country"] = player["country"]["name"]
                allPlayers.append(curPlayer)
                uniquePlayerIDs.add(player["player_id"])
    with open(os.path.join("dataset","players.json"), "w") as jsonFile:
        json.dump(allPlayers, jsonFile, indent=4)

#find all teams each player has played in w/ jersey number
with open(os.path.join("dataset","lineups.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allParticipations = []
    for lineup in data:
        team_id = lineup["team_id"]
        match_id = lineup["match_id"]
        for player in lineup["lineup"]:
            player_id = player["player_id"]
            jersey_number = player["jersey_number"]
            curPlayedIn = {}
            curPlayedIn["player_id"] = player_id 
            curPlayedIn["team_id"] = team_id
            curPlayedIn["match_id"] = match_id
            curPlayedIn["jersey_number"] = jersey_number
            allParticipations.append(curPlayedIn)
    with open(os.path.join("dataset","played_in.json"), "w") as jsonFile:
        json.dump(allParticipations, jsonFile, indent=4)

#remove the lineups file
os.remove(os.path.join("dataset", "lineups.json"))

#find all relevant events
allEvents = []
for match_id in matchIds:
    relevantEventJSON = os.path.join("data","events", str(match_id) + ".json")
    with open(relevantEventJSON, "r", encoding="utf8") as jsonFile:
        data = json.load(jsonFile)
        for event in data:
            event["match_id"] = match_id
            allEvents.append(event)
with open(os.path.join("dataset","events.json"), "w", encoding="utf8") as jsonFile:
    json.dump(allEvents, jsonFile, indent=4)

#rename "Ball Receipt*" to "Ball Receipt". Dont know why theres a *
#also renames 50/50 to Fifty-Fifty
with open(os.path.join("dataset","events.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        if event["type"]["name"] == "Ball Receipt*":
            event["type"]["name"] = "Ball Receipt"
        if event["type"]["name"] == "50/50":
            event["type"]["name"] = "Fifty-Fifty"
    with open(os.path.join("dataset","events.json"), "w", encoding="utf8") as jsonFile:
        json.dump(data, jsonFile, indent=4)

#sort events by type
sorted_events = {}
with open(os.path.join("dataset","events.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    for event in data:
        if not event["type"]["name"] in sorted_events.keys():
            sorted_events[event["type"]["name"]] = []
        sorted_events[event["type"]["name"]].append(event)
    for key in sorted_events.keys():
        with open(os.path.join("dataset", "sorted_events", str(key) + ".json"), "w") as jsonFile:
            json.dump(sorted_events[key], jsonFile, indent=4)

#filter general event data
with open(os.path.join("dataset","events.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["index"] = event["index"]
        curEvent["period"] = event["period"]
        curEvent["timestamp"] = event["timestamp"]
        curEvent["minute"] = event["minute"]
        curEvent["second"] = event["second"]
        curEvent["type"] = event["type"]["name"]
        curEvent["possession"] = event["possession"]
        curEvent["possession_team_id"] = event["possession_team"]["id"]
        curEvent["play_pattern"] = event["play_pattern"]["name"]
        curEvent["event_team_id"] = event["team"]["id"]
        curEvent["match_id"] = event["match_id"]
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset","events.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

#filter each event type No comments
with open(os.path.join("dataset", "sorted_events", "Bad Behaviour.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["card_type"] = event["bad_behaviour"]["card"]["name"]
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Bad Behaviour.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Ball Receipt.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]

        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False

        if "ball_receipt" in event.keys():
            #always incomplete when present
            curEvent["complete"] = False
        else:
            curEvent["complete"] = True
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Ball Receipt.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Ball Recovery.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False

        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "ball_recovery" in event.keys() and "recovery_failure" in event["ball_recovery"]:
            curEvent["recovery_failure"] = True
        else:
            curEvent["recovery_failure"] = False
            
        if "ball_recovery" in event.keys() and "offensive" in event["ball_recovery"]:
            curEvent["offensive"] = True
        else:
            curEvent["offensive"] = False
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Ball Recovery.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Block.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False

        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False

        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
            
        if "block" in event.keys() and "save_block" in event["block"]:
            curEvent["save"] = True
        else:
            curEvent["save"] = False
            
        if "block" in event.keys() and "deflection" in event["block"]:
            curEvent["deflection"] = True
        else:
            curEvent["deflection"] = False
            
        if "block" in event.keys() and "offensive" in event["block"]:
            curEvent["offensive"] = True
        else:
            curEvent["offensive"] = False
            
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Block.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Carry.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        curEvent["end_location_x"] = event["carry"]["end_location"][0]
        curEvent["end_location_y"] = event["carry"]["end_location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Carry.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Clearance.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "aerial_won" in event["clearance"].keys():
            curEvent["aerial_won"] = True
        else:
            curEvent["aerial_won"] = False
            
        curEvent["body_part"] = event["clearance"]["body_part"]["name"]
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Clearance.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Dispossessed.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Dispossessed.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Dribble.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "overrun" in event["dribble"].keys():
            curEvent["overrun"] = True
        else:
            curEvent["overrun"] = False
            
        if "no_touch" in event["dribble"].keys():
            curEvent["no_touch"] = True
        else:
            curEvent["no_touch"] = False
            
        if "nutmeg" in event["dribble"].keys():
            curEvent["nutmeg"] = True
        else:
            curEvent["nutmeg"] = False
            
        if "Complete" == event["dribble"]["outcome"]["name"]:
            curEvent["completed"] = True
        else:
            curEvent["completed"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Dribble.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Dribbled Past.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Dribbled Past.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Duel.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "type" in event["duel"].keys():
            curEvent["type"] = event["duel"]["type"]["name"]
        else:
            curEvent["type"] = None
            
        if "outcome" in event["duel"].keys():
            curEvent["outcome"] = event["duel"]["outcome"]["name"]
        else:
            curEvent["outcome"] = None
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Duel.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Error.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Error.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Fifty-Fifty.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
        curEvent["outcome"] = event["50_50"]["outcome"]["name"]
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Fifty-Fifty.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Foul Committed.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False

        if "foul_committed" in event.keys() and "type" in event["foul_committed"].keys():
            curEvent["type"] = event["foul_committed"]["type"]["name"]
        else:
            curEvent["type"] = None

        if "foul_committed" in event.keys() and "card" in event["foul_committed"].keys():
            curEvent["card_type"] = event["foul_committed"]["card"]["name"]
        else:
            curEvent["card_type"] = None
            
        if "foul_committed" in event.keys() and "offensive" in event["foul_committed"].keys():
            curEvent["offensive"] = True
        else:
            curEvent["offensive"] = False
            
        if "foul_committed" in event.keys() and "advantage" in event["foul_committed"].keys():
            curEvent["advantage"] = True
        else:
            curEvent["advantage"] = False
            
        if "foul_committed" in event.keys() and "penalty" in event["foul_committed"].keys():
            curEvent["penalty"] = True
        else:
            curEvent["penalty"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Foul Committed.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Foul Won.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "foul_won" in event.keys() and "defensive" in event["foul_won"].keys():
            curEvent["defensive"] = True
        else:
            curEvent["defensive"] = False
            
        if "foul_won" in event.keys() and "advantage" in event["foul_won"].keys():
            curEvent["advantage"] = True
        else:
            curEvent["advantage"] = False
            
        if "foul_won" in event.keys() and "penalty" in event["foul_won"].keys():
            curEvent["penalty"] = True
        else:
            curEvent["penalty"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Foul Won.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Goal Keeper.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
            
        if "location" in event.keys():
            curEvent["location_x"] = event["location"][0]
            curEvent["location_y"] = event["location"][1]
        else:
            curEvent["location_x"] = None
            curEvent["location_y"] = None
            
        if "end_location" in event["goalkeeper"].keys():
            curEvent["end_location_x"] = event["goalkeeper"]["end_location"][0]
            curEvent["end_location_y"] = event["goalkeeper"]["end_location"][1]
        else:
            curEvent["end_location_x"] = None
            curEvent["end_location_y"] = None
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "success_in_play" in event["goalkeeper"].keys():
            curEvent["success_in_play"] = True
        else:
            curEvent["success_in_play"] = False
            
        if "punched_out" in event["goalkeeper"].keys():
            curEvent["punched_out"] = True
        else:
            curEvent["punched_out"] = False
            
        if "lost_out" in event["goalkeeper"].keys():
            curEvent["lost_out"] = True
        else:
            curEvent["lost_out"] = False
            
        if "outcome" in event["goalkeeper"].keys():
            curEvent["outcome"] = event["goalkeeper"]["outcome"]["name"]
        else:
            curEvent["outcome"] = None
            
        if "technique" in event["goalkeeper"].keys():
            curEvent["technique"] = event["goalkeeper"]["technique"]["name"]
        else:
            curEvent["technique"] = None
            
        if "type" in event["goalkeeper"].keys():
            curEvent["type"] = event["goalkeeper"]["type"]["name"]
        else:
            curEvent["type"] = None
            
        if "shot_saved_to_post" in event["goalkeeper"].keys():
            curEvent["shot_saved_to_post"] = True
        else:
            curEvent["shot_saved_to_post"] = False
            
        if "shot_saved_off_target" in event["goalkeeper"].keys():
            curEvent["shot_saved_off_target"] = True
        else:
            curEvent["shot_saved_off_target"] = False
            
        if "lost_in_play" in event["goalkeeper"].keys():
            curEvent["lost_in_play"] = True
        else:
            curEvent["lost_in_play"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Goal Keeper.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Half End.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Half End.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Half Start.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["duration"] = event["duration"]
        
        if "half_start" in event.keys():
            curEvent["late_video_start"] = True
        else:
            curEvent["late_video_start"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Half Start.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Injury Stoppage.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["duration"] = event["duration"]
            
        if "location" in event.keys():
            curEvent["location_x"] = event["location"][0]
            curEvent["location_y"] = event["location"][1]
        else:
            curEvent["location_x"] = None
            curEvent["location_y"] = None
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
        
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        
        if "injury_stoppage" in event.keys():
            curEvent["in_chain"] = True
        else:
            curEvent["in_chain"] = False
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Injury Stoppage.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Interception.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["duration"] = event["duration"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
        
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False

        curEvent["outcome"] = event["interception"]["outcome"]["name"]
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Interception.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Miscontrol.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["duration"] = event["duration"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
        
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
        
        if "aerial_won" in event.keys():
            curEvent["aerial_won"] = True
        else:
            curEvent["aerial_won"] = False
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Miscontrol.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Offside.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]

        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Offside.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Own Goal Against.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]

        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Own Goal Against.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Own Goal For.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        if "player" in event.keys():
            curEvent["initiating_player"] = event["player"]["id"]
        else:
            curEvent["initiating_player"] = None
        if "position" in event.keys():
            curEvent["position"] = event["position"]["name"]
        else:
            curEvent["position"] = None
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]

        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Own Goal For.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Pass.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        if "recipient" in event["pass"].keys():
            curEvent["receiving_player"] = event["pass"]["recipient"]["id"]
        else:
            curEvent["receiving_player"] = None
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        curEvent["end_location_x"] = event["pass"]["end_location"][0]
        curEvent["end_location_y"] = event["pass"]["end_location"][1]
        curEvent["length"] = event["pass"]["length"]
        curEvent["height"] = event["pass"]["height"]["name"]
        curEvent["angle"] = event["pass"]["angle"]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
            
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "through_ball" in event["pass"].keys():
            curEvent["through_ball"] = True
        else:
            curEvent["through_ball"] = False
            
        if "shot_assist" in event["pass"].keys():
            curEvent["shot_assist"] = True
        else:
            curEvent["shot_assist"] = False
            
        if "inswinging" in event["pass"].keys():
            curEvent["inswinging"] = True
        else:
            curEvent["inswinging"] = False
            
        if "outswinging" in event["pass"].keys():
            curEvent["outswinging"] = True
        else:
            curEvent["outswinging"] = False
            
        if "miscommunication" in event["pass"].keys():
            curEvent["miscommunication"] = True
        else:
            curEvent["miscommunication"] = False
            
        if "aerial_won" in event["pass"].keys():
            curEvent["aerial_won"] = True
        else:
            curEvent["aerial_won"] = False
            
        if "cut_back" in event["pass"].keys():
            curEvent["cut_back"] = True
        else:
            curEvent["cut_back"] = False
            
        if "no_touch" in event["pass"].keys():
            curEvent["no_touch"] = True
        else:
            curEvent["no_touch"] = False
            
        if "deflected" in event["pass"].keys():
            curEvent["deflected"] = True
        else:
            curEvent["deflected"] = False
            
        if "switch" in event["pass"].keys():
            curEvent["switch"] = True
        else:
            curEvent["switch"] = False
            
        if "straight" in event["pass"].keys():
            curEvent["straight"] = True
        else:
            curEvent["straight"] = False
            
        if "crossing" in event["pass"].keys():
            curEvent["crossing"] = True
        else:
            curEvent["crossing"] = False
            
        if "goal_assist" in event["pass"].keys():
            curEvent["goal_assist"] = True
        else:
            curEvent["goal_assist"] = False
            
        if "technique" in event["pass"].keys():
            curEvent["technique"] = event["pass"]["technique"]["name"]
        else:
            curEvent["technique"] = None
            
        if "outcome" in event["pass"].keys():
            curEvent["outcome"] = event["pass"]["outcome"]["name"]
        else:
            curEvent["outcome"] = None
            
        if "body_part" in event["pass"].keys():
            curEvent["body_part"] = event["pass"]["body_part"]["name"]
        else:
            curEvent["body_part"] = None
            
        if "type" in event["pass"].keys():
            curEvent["type"] = event["pass"]["type"]["name"]
        else:
            curEvent["type"] = None
            
        if "assisted_shot_id" in event["pass"].keys():
            curEvent["assisted_shot_event_id"] = event["pass"]["assisted_shot_id"]
        else:
            curEvent["assisted_shot_event_id"] = None
       
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Pass.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Player Off.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False

        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Player Off.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Player On.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False

        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Player On.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Pressure.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["duration"] = event["duration"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
        
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        
        if "counterpress" in event.keys():
            curEvent["counterpress"] = True
        else:
            curEvent["counterpress"] = False
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Pressure.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Referee Ball-Drop.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Referee Ball-Drop.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Shield.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Shield.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Shot.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["duration"] = event["duration"]
        curEvent["position"] = event["position"]["name"]
        curEvent["location_x"] = event["location"][0]
        curEvent["location_y"] = event["location"][1]
        curEvent["end_location_x"] = event["shot"]["end_location"][0]
        curEvent["end_location_y"] = event["shot"]["end_location"][1]
        
        if "under_pressure" in event.keys():
            curEvent["under_pressure"] = True
        else:
            curEvent["under_pressure"] = False
            
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
            
        if "out" in event.keys():
            curEvent["out"] = True
        else:
            curEvent["out"] = False
            
        if "first_time" in event["shot"].keys():
            curEvent["first_time"] = True
        else:
            curEvent["first_time"] = False
            
        if "one_on_one" in event["shot"].keys():
            curEvent["one_on_one"] = True
        else:
            curEvent["one_on_one"] = False
            
        if "open_goal" in event["shot"].keys():
            curEvent["open_goal"] = True
        else:
            curEvent["open_goal"] = False
            
        if "deflected" in event["shot"].keys():
            curEvent["deflected"] = True
        else:
            curEvent["deflected"] = False
            
        if "redirect" in event["shot"].keys():
            curEvent["redirect"] = True
        else:
            curEvent["redirect"] = False
            
        if "saved_off_target" in event["shot"].keys():
            curEvent["saved_off_target"] = True
        else:
            curEvent["saved_off_target"] = False
            
        if "saved_to_post" in event["shot"].keys():
            curEvent["saved_to_post"] = True
        else:
            curEvent["saved_to_post"] = False
            
        if "aerial_won" in event["shot"].keys():
            curEvent["aerial_won"] = True
        else:
            curEvent["aerial_won"] = False
            
        if "follows_dribble" in event["shot"].keys():
            curEvent["follows_dribble"] = True
        else:
            curEvent["follows_dribble"] = False
            
        if "body_part" in event["shot"].keys():
            curEvent["body_part"] = event["shot"]["body_part"]["name"]
        else:
            curEvent["body_part"] = None
            
        if "technique" in event["shot"].keys():
            curEvent["technique"] = event["shot"]["technique"]["name"]
        else:
            curEvent["technique"] = None
            
        if "type" in event["shot"].keys():
            curEvent["type"] = event["shot"]["type"]["name"]
        else:
            curEvent["type"] = None
            
        curEvent["xg_score"] = event["shot"]["statsbomb_xg"]
            
        if "outcome" in event["shot"].keys():
            curEvent["outcome"] = event["shot"]["outcome"]["name"]
        else:
            curEvent["outcome"] = None
            
        if "key_pass_id" in event["shot"].keys():
            curEvent["key_pass_id"] = event["shot"]["key_pass_id"]
        else:
            curEvent["key_pass_id"] = None
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Shot.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Starting XI.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["formation"] = event["tactics"]["formation"]
        curPlayer = 1
        for lineup in event["tactics"]["lineup"]:
            curEvent["player_id_" + str(curPlayer)] = lineup["player"]["id"]
            curEvent["player_position_" + str(curPlayer)] = lineup["position"]["name"]
            curEvent["player_jersey_number_" + str(curPlayer)] = lineup["jersey_number"]
            curPlayer += 1
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Starting XI.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Substitution.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["initiating_player"] = event["player"]["id"]
        curEvent["position"] = event["position"]["name"]
        
        if "off_camera" in event.keys():
            curEvent["off_camera"] = True
        else:
            curEvent["off_camera"] = False
        
        curEvent["outcome"] = event["substitution"]["outcome"]["name"]
        curEvent["replacement_player"] = event["substitution"]["replacement"]["id"]
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Substitution.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)

with open(os.path.join("dataset", "sorted_events", "Tactical Shift.json"), "r", encoding="utf8") as jsonFile:
    data = json.load(jsonFile)
    allEventsFiltered = []
    for event in data:
        curEvent = {}
        curEvent["event_id"] = event["id"]
        curEvent["formation"] = event["tactics"]["formation"]
        curPlayer = 1
        for lineup in event["tactics"]["lineup"]:
            curEvent["player_id_" + str(curPlayer)] = lineup["player"]["id"]
            curEvent["player_position_" + str(curPlayer)] = lineup["position"]["name"]
            curEvent["player_jersey_number_" + str(curPlayer)] = lineup["jersey_number"]
            curPlayer += 1
        allEventsFiltered.append(curEvent)
    with open(os.path.join("dataset", "sorted_events", "Tactical Shift.json"), "w", encoding="utf8") as jsonFile:
        json.dump(allEventsFiltered, jsonFile, indent=4)
