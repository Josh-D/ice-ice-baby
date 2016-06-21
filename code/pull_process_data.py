import MySQLdb
import json
import itertools
import os

# Get MySQLdb password
passwd_path = '/Users/deck/_galvanize/_project/sportradar/api_keys/mysql.json'
with open(passwd_path) as f:
    data = json.load(f)
    passwd = data['access-key']


# Establish connection to MySQL database
conn = MySQLdb.connect(host="localhost",
                       user="root",
                       passwd=passwd,
                       db="hockey")
cur = conn.cursor()

# Run stored procedure to drop & create tables
cur.execute('''CALL hockey.drop_tables();''')
conn.commit()


def process_files():

    # For each JSON file in the directory:
    path = '/Users/deck/_galvanize/_project/data/games/'
    for game in os.listdir(path):
        print game

        # Open file and assign data
        with open(path+game) as outfile:
            data = json.load(outfile)
            if data['status'] == 'closed':

                # Process file
                process_game(data)
                game_id = str(data['id'])
                for period in data['periods']:
                    process_period(period, game_id)
                    for event in period['events']:
                        period_id = str(period['id'])
                        process_event(event, period_id)


# Method for processing shotmissed data and inserting into MySQL table
def sql_insert_shotmissed(dic, event_id):

    # Assign column names to data
    player = dic.get('player', None)
    if player is not None:
        player_id = str(dic['player']['id'])
        player_name = str(dic['player']['full_name'])
        player_number = str(dic['player']['jersey_number'])
    else:
        player_id = "None"
        player_name = "None"
        player_number = "None"
    team_id = str(dic['team'].get('id', None))
    team_market = str(dic['team'].get('market', None))
    team_name = str(dic['team'].get('name', None))
    strength = str(dic['strength'])
    penalty = str(dic.get('penalty', None))
    shootout = str(dic.get('shootout', None))
    event_type = str(dic['type'])

    # Create "row" tuple to insert
    insert = (event_id, player_id, player_name, player_number, team_id,
              team_market, team_name, strength, penalty, shootout, event_type)

    # Insert tuple into shotmissed table
    cur.execute('''INSERT INTO {event_type}
                   VALUES {insert}'''.format(event_type=event_type,
                                             insert=insert))
    conn.commit()


# Method for processing shotagainst data and inserting into MySQL table
def sql_insert_shotagainst(dic, event_id):

    # Assign column names to data
    player = dic.get('player', None)
    if player is not None:
        player_id = str(dic['player'].get('id', None))
        player_name = str(dic['player'].get('full_name', None))
        player_number = str(dic['player'].get('jersey_number', None))
    else:
        player_id = "None"
        player_name = "None"
        player_number = "None"
    team_id = str(dic['team'].get('id', None))
    team_market = str(dic['team'].get('market', None))
    team_name = str(dic['team'].get('name', None))
    strength = str(dic['strength'])
    goal = str(dic['goal'])
    saved = str(dic['saved'])
    penalty = str(dic['penalty'])
    shootout = str(dic['shootout'])
    event_type = str(dic['type'])

    # Create "row" tuple to insert
    insert = (event_id, player_id, player_name, player_number, team_id,
              team_market, team_name, strength, goal, saved, penalty,
              shootout, event_type)

    # Insert "row" tuple into shotagainst table
    cur.execute('''INSERT INTO {event_type}
                   VALUES {insert}'''.format(event_type=event_type,
                                             insert=insert))
    conn.commit()


# Method for processing attribution data and inserting into MySQL table
def sql_insert_attribution(dic):

    # Assign column names to data
    event_id = str(dic['id'])
    attribution = dic.get('attribution', None)
    if attribution is not None:
        attribution_id = str(dic['attribution'].get('id', None))
        attribution_market = str(dic['attribution'].get('market', None))
        attribution_name = str(dic['attribution'].get('name', None))
        attribution_goal = str(dic['attribution'].get('team_goal', None))

    # Create "row" tuple to insert
    insert = (event_id, attribution_id, attribution_market,
              attribution_name, attribution_goal)

    # Insert "row" tuple into attribution table
    cur.execute('''INSERT INTO attribution
                   VALUES {insert}'''.format(insert=insert))
    conn.commit()


# Method for processing faceoff data and inserting into MySQL table
def sql_insert_faceoff(dic, event_id):

    # Assign column names to data
    player = dic.get('player', None)
    if player is not None:
        player_id = str(dic['player']['id'])
        player_name = str(dic['player']['full_name'])
        player_number = str(dic['player']['jersey_number'])
    else:
        player_id = "None"
        player_name = "None"
        player_number = "None"
    team_id = str(dic['team'].get('id', None))
    team_market = str(dic['team'].get('market', None))
    team_name = str(dic['team'].get('name', None))
    strength = str(dic['strength'])
    win = str(dic['win'])
    event_type = str(dic['type'])

    # Create "row" tuple to insert
    insert = (event_id, player_id, player_name, player_number, team_id,
              team_market, team_name, strength, win, event_type)

    # Insert "row" tuple into faceoff table
    cur.execute('''INSERT INTO {event_type}
                   VALUES {insert}'''.format(event_type=event_type,
                                             insert=insert))
    conn.commit()


# Method for processing powerplay data and inserting into MySQL table
def sql_insert_powerplay(dic, event_id):

    # Assign column names to data
    team_id = str(dic['team']['id'])
    team_market = str(dic['team']['market'])
    team_name = str(dic['team']['name'])
    strength = str(dic['strength'])
    event_type = str(dic['type'])

    # Create "row" tuple to insert
    insert = (event_id, team_id, team_market, team_name, strength, event_type)

    # Insert "row" tuple into powerplay table
    cur.execute('''INSERT INTO {event_type}
                   VALUES {insert}'''.format(event_type=event_type,
                                             insert=insert))
    conn.commit()


# Method for processing penalty data and inserting into MySQL table
def sql_insert_penalty(dic, event_id):

    # Assign column names to data
    player = dic.get('player', None)
    if player is not None:
        player_id = str(dic['player']['id'])
        player_name = str(dic['player']['full_name'])
        player_number = str(dic['player']['jersey_number'])
    else:
        player_id = "None"
        player_name = "None"
        player_number = "None"
    team_id = str(dic['team'].get('id', None))
    team_market = str(dic['team'].get('market', None))
    team_name = str(dic['team'].get('name', None))
    minutes = str(dic['minutes'])
    severity = str(dic['severity'])
    strength = str(dic['strength'])
    event_type = str(dic['type'])

    # Create "row" tuple to insert
    insert = (event_id, player_id, player_name, player_number, team_id,
              team_market, team_name, minutes, severity, strength, event_type)

    # Insert "row" tuple into penalty table
    cur.execute('''INSERT INTO {event_type}
                   VALUES {insert}'''.format(event_type=event_type,
                                             insert=insert))
    conn.commit()


# Method for processing location data and inserting into MySQL table
def sql_insert_location(dic):

    # Assign column names to data
    event_id = str(dic['id'])
    coord_x = dic['location']['coord_x']
    coord_y = dic['location']['coord_y']

    # Create "row" tuple to insert
    insert = (event_id, coord_x, coord_y)

    # Insert "row" tuple into location table
    cur.execute('''INSERT INTO location
                   VALUES {insert}'''.format(insert=insert))
    conn.commit()


# Method for processing base statistic data and inserting into MySQL table
def sql_insert_base_stats(dic, event_id):

    # Assign column names to data
    player = dic.get('player', None)
    if player is not None:
        player_id = str(dic['player'].get('id', None))
        player_name = str(dic['player'].get('full_name', None))
        player_number = str(dic['player'].get('jersey_number', None))
    else:
        player_id = "None"
        player_name = "None"
        player_number = "None"
    team_id = str(dic['team'].get('id', None))
    team_market = str(dic['team'].get('market', None))
    team_name = str(dic['team'].get('name', None))
    strength = str(dic['strength'])
    event_type = str(dic['type'])

    # Create "row" tuple to insert
    insert = (event_id, player_id, player_name, player_number, team_id,
              team_market, team_name, strength, event_type)

    # Insert "row" tuple into each statistic table
    cur.execute('''INSERT INTO {event_type}
                   VALUES {insert}'''.format(event_type=event_type,
                                             insert=insert))
    conn.commit()


# Method for calculating statistics in JSON file
def calc_stats(dic, event_id):

    # These stats all have the same MySQL table structure
    base_stats = ['assist', 'attemptblocked', 'block', 'shot',
                  'hit', 'giveaway', 'takeaway']

    if dic['type'] in base_stats:
        sql_insert_base_stats(dic, event_id)
    elif dic['type'] == 'faceoff':
        sql_insert_faceoff(dic, event_id)
    elif dic['type'] == 'shotagainst':
        sql_insert_shotagainst(dic, event_id)
    elif dic['type'] == 'shotmissed':
        sql_insert_shotmissed(dic, event_id)
    elif dic['type'] == 'penalty':
        sql_insert_penalty(dic, event_id)
    elif dic['type'] == 'powerplay':
        sql_insert_powerplay(dic, event_id)


# Method for processing each event in JSON file
def process_event(event, period_id):

    if 'attribution' in event:
        sql_insert_attribution(event)
    if 'location' in event:
        sql_insert_location(event)
    if 'statistics' in event:
        for statistic in event['statistics']:
            event_id = str(event['id'])
            calc_stats(statistic, event_id)
    stoppage_type = str(event.get('stoppage_type', None))
    strength = str(event.get('strength', None))
    updated = str(event.get('updated', None))
    zone = str(event.get('zone', None))
    event_id = str(event['id'])
    clock = str(event.get('clock', None))
    description = str(event['description'])
    duration = str(event.get('duration', None))
    event_type = str(event.get('event_type', None))
    penalty_type = str(event.get('penalty_type', None))

    # Create "row" tuple to insert
    insert = (event_id, period_id, clock, description, duration, event_type,
              penalty_type, stoppage_type, strength, updated, zone)

    # Insert "row" tuple into event table
    cur.execute('''INSERT INTO event
                   VALUES {insert}'''.format(insert=insert))
    conn.commit()


# Method for processing each period in JSON file
def process_period(period, game_id):

    period_id = str(period['id'])
    period_number = str(period['number'])
    unit_type = str(period['type'])
    home = period['scoring'].get('home', None)
    if home is not None:
        home_id = str(period['scoring']['home'].get('id', None))
        home_name = str(period['scoring']['home'].get('name', None))
        home_market = str(period['scoring']['home'].get('market', None))
        home_goals = str(period['scoring']['home'].get('points', None))
    else:
        home_id = "None"
        home_name = "None"
        home_market = "None"
        home_goals = "None"
    away = period['scoring'].get('away', None)
    if away is not None:
        away_id = str(period['scoring']['away'].get('id', None))
        away_name = str(period['scoring']['away'].get('name', None))
        away_market = str(period['scoring']['away'].get('market', None))
        away_goals = str(period['scoring']['away'].get('points', None))
    else:
        away_id = "None"
        away_name = "None"
        away_market = "None"
        away_goals = "None"
    # home_id = str(period['scoring']['home']['id'])
    # home_market = str(period['scoring']['home']['market'])
    # home_name = str(period['scoring']['home']['name'])
    # home_goals = str(period['scoring']['home']['points'])
    # away_id = str(period['scoring']['away']['id'])
    # away_market = str(period['scoring']['away']['market'])
    # away_name = str(period['scoring']['away']['name'])
    # away_goals = str(period['scoring']['away']['points'])
    period_sequence = str(period['sequence'])

    # Create "row" tuple to insert
    insert = (game_id, period_id, period_number, unit_type, home_id,
              home_market, home_name, home_goals, away_id, away_market,
              away_name, away_goals, period_sequence)

    # Insert "row" tuple into period table
    cur.execute('''INSERT INTO period
                   VALUES {insert}'''.format(insert=insert))
    conn.commit()


# Method for processing each JSON file (game) in games folder
def process_game(data):

    status = str(data['status'])
    scheduled = str(data['scheduled'])
    attendance = str(data.get('attendance', None))
    total_game_duration = str(data.get('total_game_duration', None))
    clock = str(data.get('clock', None))
    coverage = str(data.get('coverage', None))
    start_time = str(data.get('start_time', None))
    end_time = str(data.get('end_time', None))
    period = str(data.get('period', None))
    game_id = str(data['id'])
    home = data.get('home', None)
    if home is not None:
        home_id = str(data['home'].get('id', None))
        home_name = str(data['home'].get('name', None))
        home_market = str(data['home'].get('market', None))
        home_points = str(data['home'].get('points', None))
    else:
        home_id = "None"
        home_name = "None"
        home_market = "None"
        home_points = "None"
    away = data.get('away', None)
    if away is not None:
        away_id = str(data['away'].get('id', None))
        away_name = str(data['away'].get('name', None))
        away_market = str(data['away'].get('market', None))
        away_points = str(data['away'].get('points', None))
    else:
        away_id = "None"
        away_name = "None"
        away_market = "None"
        away_points = "None"

    # Create "row" tuple to insert
    insert = (game_id, scheduled, total_game_duration, clock,
              coverage, start_time, end_time, period, home_id,
              home_market, home_name, home_points, away_id,
              away_name, away_market, away_points, status)

    # Insert "row" tuple into game table
    cur.execute('''INSERT INTO game
                   VALUES {insert}'''.format(insert=insert))
    conn.commit()

if __name__ == '__main__':
    process_files()
