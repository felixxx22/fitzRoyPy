import src.libs.code_loader as code_loader

SEASON = 2024
ROUNDS = range(1, 25)

import numpy as np

def clean_value(val):
    # Handle sentinel
    if isinstance(val, (int, float, np.integer, np.floating)) and val == -2147483648:
        return None

    # Convert numpy scalars to native types
    if isinstance(val, np.generic):
        return val.item()

    return val
# --- CREATE TABLE SQLs ---

CREATE_MATCHES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS matches (
    match_id TEXT PRIMARY KEY,
    utc_start_time TIMESTAMP,
    status TEXT,
    season_name TEXT,
    round_name TEXT,
    round_number INTEGER,
    venue_name TEXT,
    home_team_name TEXT,
    away_team_name TEXT
);
"""

CREATE_TEAMS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS teams (
    team_id TEXT PRIMARY KEY,
    team_name TEXT,
    club_name TEXT
);
"""

CREATE_PLAYERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS players (
    player_id TEXT PRIMARY KEY,
    given_name TEXT,
    surname TEXT,
    photo_url TEXT,
    captain BOOLEAN
);
"""

CREATE_PLAYER_STATS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_stats (
    player_id TEXT,
    match_id TEXT,
    season INTEGER,
    round INTEGER,
    goals NUMERIC,
    kicks NUMERIC,
    handballs NUMERIC,
    disposals NUMERIC,
    marks NUMERIC,
    bounces NUMERIC,
    tackles NUMERIC,
    team_id TEXT,
    team_status TEXT,
    PRIMARY KEY (player_id, match_id)
);
"""

# --- INSERT SQLs ---

INSERT_MATCH_SQL = """
INSERT INTO matches (
    match_id, utc_start_time, status,
    season_name, round_name, round_number,
    venue_name, home_team_name, away_team_name
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (match_id) DO NOTHING;
"""

INSERT_TEAM_SQL = """
INSERT INTO teams (team_id, team_name, club_name)
VALUES (%s, %s, %s)
ON CONFLICT (team_id) DO NOTHING;
"""

INSERT_PLAYER_SQL = """
INSERT INTO players (player_id, given_name, surname, photo_url, captain)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (player_id) DO NOTHING;
"""

INSERT_PLAYER_STATS_SQL = """
INSERT INTO player_stats (
    player_id, match_id, season, round,
    goals, kicks, handballs, disposals,
    marks, bounces, tackles,
    team_id, team_status
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (player_id, match_id) DO UPDATE SET
    goals = EXCLUDED.goals,
    kicks = EXCLUDED.kicks,
    handballs = EXCLUDED.handballs,
    disposals = EXCLUDED.disposals,
    marks = EXCLUDED.marks,
    bounces = EXCLUDED.bounces,
    tackles = EXCLUDED.tackles,
    team_id = EXCLUDED.team_id,
    team_status = EXCLUDED.team_status
"""

# --- MAIN SCRIPT ---

import pandas as pd

with code_loader.get_resources() as resources:
    logger = resources.logger

    # Load config
    config = resources.get_config('config.ini', 'credentials')

    # Connect to database
    database = resources.get_connector(config)

    # Create tables if they don't exist
    with database as db:
        db.execute(CREATE_MATCHES_TABLE_SQL)
        db.execute(CREATE_TEAMS_TABLE_SQL)
        db.execute(CREATE_PLAYERS_TABLE_SQL)
        db.execute(CREATE_PLAYER_STATS_TABLE_SQL)
        logger.info("All tables created or confirmed.")

    # FitzRoy API
    source = resources.get_source('fitzRoy')

    # Fetch and load all rounds
    for round_num in ROUNDS:
        logger.info(f"Fetching Season {SEASON}, Round {round_num}")

        try:
            df = source.get_player_stats(season=SEASON, round=round_num)
        except Exception as e:
            logger.error(f"Error fetching Round {round_num}: {e}")
            continue

        if df is None or df.empty:
            logger.warning(f"No data for Round {round_num}. Skipping.")
            continue

        with database as db:
            for _, row in df.iterrows():
                # Insert team
                db.execute(INSERT_TEAM_SQL, (
                    row.get('teamId'),
                    row.get('team.name'),
                    row.get('home.team.club.name') or row.get('away.team.club.name')
                ))
                
                # Insert match
                db.execute(INSERT_MATCH_SQL, (
                    row.get('providerId'),
                    row.get('utcStartTime'),
                    row.get('status'),
                    row.get('compSeason.shortName'),
                    row.get('round.name'),
                    clean_value(row.get('round.roundNumber')),
                    row.get('venue.name'),
                    row.get('home.team.name'),
                    row.get('away.team.name')
                ))

                # Insert player
                db.execute(INSERT_PLAYER_SQL, (
                    row.get('player.playerId'),
                    row.get('player.givenName'),
                    row.get('player.surname'),
                    row.get('player.photoURL'),
                    bool(row.get('player.captain')) if row.get('player.captain') is not None else None
                ))

                # Insert player stats
                db.execute(INSERT_PLAYER_STATS_SQL, (
                    row.get('player.playerId'),
                    row.get('providerId'),
                    SEASON,
                    round_num,
                    clean_value(row.get('goals')),
                    clean_value(row.get('kicks')),
                    clean_value(row.get('handballs')),
                    clean_value(row.get('disposals')),
                    clean_value(row.get('marks')),
                    clean_value(row.get('bounces')),
                    clean_value(row.get('tackles')),
                    row.get('teamId'),
                    row.get('teamStatus'),
                ))

            logger.info(f"Data for Round {round_num} loaded successfully.")

    logger.info("All 2024 data fully loaded into DB.")
