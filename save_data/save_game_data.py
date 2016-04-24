import sqlite3 as lite
import sys

import os

print(os.path.dirname(os.path.abspath(__file__)))
print(os.getcwd())

con = lite.connect('../resources/predict.db', isolation_level=None)


def save_games(games: list) -> None:
    return con.executemany(
        "INSERT OR IGNORE INTO games(gameID, day,Team1Abbr,Team2Abbr,Team1Score,Team2Score) VALUES (?,?,?,?,?,?)",
        games)


def save_rosters(players: list) -> None:
    con.executemany("INSERT OR IGNORE INTO players VALUES(?,?,?,?,?,?,?,?)", players)


def save_game(game: dict) -> None:
    game_dbo = game["game"]
    save_the_game = con.execute(
        "INSERT OR IGNORE INTO games(gameID, day,Team1Abbr,Team2Abbr,Team1Score,Team2Score) VALUES (?,?,?,?,?,?)",
        game_dbo)

    player_dbos = game["home_team"] + game["away_team"]
    filtered_player_dbos = [player for player in player_dbos if player]
    save_players(filtered_player_dbos)
    sys.stdout.write(".")
    sys.stdout.flush()


def save_players(players: list) -> None:
    result = con.executemany(
        "INSERT OR IGNORE INTO gameData(gameID, teamID, playerID, Name, minutes, fgm, fga, tpm, tpa, ftm, fta, oreb, dreb, reb, assist, steal, block, turnover, fouls, plus_minus, points, injury) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        players)


def get_game_ids():
    return [gameid[0] for gameid in con.execute("SELECT gameID FROM games ORDER BY day ASC").fetchall()]


def get_game_dates():
    "SELECT gameId, Team1Abbr, Team2Abbr, Team1Score, Team2Score, Team1ELO, Team2ELO FROM games where day = \'"

    unique_days = con.execute("SELECT DISTINCT(day) FROM games").fetchall()

    data_for_days = []
    for day in unique_days:
        data_for_day = con.execute(
            "SELECT gameId, team1abbr, team2abbr, team1score, team2score, team1elo, team2elo FROM games WHERE day = {}".format(
                day[0])).fetchall()
        data_for_days.append(data_for_day)

    result = con.execute(
        "SELECT gameId, team1abbr, team2abbr, team1score, team2score, team1elo, team2elo FROM games").fetchall()

    return [game_day[0] for game_day in con.execute("SELECT DISTINCT day FROM games ORDER BY day ASC").fetchall()]
