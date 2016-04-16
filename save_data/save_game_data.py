import sqlite3 as lite

con = lite.connect('../predict.db', isolation_level=None)
db = con.cursor()


def save_games(games: list):
    con.executemany("INSERT OR IGNORE INTO games(gameID, day,Team1Abbr,Team2Abbr,Team1Score,Team2Score) VALUES (?,?,?,?,?,?)",
                    games)


def insert_player_for_game(player_data):
    pass


def insert_game(game_data, game_date):
    pass
