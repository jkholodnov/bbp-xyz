import sqlite3 as lite
con = lite.connect('predict.db')

with con:
    cur = con.cursor()

    cur.execute(
        "CREATE TABLE games(gameID INT PRIMARY KEY NOT NULL, " +
                            "day DATE, Team1Abbr TEXT, Team2Abbr TEXT, Team1Score INT, " +
                            "Team2Score INT, Team1ELO INT, Team2ELO INT)")
    cur.execute(
        "CREATE TABLE players(playerID INTEGER PRIMARY KEY NOT NULL, " +
                            "position TEXT, teamID TEXT, Name TEXT, height INT, "+
                            "weight INT, salary INT, season INT)")
    cur.execute(
        "CREATE TABLE gameData(gameID INT, teamID TEXT, playerID INT, "+
                                "Name TEXT, minutes INT, fgm INT, " +
                                "fga INT, tpm INT, tpa INT, ftm INT, "+
                                "fta INT, oreb INT, dreb INT, " +
                                "reb INT, assist INT, steal INT, "+
                                "block INT, turnover INT, fouls INT,"+
                                " plus_minus INT, points INT, "+
                                "injury TEXT, pir FLOAT, npr FLOAT, "+
                                "UNIQUE(gameID, playerID))")

    cur.execute(
        "CREATE TABLE teams(teamID TEXT, currentELO FLOAT)"
        )


con.commit()
con.close()
