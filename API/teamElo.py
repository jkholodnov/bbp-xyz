import urllib.request
import urllib.error
import sys
from queue import *
from threading import Thread
import bs4
from bs4 import BeautifulSoup
import sqlite3 as lite
import math
import time
import re
from socket import timeout
import errno
import random

class Team(object):
    def __init__(self,teamAbbreviation):
        self.Abbreviation = teamAbbreviation

class Player(object):
    def __init__(self,playerName):
        self.Name = playerName

def loadTeamsAndPlayers():
    con = lite.connect('predict.db', isolation_level=None)
    cur = con.cursor()
    
    Teams = {}

    cur.execute("SELECT distinct Team1Abbr FROM games;")
    teamAbbrsFromDb = cur.fetchall()

    del(teamAbbrsFromDb[-1])
    for team in teamAbbrsFromDb:
        newTeam = Team(team[0])
        currentElo = "SELECT currentElo FROM teams WHERE teamid = \'" + str(team[0]) + "\';"
        cur.execute(currentElo)
        elo = cur.fetchone()
        if(elo):
            newTeam.currentElo = elo[0]
        else:
            newTeam.currentElo = 1500.0
        Teams[newTeam.Abbreviation] = newTeam

    Players = {}

    cur.execute("SELECT DISTINCT(name) from gamedata;")
    players = cur.fetchall()
    for player in players:
        newPlayer = Player(player[0])
        Players[newPlayer.Name] = newPlayer

    teamsAndPlayers = {}
    teamsAndPlayers['Teams'] = Teams
    teamsAndPlayers['Players'] = Players

    return teamsAndPlayers

def loadGames():
    con = lite.connect('predict.db', isolation_level=None)
    cur = con.cursor()
    gameDays = "SELECT DISTINCT day FROM games order by day asc;";
    cur.execute(gameDays)
    gameDays = cur.fetchall()

    allGames = {}
    for day in gameDays:
        thisDaysGames = []
        query = "SELECT gameId, Team1Abbr, Team2Abbr, Team1Score, Team2Score, Team1ELO, Team2ELO FROM games where day = \'" + day[0] + "\';"
        cur.execute(query)
        games = cur.fetchall()
        for game in games:
            thisDaysGames.append(game)
        allGames[day[0]]=thisDaysGames
    return allGames

def generateElo():
    x = loadTeamsAndPlayers()
    Teams = x['Teams']
    Players = x['Players']
    allGames = loadGames()    
    databaseQueries = []

    def computeElo():
        con = lite.connect('predict.db', isolation_level=None)
        cur = con.cursor()
        daysWhereWeNeedToCalcElo = "SELECT DISTINCT(day) FROM games WHERE (team1elo = 'NULL' or team2elo = 'NULL') and (team1abbr != '' and team2abbr != '' ) order by day ASC;"
        cur.execute(daysWhereWeNeedToCalcElo)
        daysWhereWeNeedToCalcElo = cur.fetchall()
        for day in daysWhereWeNeedToCalcElo:
            games = allGames[day[0]]
            threads = []

            for game in games:
                thread = Thread(target=generateEloForGame, args=(game))
                thread.daemon = True
                thread.start()
                threads.append(thread)
                
            for thread in threads:
                thread.join()
                
    def generateEloForGame(*game):
        try:
            gameid = game[0]
            team1 = Teams[game[1]]
            team2 = Teams[game[2]]
            team1Score = game[3]
            team2Score = game[4]
            team1Elo = team1.currentElo
            team2Elo = team2.currentElo

            setEloAtStartOfGame = "UPDATE games SET team1ELO = " + str(team1Elo) + ", team2ELO = " + str(team2Elo) + " WHERE gameid = '" + str(gameid) + "';"
            databaseQueries.append(setEloAtStartOfGame);
            team1Expected = 1 / (1 + math.pow(10, (( team2Elo - team1Elo) / 400)))
            team2Expected = 1 / (1 + math.pow(10, (( team1Elo - team2Elo) / 400)))

            if(team1Score > team2Score):
                team1.currentElo += (50 * (1 - team1Expected))
                team2.currentElo += (50 * (0 - team2Expected))
            else:
                team1.currentElo += (50 * (0 - team1Expected))
                team2.currentElo += (50 * (1 - team2Expected))

        except Exception as e:
            pass
            #THE GODDAM SUPERSONICS MAN

    computeElo()
    con = lite.connect('predict.db', isolation_level=None)
    cur = con.cursor()
    
    print(len(databaseQueries))
    for query in databaseQueries:
        cur.execute(query)
    con.commit()

    for team in Teams.items():
        query = "UPDATE teams SET currentElo = " + str(team[1].currentElo) + " WHERE teamid = " + str(team[1].Abbreviation) + ";"
        cur.execute(query)
    con.commit()

if __name__ == '__main__':
    generateElo()