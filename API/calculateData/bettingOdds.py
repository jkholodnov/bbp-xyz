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
import json

def predictGame(homeTeam, awayTeam):
    con = lite.connect('predict.db', isolation_level=None)
    cur = con.cursor()

    team1query = "SELECT currentELO FROM teams WHERE teamID = \'" + str(awayTeam) + "\';"
    team2query = "SELECT currentELO FROM teams WHERE teamID = \'" + str(homeTeam) + "\';"

    cur.execute(team1query)
    awayTeamElo = cur.fetchone()[0]
    cur.execute(team2query)
    homeTeamElo = cur.fetchone()[0]

    eloDifference = homeTeamElo - awayTeamElo
    print(homeTeamElo)
    print(awayTeamElo)
    query = "SELECT count(*) FROM games WHERE (team2elo - team1elo) > " + str(eloDifference - 3) + " AND (team2elo - team1elo) < " + str(eloDifference + 3) + ";"
    cur.execute(query)
    totalNumberOfGames = cur.fetchone()[0]
    print(totalNumberOfGames)

    query = "SELECT count(*) FROM games WHERE (team2elo - team1elo) > " + str(eloDifference - 3) + " AND (team2elo - team1elo) < " + str(eloDifference + 3) + " AND (team2Score > team1Score);"
    cur.execute(query)
    correctGames = cur.fetchone()[0]
    print(correctGames)

    percentWin = correctGames / totalNumberOfGames


    response = {}
    response["homeRating"] = round(homeTeamElo,2)
    response["awayRating"] = round(awayTeamElo,2)
    response["homeWinPercent"] = round(percentWin,4)
    response["awayWinPercent"] = round(1-percentWin,4)

    def determine_required_return_per_win(winPct):
        lossPct = 1-winPct
        return 100 * ((1-winPct) / winPct)

    homeTeamRequiredReturnPerWin = round(determine_required_return_per_win(response["homeWinPercent"]),4)
    awayTeamRequiredReturnPerWin = round(determine_required_return_per_win(response["awayWinPercent"]),4)

    if homeTeamRequiredReturnPerWin > 100:
        #you better be making more than 100 per bet, because you have a 50% chance or greater to lose the bet.
        response["homeMoneyLine"] = "+" + str(homeTeamRequiredReturnPerWin)
    else:
        response["homeMoneyLine"] = "-" + str(10000/homeTeamRequiredReturnPerWin)
    

    if awayTeamRequiredReturnPerWin > 100:
        response["awayMoneyLine"] = "+" + str(awayTeamRequiredReturnPerWin)
    else:
        response["awayMoneyLine"] = "-" + str(10000/awayTeamRequiredReturnPerWin)

    return json.dumps(response)
