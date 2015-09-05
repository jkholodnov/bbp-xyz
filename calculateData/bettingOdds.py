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
from datetime import timedelta
from datetime import *
import statistics

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

def compareTeams(homeTeam, awayTeam, firstDay, dataPoints):
    con = lite.connect('predict.db', isolation_level=None)
    cur = con.cursor()

    def getTeamGameRankings(teamAbbr):
        query = "SELECT team1elo,day FROM games WHERE (team1abbr = \'" + str(teamAbbr) + "\') and (team1Abbr != '' AND team2Abbr != '') AND (day > \'" + firstDay + "\') ORDER BY day asc;"
        cur.execute(query)
        firsthalf = cur.fetchall()
        query = "SELECT team2elo,day FROM games WHERE (team2abbr = \'" + str(teamAbbr) + "\') and (team1Abbr != '' AND team2Abbr != '') AND (day > \'" + firstDay + "\') ORDER BY day asc;"
        cur.execute(query)
        secondhalf = cur.fetchall()
        allgames = firsthalf + secondhalf
    
        allgames = sorted(allgames, key=lambda game: game[1])   # sort by date

        return allgames

    homeGames = getTeamGameRankings(homeTeam);
    awayGames = getTeamGameRankings(awayTeam);

    def convert_str_to_datetime(dateString):
        year_month_day = dateString.split("-")
        return datetime(int(year_month_day[0]),int(year_month_day[1]),int(year_month_day[2]))

    firstDayHome = convert_str_to_datetime(homeGames[0][1])
    firstDayAway = convert_str_to_datetime(awayGames[0][1])

    if(firstDayHome < firstDayAway):
        earliestDay = firstDayHome
    else:
        earliestDay = firstDayAway

    lastDayHome = convert_str_to_datetime(homeGames[len(homeGames)-1][1])
    lastDayAway = convert_str_to_datetime(awayGames[len(awayGames)-1][1])

    if(lastDayHome > lastDayAway):
        latestDay = lastDayHome
    else:
        latestDay = lastDayAway

    def initializeDatesObject(dates):
        difference_in_seconds = latestDay - earliestDay
        delta_between_games = difference_in_seconds / dataPoints
        
        currentDate = earliestDay
        while(currentDate < latestDay):
            dateString = "%d-%02d-%02d" % (currentDate.year, currentDate.month, currentDate.day)
            dates[currentDate] = {
                "date": dateString
            }
            currentDate += delta_between_games

    dates = {}
    initializeDatesObject(dates)

    def getAverageRatingTimeSeries(earliestDay, latestDay, gameData, dataPoints, teamAbbreviation ):
        difference_in_seconds = latestDay - earliestDay
        delta_between_games = difference_in_seconds / dataPoints

        currentDate = earliestDay

        count = 0
        ratingSum = 0
        timeSeries = []
        
        earliestRating = gameData[0][0]
        for game in gameData:
            if(convert_str_to_datetime(game[1]) > currentDate):
                ratingSum += game[0]
                count += 1
                average = ratingSum / count
                timeSeries.append(round(average,1))
                x = dates[currentDate]
                x[teamAbbreviation] = round(average,2)
                currentDate += delta_between_games
                ratingSum = 0
                count = 0
            else:
                ratingSum += game[0]
                count += 1

        return timeSeries


    homeTeamTimeSeries = getAverageRatingTimeSeries(earliestDay, latestDay, homeGames, dataPoints, homeTeam)
    awayTeamTimeSeries = getAverageRatingTimeSeries(earliestDay, latestDay, awayGames, dataPoints, awayTeam)
    values = []
    for key,value in dates.items():
        values.append(value)

    values = sorted(values, key=lambda value: convert_str_to_datetime(value["date"]))
    
    ratings = {}
    ratings[homeTeam] = None
    ratings[awayTeam] = None
    for value in values:
        value["date"] = value["date"].replace("-","")
        try:
            ratings[homeTeam] = value[homeTeam]
        except KeyError:
            value[homeTeam] = ratings[homeTeam]

        try:
            ratings[awayTeam] = value[awayTeam]
        except KeyError:
            value[awayTeam] = ratings[awayTeam]

    return json.dumps(values)

#This might have a problem dealing with the current way salaries are stored in the database. 
#There can be multiple salaries for one player, thus we also need the year which we are analyzing... 
#For now hardcoded to be 2015.. fix?
def analyzeTeam(teamName):
    year = 2015
    response = {}
    response["teamName"] = teamName

    con = lite.connect('predict.db', isolation_level=None)
    cur = con.cursor()

    salaryQuery = "SELECT name,playerID,salary FROM players WHERE teamID == \'" + str(teamName) + "\' and season == '2015';"

    cur.execute(salaryQuery)
    x = cur.fetchall()
    players = []

    for salary in x:
        player = {}
        player["name"] = salary[0]
        player["id"] = salary[1]
        player["salary"] = salary[2]
        players.append(player)

    for player in players:
        playerStatQuery = "SELECT avg(minutes), avg((fgm/fga)), avg((tpm/tpa)), avg((ftm/fta)), avg(oreb), avg(dreb), avg(assist), avg(steal), avg(block), avg(turnover), avg(points) FROM gamedata WHERE playerID ==" + str(player["id"]) + ";"
        cur.execute(playerStatQuery)
        playerStats = cur.fetchone()
        player["stats"] = {}
        player["stats"]["minutes"] = playerStats[0]
        player["stats"]["fgPercent"] = playerStats[1]
        player["stats"]["threePercent"] = playerStats[2]
        player["stats"]["ftPercent"] = playerStats[3]
        player["stats"]["oreb"] = playerStats[4]
        player["stats"]["dreb"] = playerStats[5]
        player["stats"]["assist"] = playerStats[6]
        player["stats"]["steal"] = playerStats[7]
        player["stats"]["block"] = playerStats[8]
        player["stats"]["turnover"] = playerStats[9]
        player["stats"]["points"] = playerStats[10]

    response["players"] = players
    return json.dumps(response)