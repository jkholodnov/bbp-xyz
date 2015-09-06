import sys
from queue import *
from threading import Thread
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
from statistics import mean, stdev

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

    keys = ["Minutes","FG%","3%","FT%","Oreb","Dreb","Assist","Steal","Block","Turnover","Points"]
    meanSDs = {}
    for key in keys:
        meanSD = {}
        query = "SELECT statMean, statStdev FROM statistics WHERE statID == \"{0}\";".format(key)
        cur.execute(query)
        x = cur.fetchone()
        meanSD["mean"] = x[0]
        meanSD["stdev"] = x[1]
        meanSDs[key] = meanSD
    # print(meanSDs)

    def floatValidate(statString):
        if(statString == None):
            return 0
        else:
            return float(statString)


    for player in players:
        playerStatQuery = "SELECT avg(minutes), avg(fgm), avg(fga), avg(tpm), avg(tpa), avg(ftm), avg(fta), avg(oreb), avg(dreb), avg(assist), avg(steal), avg(block), avg(turnover), avg(points) FROM gamedata WHERE playerID ==" + str(player["id"]) + ";"
        cur.execute(playerStatQuery)
        playerStats = cur.fetchone()
        player["stats"] = {}

        rawStats = {}

        rawStats["Minutes"] = round(floatValidate(playerStats[0]),2)

        if(playerStats[2] != None and float(playerStats[2]) != 0):
            rawStats["FG%"] = round(float(playerStats[1]) / float(playerStats[2]),2)
        else:
            rawStats["FG%"] = 0

        if(playerStats[2] != None and float(playerStats[4]) != 0):
            rawStats["3%"] = round(float(playerStats[3]) / float(playerStats[4]),2)
        else:
            rawStats["3%"] = 0

        if(playerStats[2] != None and float(playerStats[6]) != 0):
            rawStats["FT%"] = round(float(playerStats[5]) / float(playerStats[6]),2)
        else:
            rawStats["FT%"] = 0

        rawStats["Oreb"] = round(floatValidate(playerStats[7]), 2)
        rawStats["Dreb"] = round(floatValidate(playerStats[8]), 2)
        rawStats["Assist"] = round(floatValidate(playerStats[9]), 2)
        rawStats["Steal"] = round(floatValidate(playerStats[10]), 2)
        rawStats["Block"] = round(floatValidate(playerStats[11]), 2)
        rawStats["Turnover"] = round(floatValidate(playerStats[12]), 2)
        rawStats["Points"] = round(floatValidate(playerStats[13]), 2)

        for key in keys:
            rawValue = rawStats[key]
            mean = meanSDs[key]["mean"]
            stdev = meanSDs[key]["stdev"]

            diff = rawValue - mean
            numSDs = diff / stdev
            if(rawValue == 0):
                numSDs = 0
            
            playerIndividualStats = {}
            playerIndividualStats["numSDs"] = round(numSDs,2)
            playerIndividualStats["rawValue"] = rawValue
            player["stats"][key] = playerIndividualStats

    response["players"] = players
    print(response)
    return json.dumps(response)

analyzeTeam("chi")