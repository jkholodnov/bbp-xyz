import urllib
from bs4 import BeautifulSoup
import re


def updateRoster(Queries, rosterURL):
    try:
        soup = BeautifulSoup(
            urllib.request.urlopen(rosterURL, timeout=100).read())
        player_rows = soup.find_all('tr', class_=re.compile('player-46-'))
        player_infos = []
        for row in player_rows:
            data = row.find_all('td')

            player_Info = []

            playerLink = data[1].find('a')
            playerID = playerLink.get('href')
            base = "http://espn.go.com/nba/player/_/id/"
            playerID = playerID.replace(base, "")
            pos = playerID.find("/")
            playerID = playerID[:pos]

            Name = data[1].getText()

            playerPosition = data[2].getText()

            teamID = rosterURL.replace(
                "http://espn.go.com/nba/team/roster/_/name/", "")
            teamID = teamID.replace("/", "")
            teamID = teamID.replace("nor", "no")
            teamID = teamID.replace("uth", "utah")
            teamID = teamID.replace("gsw", "gs")
            teamID = teamID.replace("was", "wsh")
            teamID = teamID.replace("sas", "sa")
            teamID = teamID.replace("pho", "phx")

            preformatted_ht = str(data[4].getText())
            ftpos = preformatted_ht.find("-")
            inches = int(
                preformatted_ht[:ftpos]) * 12 + int(preformatted_ht[ftpos + 1:])
            weight = data[5].getText()
            salary = data[7].getText()
            salary = salary.replace("$", "")
            salary = salary.replace(",", "")

            player_Info.append(playerID)
            player_Info.append(playerPosition)
            player_Info.append(teamID)
            player_Info.append(Name)
            player_Info.append(inches)
            player_Info.append(weight)
            player_Info.append(salary)
            player_Info.append(str(YEAR))

            sqlformat = "?,?,?,?,?,?,?,?"
            sqlupdate = convert_Str_To_Tuple(sqlformat, player_Info)

            player_infos.append(player_Info)

        Queries.put(["Players", player_infos])
    except:
        pass