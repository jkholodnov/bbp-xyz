import urllib.request
import urllib.error
import sys
from queue import *
from threading import Thread
import bs4
from bs4 import BeautifulSoup
import sqlite3 as lite
from math import *
import time
import re
from socket import timeout
import errno
import random

def main(YEAR):
    user_agents = ["Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko","Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)","Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)","NokiaE66/GoBrowser/2.0.297","Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1","Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:2.0) Treco/20110515 Fireweb Navigator/2.4", "Googlebot", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0", "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0", ]
    
    def convert_Str_To_Tuple(str_format, data):
            unformatted_string = str_format.replace("?", "{}")
            formatted_string = unformatted_string.format(*data)
            parameters = formatted_string.split(",")
            return tuple(parameters)

    def get_Response_Time_of_URL(num_pings, url):
        response_times = []
        for i in range(num_pings):
            sys.stdout.write(".")
            sys.stdout.flush()
            starttime = time.time()
            urllib.request.urlopen("http://www.espn.go.com")
            endtime = time.time()
            response_time = endtime - starttime
            response_times.append(response_time)
        MRT = response_times[int(len(response_times) / 2)]
        print("]")
        return MRT

    def mainEntry():  # Get the page that holds all team url pages
        print("The year being scraped is: ",YEAR)
        con = lite.connect('predict.db', isolation_level=None)
        cur = con.cursor()

        result = Queue()  # The result of the algorithm onto the queue items
        players = Queue()

        rosters = []


        sys.stdout.write("Acquiring MRT [")
        sys.stdout.flush()

        MRT = get_Response_Time_of_URL(
                50, "http://espn.go.com/nba/team/_/name/mia")
        print ("MRT Acquired: " + str(MRT))

        def getTeamIDs(result, rosters):
            baseurl = "http://espn.go.com/nba/teams"
            content = urllib.request.urlopen(baseurl).read()
            soup = BeautifulSoup(content)
            team_Abbreviations = []
            y = "/nba/teams/schedule?team="
            for link in soup.find_all('a'):
                url = str(link.get('href'))
                if(url.find(y) != -1):
                    baseurl = "http://espn.go.com/"
                    teamABBR = url.replace(y, "")
                    team_Abbreviations.append(teamABBR)

            sys.stdout.write("Acquiring Teams:")
            sys.stdout.flush()
            
            threads = []
            start_of_first_parallel = time.time()

            for teamAbbrev in team_Abbreviations:
                print(teamAbbrev)
                teamabbr = teamAbbrev
                teamabbr = teamabbr.replace("nor", "no")
                teamabbr = teamabbr.replace("uth", "utah")
                teamabbr = teamabbr.replace("gsw", "gs")
                teamabbr = teamabbr.replace("was", "wsh")
                teamabbr = teamabbr.replace("sas", "sa")
                teamabbr = teamabbr.replace("pho", "phx")
                print(teamabbr)
                cur.execute("SELECT count(*) FROM teams WHERE teamid = \'" + str(teamabbr) + "\';")
                rows = cur.fetchall()
                if(rows[0][0] == 0):
                    print(rows)
                    cur.execute("INSERT INTO teams VALUES(\'" + str(teamabbr) + "\',1500.0)")
                    
                timestart = time.time()
                thread = Thread(
                    target=getTeamsGameIds, args=(teamAbbrev, result, rosters, 0))
                thread.daemon = True
                thread.start()
                threads.append(thread)
                while(time.time() - timestart < MRT):
                    spinlock = True

            for thread in threads:
                thread.join()
            end_of_first_parallel = time.time()
            print("Acquired gameIDs and Roster Urls in : " +
                  str(end_of_first_parallel - start_of_first_parallel) + " seconds.")

        getTeamIDs(result, rosters)

        def scrape_Games(rosters):
            retrievedGameIDs = set({})
            while not result.empty():
                gameId = result.get()
                retrievedGameIDs.add(str(gameId))

            cur.execute("SELECT gameID FROM games")
            rows = cur.fetchall()
            databaseGameIDs = set({})
            for row in rows:
                theid = str(row[0])
                databaseGameIDs.add(theid)

            print("Number of ids taken from parse: " + str(len(retrievedGameIDs)))
            print("Number of ids in db: " + str(len(databaseGameIDs)))

            newGameIDs = retrievedGameIDs.difference(databaseGameIDs)

            Queries = Queue()

            #spawn 10 database updater threads#
            db_updater_Threads = []
            for i in range(15):
                thread = Thread(target=updateDb, args=(i, Queries))
                db_updater_Threads.append(thread)
                thread.start()
            print("Database Updater Threads have been initialized.")

            print("The number of rosters to scrape is: " + str(len(rosters)))
            roster_Scraper_Threads = []
            for rosterURL in rosters:
                thread = Thread(
                    target=updateRoster, args=(Queries, rosterURL))
                thread.start()
                roster_Scraper_Threads.append(thread)
                time.sleep(MRT/2)

            for thread in roster_Scraper_Threads:
                thread.join()

            print("The number of games to scrape is: " + str(len(newGameIDs)))

            #spawn a ton of game scraper threads#

            start = time.time()
            i = 1
            game_Scraper_Threads = []
            for gameID in newGameIDs:
                sys.stdout.write("@")
                sys.stdout.flush()
                thread = Thread(target=scrapeGameData, args=(gameID, 0, Queries))
                thread.start()
                game_Scraper_Threads.append(thread)
                time.sleep(MRT/2)
                i += 1


            for thread in game_Scraper_Threads:
                thread.join()

            end = time.time()
            print("It took " + str(end - start) + " to scrape games.")

            Queries.put("TERMINATE")

            for thread in db_updater_Threads:
                thread.join()

            print("It took " + str(end - start) + " to scrape games and finish DB updates.")


            print("Database Updater Threads have been terminated.")
        scrape_Games(rosters)


    ##########################################################################
    ##########################################################################
    #############################
    # BEGIN  PARALLEL FUNCTIONS
    #############################
    ##########################################################################
    ##########################################################################

    def getTeamsGameIds(teamABBR, the_Game_IDs, roster_URLS, attempt):
        try:
            baseurl = "http://espn.go.com/nba/team/_/name/"
            ################################################
            #               Scrape pre-season              #
            ################################################
            # the_year must be a 4 digit integer
            rosters = "http://espn.go.com/nba/team/roster/_/name/" + teamABBR
            roster_URLS.append(rosters)
            schedules = "http://espn.go.com/nba/team/schedule/_/name/" + teamABBR + "/year/" + str(YEAR) + "/"



            ################################################
            #               Scrape pre season              #
            ################################################
            pre_season = schedules + "seasontype/1/"
            soup = BeautifulSoup(
                urllib.request.urlopen(pre_season, timeout=150).read())
            for link in soup.find_all('a'):
                thelink = str(link.get('href'))
                y = "/nba/recap?id="
                x = thelink.find(y)
                if(x != -1):
                    gameID = thelink[-9:]
                    the_Game_IDs.put(gameID)

            ################################################
            #           Scrape regular season              #
            ################################################
            regular_season = schedules + "seasontype/2/"
            soup = BeautifulSoup(
                urllib.request.urlopen(regular_season, timeout=150).read())
            for link in soup.find_all('a'):
                thelink = str(link.get('href'))
                y = "/nba/recap?id="
                x = thelink.find(y)
                if(x != -1):
                    gameID = thelink[-9:]
                    the_Game_IDs.put(gameID)
            endtime = time.time()
            ################################################
            #               Scrape POST season             #
            ################################################
            post_season = schedules + "seasontype/3/"
            soup = BeautifulSoup(
                urllib.request.urlopen(post_season, timeout=150).read())
            for link in soup.find_all('a'):
                thelink = str(link.get('href'))
                y = "/nba/recap?id="
                x = thelink.find(y)
                if(x != -1):
                    gameID = thelink[-9:]
                    the_Game_IDs.put(gameID)
        # THREAD EXCEPTION HANDLING
        except timeout:
            print("URL timeout. Restarting.")
            getTeamsGameIds(teamABBR, the_Game_IDs, rosterIDs, attempt + 1)
        except urllib.error.URLError as e:
            print("URL Timeout Error. Restarting")
            getTeamsGameIds(teamABBR, the_Game_IDs, rosterIDs, attempt + 1)
        except ConnectionResetError as e:
            print("Connection got reset. Calling back.")
            getTeamsGameIds(teamABBR, the_Game_IDs, rosterIDs, attempt + 1)

        except OSError as e:
            if(e.errno == errno.EHOSTUNREACH):
                print("Host Unreachable. Trying again.")
                getTeamsGameIds_In_Parallel(teamABBR, the_Game_IDs, attempt + 1)
            if(e.errno == errno.ECONNRESET):
                print("Host hung up. Calling back.")
                getTeamsGameIds_In_Parallel(teamABBR, the_Game_IDs, attempt + 1)
            else:
                print(e.args)
                print(e[0])
        except Exception as e:
            print("idk what just happened.")
            print(e.arg)


    ##########################################################################
    #                                                                                            #
    #                       Begin Scraping the Games for their Content.                          #
    #               We have 15 db updater threads and numGameIDs scraper threads                 #
    #                                                                                            #
    ##########################################################################
    def updateRoster(Queries, rosterURL):
        try:
            print("BLAH",rosterURL)
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
        except Exception as e:
            print(e)

    def updateDb(i, Queries):
        con = lite.connect('predict.db', isolation_level=None)
        cur = con.cursor()
        while(True):
            if(Queries.qsize == 0):
                pass
            else:
                data = Queries.get()
                if(data == "TERMINATE"):
                    Queries.put(data)
                    print("Database Updater thread " + str(i) + " has terminated.")
                    break
                try:
                    if(data[0] == "Players"):
                        cur.executemany(
                            "INSERT OR IGNORE INTO players VALUES(?,?,?,?,?,?,?,?)", data[1])
                    if(data[0] == "GameData"):
                        cur.executemany(
                            "INSERT OR IGNORE INTO gameData VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'NULL','NULL')", data[1])
                    if(data[0] == "Games"):
                        gameid = data[1][0]
                        cur.execute(
                            "INSERT OR IGNORE INTO games VALUES(?,?,?,?,?,?, 'NULL','NULL')", data[1])
                    con.commit()
                    sys.stdout.write(".")
                    sys.stdout.flush()


                except Exception as e:
                    print(e)
                    Queries.put(data)

    def scrapeGameData(gameID, attempt, Queries):
        #print(gameID)
        class player(object):
            def __init__(self):
                self.name = None
                self.position = None
                self.id = None
                self.salary = None
                self.height = None
                self.weight = None
                self.teamAbbr = None
                self.game_data = []

            def add_ID(self, x):
                self.id = x

            def add_data(self, x):
                self.game_data.append(x)

            def change_name(self, name):
                self.name = name

            def height_and_weight(self, ht, wt):
                self.weight = wt

            def sanitize_data(self):
                # This function takes the raw player data and extracts the name and
                # position from it.
                newData = []
                for i in range(len(self.game_data)):
                    position = str(self.game_data[i]).find(",")
                    # fix the name and position
                    if(position != -1):
                        if(self.position == None):
                            pos = str(self.game_data[i])[position + 1:]
                            temp_name = str(self.game_data[i])[:position]
                            temp_name = temp_name.replace("'", "")
                            temp_name = temp_name.replace("-", "")
                            self.name = temp_name
                            self.position = pos.replace(" ", "")
                    # put rest of data into dataset.
                    else:
                        g = self.game_data[i].find("-")
                        if(g != -1 and g > 0):
                            newData.append(self.game_data[i][:g])
                            newData.append(self.game_data[i][g + 1:])
                        else:
                            newData.append(self.game_data[i])
                self.game_data = newData

        gameData_Insert_Queries = []
        games_Insert_Queries = []

        if(attempt == 4):
            print("Maximum attempts reached for game id: " + str(gameID))
            return 0

        try:
            url = "http://espn.go.com/nba/boxscore?gameId=" + str(gameID)
            UA_Header = {}
            UA_Header["User-Agent"] = str(random.choice(user_agents))
            request = urllib.request.Request(url, headers = UA_Header)
            soup = BeautifulSoup(urllib.request.urlopen(request, timeout=100).read())
            # print(soup.getText())

            matchup = soup.find('div', {"class": "matchup"})
            if(not matchup):
                matchup = soup.find('div', {"class": "matchup "})

            firstteam = matchup.find('div', {"class": "team away"})
            secondteam = matchup.find('div', {"class": "team home"})

            team1link = firstteam.find('a')
            team2link = secondteam.find('a')

            firstinfo = firstteam.find('div', {"class": "team-info"})
            secondinfo = secondteam.find('div', {"class": "team-info"})

            team1scorespan = firstinfo.find('span')
            team2scorespan = secondinfo.find('span')

            team1score = team1scorespan.getText()
            team2score = team2scorespan.getText()

            months = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                      'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}
            date_div = soup.find('div', {"class": "game-time-location"})
            date = (date_div.find('p')).getText()
            x = date.find(",")
            if(x != -1):
                date = date[x + 2:]
            x = date.find(" ")
            if(x != -1):
                month = date[:x]
                day_year = date[x + 1:]
            else:
                print(date)
            x = day_year.find(",")
            if(x != -1):
                day = day_year[:x]
                year = day_year[x + 2:]
            else:
                print(day_year)

            if(len(str(day)) == 1):
                day = "0" + str(day)
            if(len(str(months[month])) == 1):
                themonth = "0" + str(months[month])
            else:
                themonth = str(months[month])

            date_of_game = str(year) + "-" + themonth + "-" + day

            team1abbr = ""
            team2abbr = ""
            g = "http://espn.go.com/nba/team/_/name/"
            if not team1link:
                team1Name = (firstinfo.find('h3')).getText()
                if team1Name.find("Bobcats") != -1:
                    team1abbr = "cha"
                elif team1Name.find("Nets") != -1:
                    team1abbr = "bkn"
                elif team1Name.find("Hornets") != -1:
                    team1abbr = "no"
            else:
                team1href = team1link.get('href')
                team1href = team1href.replace(g, "")
                x = team1href.find("/")
                team1abbr = team1href[:x]


            if not team2link:
                team2Name = (secondinfo.find('h3')).getText()
                if team2Name.find("Bobcats") != -1:
                    team2abbr = "cha"
                elif team2Name.find("Nets") != -1:
                    team2abbr = "bkn"
                elif team2Name.find("Hornets") != -1:
                    team2abbr = "no"
            else:
                team2href = team2link.get('href')
                team2href = team2href.replace(g, "")
                y = team2href.find("/")
                team2abbr = team2href[:y]
               

            tbodies = soup.find_all('tbody')

            if(len(tbodies) == 6):
                # print(gameID)
                bodies = []
                bodies.extend(tbodies[:2])
                bodies.extend(tbodies[3:5])
            else:
                bodies = []
                bodies.extend(tbodies[:])

            body_count = 0
            for body in bodies:
                trs = body.find_all('tr')
                for tr in trs:
                    playerLink = tr.find('a')
                    if playerLink is not None:
                        playerLink = str(playerLink.get('href'))
                        base_href = "http://espn.go.com/nba/player/_/id/"
                        uniques = playerLink.replace(base_href, "")
                        breakpoint = uniques.find("/")
                        playerID = uniques[:breakpoint]
                        tds = tr.find_all('td')
                        temporary_player = player()
                        temporary_player.add_ID(playerID)
                        for td in tds:
                            temporary_player.add_data(td.getText())
                        temporary_player.sanitize_data()
                        # we are on the first team
                        if(body_count < 2):
                            temporary_player.teamAbbr = team1abbr
                        else:
                            temporary_player.teamAbbr = team2abbr

                        query = []
                        query.append(gameID)
                        query.append(temporary_player.teamAbbr)
                        query.append(temporary_player.id)
                        query.append(temporary_player.name)

                        player_key = []
                        player_key.append(gameID)
                        player_key.append(temporary_player.id)

                        if(len(temporary_player.game_data) > 0 and len(temporary_player.game_data) < 5):
                            query.append(temporary_player.game_data[0])

                            sqlformat = "?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?"
                            sqlupdate = sqlupdate = convert_Str_To_Tuple(
                                sqlformat, query)
                            gameData_Insert_Queries.append(sqlupdate)

                        else:
                            for data in temporary_player.game_data:
                                query.append(data)

                            if(len(query) == 20):
                                sqlformat = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,NULL"
                                sqlupdate = convert_Str_To_Tuple(sqlformat, query)
                                gameData_Insert_Queries.append(sqlupdate)

                            elif(len(query) == 21):
                                sqlformat = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL"
                                sqlupdate = convert_Str_To_Tuple(sqlformat, query)
                                gameData_Insert_Queries.append(sqlupdate)

                body_count += 1

            #UPDATE THE GAMES TABLE IN DATABASE#
            to_insert = []
            to_insert.append(gameID)
            to_insert.append(date_of_game)
            to_insert.append(team1abbr)
            to_insert.append(team2abbr)
            to_insert.append(team1score)
            to_insert.append(team2score)
            gameData_Insert_Queries = tuple(gameData_Insert_Queries)

            Queries.put(['GameData', gameData_Insert_Queries])
            Queries.put(['Games', to_insert])

        # Thread Exception Handling
        except timeout:
            print("Timeout.")
            scrapeGameData(gameID, attempt + 1, Queries)

        except urllib.error.URLError as e:
            print(e.reason)
            #print("URL Timeout." + str())
            scrapeGameData(gameID, attempt + 1, Queries)

        except ConnectionResetError as e:
            print("Connection Reset by Host.")
            scrapeGameData(gameID, attempt + 1, Queries)

        except OSError as e:
            if(e.errno == errno.EHOSTUNREACH):
                print("Host Unreachable. Trying again.")
                scrapeGameData(gameID, attempt + 1, Queries)

            if(e.errno == errno.ECONNRESET):
                print("Host hung up. Calling back.")
                scrapeGameData(gameID, attempt + 1, Queries)
        except IndexError as e:
            print(str(e) + " " + str(gameID))


    mainEntry()
    return "Success!"
##########################################################################
#                                                                        #
#             Scrape Player Webpages for Weight+Height                   #
#                                                                        #
##########################################################################
if __name__ == '__main__':
    main()



#TODO: MAKE IT RIGHT:

#SEATTLE SUPERSONICS CASE: 280408006
