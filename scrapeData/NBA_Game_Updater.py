import random
import urllib.request
import urllib.error
import time
import re
import sys
import datetime

from bs4 import BeautifulSoup

user_agents = ["Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
               "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
               "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)", "NokiaE66/GoBrowser/2.0.297",
               "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1",
               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
               "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:2.0) Treco/20110515 Fireweb Navigator/2.4",
               "Googlebot", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0",
               "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0", ]


def convert_str_to_tuple(str_format, data):
    unformatted_string = str_format.replace("?", "{}")
    formatted_string = unformatted_string.format(*data)
    parameters = formatted_string.split(",")
    return tuple(parameters)


def get_response_time_of_url(num_pings, url):
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


# median_response_time_of_server = get_response_time_of_url(50, "http://espn.go.com/nba/team/_/name/mia")


def get_team_abbreviations():
    team_urls = [
        "bkn", "nyk", "phi", "tor", "gs", "lac",
        "lal", "phx", "sac", "chi", "cle", "det",
        "ind", "mil", "dal", "hou", "mem", "no",
        "sa", "atl", "cha", "mia", "orl", "wsh",
        "den", "min", "okc", "por", "utah", "bos"
    ]
    return team_urls


def get_roster_for_team(team_abbreviation: str, season_year: str):
    team_roster_url = "http://espn.go.com/nba/team/roster/_/name/" + team_abbreviation
    roster_soup = BeautifulSoup(urllib.request.urlopen(team_roster_url, timeout=100).read())

    player_rows = roster_soup.find_all('tr', class_=re.compile('player-46-'))

    def convert_preformatted_height_to_inches(height: str):
        ftpos = height.find("-")
        inches = int(height[:ftpos]) * 12 + int(height[ftpos + 1:])
        return inches

    players_data = []

    for row in player_rows:
        data = row.find_all('td')
        player_link = data[1].find('a')
        player_id = player_link.get('href').replace("http://espn.go.com/nba/player/_/id/")
        pos = player_id.find("/")
        player_id = player_id[:pos]
        name = data[1].getText()
        position = data[2].getText()
        height = convert_preformatted_height_to_inches(data[4].getText())
        weight = data[5].getText()
        salary = data[7].getText().replace("$", "").replace(",", "")

        player_data = (player_id, position, team_abbreviation, name, height, weight, salary, season_year)
        players_data.append(player_data)
    return players_data


def get_game_ids(team_abbreviation: str, season_year: str):
    print("getting schedule for " + team_abbreviation)
    team_schedule = "http://espn.go.com/nba/team/schedule/_/name/" + team_abbreviation + "/year/" + season_year + "/"
    preseason_url = team_schedule + "seasontype/1/"
    midseason_url = team_schedule + "seasontype/2/"
    postseason_url = team_schedule + "seasontype/3/"

    months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'June': 6, 'July': 7, 'Aug': 8,
              'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

    def extract_date(date_string: str):
        date = date_string.split(",")[1]
        split_date = date.split(" ")[1:]
        month = split_date[0]
        day = split_date[1]
        month_number = months[month]
        day_number = int(day)
        year_number = int(season_year) + (-1 if month_number > 6 else 0)
        date_time = datetime.datetime(year_number, month_number, day_number)

        return date_time

    def scrape_url_for_gameids(url: str):
        game_ids = []
        soup = BeautifulSoup(urllib.request.urlopen(url, timeout=150).read())
        game_rows = soup.find_all('tr', class_=re.compile('team-46-'))
        for game_row in game_rows:
            game_tds = game_row.find_all('td')
            date_td = game_tds[0]
            game_date = extract_date(date_td.getText())
            game_id = -1
            game_links = game_row.find_all('a')
            for link in game_links:
                url = link.get("href")
                if (url.find("/nba/recap?id=") != -1):
                    game_id = url[-9:]
            game_ids.append((game_date, int(game_id)))
        return game_ids

    preseason_games = scrape_url_for_gameids(preseason_url)
    midseason_games = scrape_url_for_gameids(midseason_url)
    postseason_games = scrape_url_for_gameids(postseason_url)

    return preseason_games + midseason_games + postseason_games


def scrape_gameid(game_id: str, attempt: int=0):
    if (attempt > 4):
        return
    try:
        url = "http://espn.go.com/nba/boxscore?gameId=" + str(game_id)
        UA_Header = {}
        UA_Header["User-Agent"] = str(random.choice(user_agents))
        request = urllib.request.Request(url, headers=UA_Header)
        soup = BeautifulSoup(urllib.request.urlopen(request, timeout=15000).read())

        matchup = soup.find('div', {"class": "competitors"})
        if not matchup:
            matchup = soup.find('div', {"class": "competitors "})

        away_team = matchup.find('div', {"class": "team away"})
        away_score = away_team.find('div', {"class": "score"}).getText()
        away_info = away_team.find('div', {"class": "team-info"})
        away_team_link = away_info.find('a').get('href')
        away_team_abbr = away_team_link.replace("/nba/team/_/name/", "")

        home_team = matchup.find('div', {"class": "team home"})
        home_score = home_team.find('div', {"class": "score"}).getText()
        home_info = home_team.find('div', {"class": "team-info"})
        home_team_link = home_info.find('a').get('href')
        home_team_abbr = home_team_link.replace("/nba/team/_/name/", "")

        def parse_row_data(data_row):
            row_class = data_row.get("class")
            if (row_class == ['highlight']):
                pass
            else:
                name_td = data_row.find('td', {'class': 'name'})
                player_data = {
                    'player_id': int(name_td.find('a').get('href').replace("http://espn.go.com/nba/player/_/id/", "")),
                    'name': name_td.find('a').getText()
                }

                injured = data_row.find('td', {'class': 'dnp'})
                if not injured:
                    fg = data_row.find('td', {'class': "fg"}).getText()
                    ft = data_row.find('td', {'class': "ft"}).getText()
                    threes = data_row.find('td', {'class': "3pt"}).getText()

                    player_data.update({
                        'min': int(data_row.find('td', {'class': "min"}).getText()),
                        'fgm': int(fg.split("-")[0]),
                        'fga': int(fg.split("-")[1]),
                        '3ptm': int(threes.split("-")[0]),
                        '3pta': int(threes.split("-")[1]),
                        'ftm': int(ft.split("-")[0]),
                        'fta': int(ft.split("-")[1]),
                        'oreb': int(data_row.find('td', {'class': "oreb"}).getText()),
                        'dreb': int(data_row.find('td', {'class': "dreb"}).getText()),
                        'reb': int(data_row.find('td', {'class': "reb"}).getText()),
                        'ast': int(data_row.find('td', {'class': "ast"}).getText()),
                        'stl': int(data_row.find('td', {'class': "stl"}).getText()),
                        'blk': int(data_row.find('td', {'class': "blk"}).getText()),
                        'to': int(data_row.find('td', {'class': "to"}).getText()),
                        'pf': int(data_row.find('td', {'class': "pf"}).getText()),
                        'plus_minus': int(data_row.find('td', {'class': "plusminus"}).getText()),
                        'pts': int(data_row.find('td', {'class': "pts"}).getText()),
                        'injured': False
                    })
                else:
                    player_data['injured'] = True
                return player_data

        def parse_table_data(team_data):
            tbodies = team_data.find_all('tbody')
            table_rows = [tbody.find_all('tr') for tbody in tbodies]
            rows = [item for sublist in table_rows for item in sublist]
            rows = [parse_row_data(row) for row in rows]
            return rows

        away_team_data = soup.find('div', {'class': 'gamepackage-home-wrap'})
        home_team_data = soup.find('div', {'class': 'gamepackage-home-wrap'})
        away_team_results = parse_table_data(away_team_data)
        home_team_results = parse_table_data(home_team_data)

        results = {
            'home_score': int(home_score),
            'away_score': int(away_score),
            'home_team_name': home_team_abbr,
            'away_team_name': away_team_abbr,
            'game_id': game_id,
            'home_team': home_team_results,
            'away_team': away_team_results
        }
        return results
    except:
        scrape_gameid(game_id, attempt + 1)

# TODO: MAKE IT RIGHT:

# SEATTLE SUPERSONICS CASE: 280408006
