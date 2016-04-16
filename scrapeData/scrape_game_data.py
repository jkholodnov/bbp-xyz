import random

from bs4 import BeautifulSoup

from scrapeData.NBA_Game_Updater import user_agents, urllib

def scrape_gameid(game_id, attempt):
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
                    'player_id': name_td.find('a').get('href').replace("http://espn.go.com/nba/player/_/id/", ""),
                    'name': name_td.find('a').getText()
                }

                injured = data_row.find('td', {'class': 'dnp'})
                if not injured:
                    fg = data_row.find('td', {'class': "fg"}).getText()
                    ft = data_row.find('td', {'class': "ft"}).getText()
                    threes = data_row.find('td', {'class': "3pt"}).getText()

                    player_data.update({
                        'min': data_row.find('td', {'class': "min"}).getText(),
                        'fgm': fg.split("-")[0],
                        'fga': fg.split("-")[1],
                        '3ptm': threes.split("-")[0],
                        '3pta': threes.split("-")[1],
                        'ftm': ft.split("-")[0],
                        'fta': ft.split("-")[1],
                        'oreb': data_row.find('td', {'class': "oreb"}).getText(),
                        'dreb': data_row.find('td', {'class': "dreb"}).getText(),
                        'reb': data_row.find('td', {'class': "reb"}).getText(),
                        'ast': data_row.find('td', {'class': "ast"}).getText(),
                        'stl': data_row.find('td', {'class': "stl"}).getText(),
                        'blk': data_row.find('td', {'class': "blk"}).getText(),
                        'to': data_row.find('td', {'class': "to"}).getText(),
                        'pf': data_row.find('td', {'class': "pf"}).getText(),
                        'plus_minus': data_row.find('td', {'class': "plusminus"}).getText(),
                        'pts': data_row.find('td', {'class': "pts"}).getText(),
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
            'home_score': home_score,
            'away_score': away_score,
            'home_team_name': home_team_abbr,
            'away_team_name': away_team_abbr,
            'game_id': game_id,
            'home_team': home_team_results,
            'away_team': away_team_results
        }
        return results
    except:
        scrape_gameid(game_id, attempt + 1)
