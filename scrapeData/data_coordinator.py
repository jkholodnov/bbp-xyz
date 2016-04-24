from multiprocessing import Queue

from scrapeData import NBA_Game_Updater as nba
from save_data import save_game_data as db
from fixData import teamElo


def main(year: str):
    teams = nba.get_team_abbreviations()


    rosters = [nba.get_roster_for_team(team, year) for team in teams]
    rosters_dbos = [item for sublist in rosters for item in sublist]

    db.save_rosters(rosters_dbos)


    team_game_ids_map = {team: nba.get_game_ids(team, year) for team in teams}

    scraped_game_ids = []
    for games in team_game_ids_map:
        scraped_game_ids.extend(team_game_ids_map[games])

    retrieved_game_ids = set(db.get_game_ids())
    retrieved_game_ids.add(-1)

    filtered_games = []
    for game in scraped_game_ids:
        if game[1] not in retrieved_game_ids:
            filtered_games.append(game)

    print("Games Found needing scraping: " + str(len(filtered_games)))

    def generate_player_data_dbos(game_id: int, team_name: str, players: list):
        player_dbos = []
        for player in players:
            if not player:
                pass
            else:
                injured = player['injured']
                player_dbo = []
                player_dbo.append(game_id)
                player_dbo.append(team_name)
                player_dbo.append(player['player_id'])
                player_dbo.append(player['name'])
                if injured:
                    player_dbo = player_dbo.extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, True])
                    player_dbos.append(player_dbo)
                else:
                    player_dbo.extend([
                        player['min'],
                        player['fgm'],
                        player['fga'],
                        player['3ptm'],
                        player['3pta'],
                        player['ftm'],
                        player['fta'],
                        player['oreb'],
                        player['dreb'],
                        player['reb'],
                        player['ast'],
                        player['stl'],
                        player['blk'],
                        player['to'],
                        player['pf'],
                        player['plus_minus'],
                        player['pts'],
                        str(player['injured'])  ## Make it pull the injury text.. not just rpelace with bool
                    ])
                    player_dbos.append(player_dbo)

        return player_dbos

    def save_game_and_players(game):
        game_date = game[0]
        game_id = game[1]
        game_data = nba.scrape_gameid(game_id, 0)

        if not game_data:
            print("\nGame failed to scrape. {}".format(game_id))
        else:
            game_insert_array = [
                game_id,
                game_date,
                game_data['home_team_name'],
                game_data['away_team_name'],
                game_data['home_score'],
                game_data['away_score']
            ]
            home_team_players = generate_player_data_dbos(game_id, game_data['away_team_name'], game_data['away_team'])
            away_team_players = generate_player_data_dbos(game_id, game_data['home_team_name'], game_data['home_team'])

            game_info = {"game": game_insert_array, "home_team": home_team_players, "away_team": away_team_players}
            db.save_game(game_info)

    save_game_results = {game_id: save_game_and_players(game_id) for game_id in filtered_games}

    teamElo.generateElo()


main("2016")
if __name__ == "main":
    main("2016")
