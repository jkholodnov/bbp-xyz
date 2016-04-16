from scrapeData import NBA_Game_Updater as nba
from save_data import save_game_data as db


def main(year: str):
    teams = nba.get_team_abbreviations()

    team_game_ids_map = {team: nba.get_game_ids(team, year) for team in teams}

    all_game_ids = set()
    for games in team_game_ids_map:
        # print(games)
        all_game_ids.update(team_game_ids_map[games])

    game_dbos = []
    for game in all_game_ids:
        game_date = game[0]
        game_id = game[1]
        game_data = nba.scrape_gameid(game_id, 0)
        if not game_data:
            print("Game failed to scrape. {}".format(game_id))
        else:
            game_dbos.append([
                game_id,
                game_date,
                game_data['home_team_name'],
                game_data['away_team_name'],
                game_data['home_score'],
                game_data['away_score']
            ])

    db.save_games(game_dbos)
    def generate_stat_dbo(player_stat: dict):
        pass

    print((game_id, game_date, game_data))
    print("#")

    # print(len(all_game_ids))
    # print(len(all_game_ids))
    # print(all_game_ids.pop())
    # team_dates_and_ids = team_game_ids_map['bos']

    #


main("2016")
if __name__ == "main":
    main("2016")
