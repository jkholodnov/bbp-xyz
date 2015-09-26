from flask import Flask, request
from flask.ext.cors import CORS, cross_origin
from APItasks import get_data_for_one_season, get_data_for_all_seasons, generate_Elo_For_New_Games
from calculateData import bettingOdds
import os
app = Flask(__name__)
cors = CORS(app)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/tasks/scrape/season/<int:year>', methods=['POST'])
def get_all_games_for_one_season(year):
    #THIS SPAWNS A CELERY TASK
    get_data_for_one_season.delay(year)
    return "Successfully grabbed all games for the " + str(year) + " season."

@app.route('/tasks/scrape/season/all', methods=['POST'])
def get_all_games_ever():
    #THIS SPAWNS A CELERY TASK
    get_data_for_all_seasons.delay()
    return "Successfully grabbed all games for all seasons."

@app.route('/tasks/fix/elo', methods=['POST'])
def generate_Elo():
    #THIS SPAWNS A CELERY TASK
    generate_Elo_For_New_Games.delay()
    return "Successfully started ELO generation."

@app.route('/predict/games/')
def predict_Game():
    home = request.args.get('home')
    away = request.args.get('away')
    return bettingOdds.predictGame(home,away)

@app.route('/compare/teams/')
def compare_Teams():
    home = request.args.get('home')
    away = request.args.get('away')
    return bettingOdds.compareTeams(home,away,'2012-05-26',180)

@app.route('/analyze/team/<teamName>')
def analyze_Team(teamName):
    return bettingOdds.analyzeTeam(teamName)

@app.route('/initialize/analyze')
def get_Team_Analysis_Stats():
    print("HIT")
    x = bettingOdds.analyzeAllTeams()
    return bettingOdds.analyzeAllTeams()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
