from celery import Celery
import scrapeData.NBA_Game_Updater as NBA_Game_Updater
from scrapeData import NBA_Game_Updater
from fixData import teamElo, meanSD
#import scrape.create_tables_nba as create_tables

app = Celery('APItasks', broker='amqp://127.0.0.1:5000')

@app.task
def get_data_for_one_season(year):
    NBA_Game_Updater.main(year)
    return "Successfully grabbed all games for the " + str(year) + " season."
    teamElo.generate_Elo()
    meanSD.calculateMeanAndSDs()

@app.task
def get_data_for_all_seasons():
    years=[
    '2003',
    '2004',
    '2005',
    '2006',
    '2007',
    '2008',
    '2009',
    '2010',
    '2011',
    '2012',
    '2013',
    '2014',
    '2015']

    for year in years:
        NBA_Game_Updater.main(year)
    generate_Elo()
    meanSD.calculateMeanAndSDs()
    return "Successfully grabbed all game for all seasons!"

@app.task
def generate_Elo_For_New_Games():
    teamElo.generateElo()
    return "Generating ELO."