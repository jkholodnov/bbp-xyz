import sqlite3 as lite
from statistics import mean, stdev


def calculateMeanAndSDs():
    con = lite.connect('../predict.db', isolation_level=None)
    cur = con.cursor()
    overallStatQuery = "SELECT minutes, fgm, fga, tpm, tpa, ftm, fta, oreb, dreb, assist, steal, block, turnover, points FROM gamedata WHERE injury =='NULL';"
    cur.execute(overallStatQuery)
    overallStats = cur.fetchall()

    minutes = []
    fgpct = []
    tppct = []
    ftpct = []
    oreb = []
    dreb = []
    assist = []
    steal = []
    block = []
    turnover = []
    points = []

    for stat in overallStats:
        minutes.append(stat[0])

        # DONT KNOW PYTHON TERNARY... THIS IS JUST DIVIDE BY 0 CHECKS.
        fgpct.append(0) if float(stat[2] == 0) else fgpct.append(float(stat[1]) / float(stat[2]))
        tppct.append(0) if float(stat[2] == 0) else tppct.append(float(stat[3]) / float(stat[4]))
        ftpct.append(0) if float(stat[2] == 0) else ftpct.append(float(stat[5]) / float(stat[6]))

        oreb.append(stat[7])
        dreb.append(stat[8])
        assist.append(stat[9])
        steal.append(stat[10])
        block.append(stat[11])
        turnover.append(stat[12])
        points.append(stat[13])

    def clean_getMeanStdev(dataArray, dataKey):
        cleaned_data = [0.0 if not data else float(data) for data in dataArray]
        dataMean = mean(cleaned_data)
        dataStdev = stdev(cleaned_data, dataMean)
        response = {
            "dataKey": dataKey,
            "mean": dataMean,
            "stdev": dataStdev
        }
        return response

    meanSDs = []
    meanSDs.append(clean_getMeanStdev(minutes, "Minutes"))
    meanSDs.append(clean_getMeanStdev(fgpct, "FG%"))
    meanSDs.append(clean_getMeanStdev(tppct, "3%"))
    meanSDs.append(clean_getMeanStdev(ftpct, "FT%"))
    meanSDs.append(clean_getMeanStdev(oreb, "Oreb"))
    meanSDs.append(clean_getMeanStdev(dreb, "Dreb"))
    meanSDs.append(clean_getMeanStdev(assist, "Assist"))
    meanSDs.append(clean_getMeanStdev(steal, "Steal"))
    meanSDs.append(clean_getMeanStdev(block, "Block"))
    meanSDs.append(clean_getMeanStdev(turnover, "Turnover"))
    meanSDs.append(clean_getMeanStdev(points, "Points"))

    for meanSD in meanSDs:
        query = "INSERT OR REPLACE INTO statistics VALUES(\"{0}\",{1},{2});".format(meanSD["dataKey"], meanSD["mean"],
                                                                                    meanSD["stdev"])
        cur.execute(query)
    con.commit()
