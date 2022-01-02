##https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/commonteamyears.md

from nba_api.stats.endpoints import commonteamyears
from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.endpoints import boxscoresummaryv2
# from nba_api.stats.library.parameters import Season
import pandas as pd
from datetime import datetime
import time


def nba_teams_list():
    #logging and printing
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting team list function\n")
    f.close()

    teams = commonteamyears.CommonTeamYears(league_id='00')
    df = teams.get_data_frames()[0]
    print(df)
    return(df)

def nba_team_games(teams):
    # print(teams[4].str.slice(start=2))
    # teams['MAX_YEAR_ABBR'] = teams.MAX_YEAR.str.slice(start=-2)
    # teams['MIN_YEAR_ABBR'] = teams.MIN_YEAR.str.slice(start=-2)
    gamelog = pd.DataFrame(columns = ['Team_ID','Game_ID','GAME_DATE','MATCHUP','WL','W','L',
                                    'W_PCT','MIN','FGM','FGA','FG_PCT','FG3M','FG3A','FG3_PCT',
                                    'FTM','FTA','FT_PCT','OREB','DREB','REB','AST','STL','BLK',
                                    'TOV','PF','PTS'])

    for row in teams.itertuples(index=False):
        team = row[teams.columns.get_loc('TEAM_ID')]
        min = row[teams.columns.get_loc('MIN_YEAR')]
        max = row[teams.columns.get_loc('MAX_YEAR')]

        #logging and printing
        f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
        f.write("starting: " + str(team) + "\n")
        f.write("minimum: " + str(min) + "\n")
        f.write("maximum: " + str(max) + "\n")
        f.close()
        print('starting: ' + str(team))
        print('minimum year: ' + str(min))
        print('maximum year: ' + str(max))

        for x in range(int(min)-1, int(max)+1):
            print(x)

            games = teamgamelog.TeamGameLog(season=x, season_type_all_star='Regular Season', team_id=team)
            df = games.get_data_frames()[0]
            gamelog = gamelog.append(df,ignore_index=True)
            
            #delaying 10 seconds to avoid api throttle limits
            time.sleep(10)

        gamelog.to_csv(str(team)+'_'+'teamGameLog.csv')
        
        #logging and printing
        print('finishing team: ' + str(team))
        f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
        f.write('finishing team: ' + str(team) + "\n")
        f.close()
    

if __name__ == "__main__":
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "w")
    f.write("starting historical_downloader at: " + str(datetime.now()) + "\n")
    f.close()

    teams = nba_teams_list()
    # print(teams)
    # data = [['00','1610612737',2016,2017,'ATL'] , ['00','1610612738',2017,2019,'BOS']]
    # teams = pd.DataFrame(data, columns = ['LEAGUE_ID','TEAM_ID','MIN_YEAR','MAX_YEAR','ABBREVIATION'])

    nba_team_games(teams)
