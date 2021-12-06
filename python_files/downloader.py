##https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/commonteamyears.md

from nba_api.stats.endpoints import commonteamyears
from nba_api.stats.endpoints import teamgamelog
# from nba_api.stats.library.parameters import Season
import pandas as pd


def nba_teams_list():
    teams = commonteamyears.CommonTeamYears(league_id='00')
    df = teams.get_data_frames()[0]
    print(df)
    return(df)

def nba_team_games(teams):
    # print(teams[4].str.slice(start=2))
    # teams['MAX_YEAR_ABBR'] = teams.MAX_YEAR.str.slice(start=-2)
    # teams['MIN_YEAR_ABBR'] = teams.MIN_YEAR.str.slice(start=-2)

    for row in teams.itertuples(index=False):
        team = row[teams.columns.get_loc('TEAM_ID')]
        min = row[teams.columns.get_loc('MIN_YEAR')]
        max = row[teams.columns.get_loc('MAX_YEAR')]

        for x in range(min, max+1):
            print(x)
            games = teamgamelog.TeamGameLog(season=x, season_type_all_star='Regular Season', team_id=team)
            df = games.get_data_frames()[0]
            # print(df)

        # print()
        # print(row[teams.columns.get_loc('TEAM_ID')], 
        #         row[teams.columns.get_loc('MIN_YEAR')],
        #         row[teams.columns.get_loc('MAX_YEAR')],
        #         row[teams.columns.get_loc('MIN_YEAR_ABBR')],
        #         row[teams.columns.get_loc('MAX_YEAR_ABBR')]
        #          )

    # games = teamgamelog.TeamGameLog(season='2021', season_type_all_star='Regular Season', team_id='1610612737')
    # df = games.get_data_frames()[0]
    # print(df)

if __name__ == "__main__":
#    teams = nba_teams_list()

    data = [['00','1610612737',2016,2017,'ATL'] , ['00','1610612738',2017,2019,'BOS']]
    teams = pd.DataFrame(data, columns = ['LEAGUE_ID','TEAM_ID','MIN_YEAR','MAX_YEAR','ABBREVIATION'])

    nba_team_games(teams)
