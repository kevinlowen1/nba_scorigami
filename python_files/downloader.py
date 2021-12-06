from nba_api.stats.endpoints import commonteamyears
##https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/commonteamyears.md

from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.library.parameters import Season


def nba_teams_list():
    teams = commonteamyears.CommonTeamYears(league_id='00')
    df = teams.get_data_frames()[0]
    print(df)
    return(df)

def nba_team_games(teams):
    # print(teams[4].str.slice(start=2))
    teams['MAX_YEAR'] = teams.MAX_YEAR.str.slice(start=-2)
    print(teams)
    # teams[3].apply(lambda x: x.str.slice(0, 3))

    for row in teams.itertuples(index=False):
        print(row[teams.columns.get_loc('TEAM_ID')], 
                row[teams.columns.get_loc('MIN_YEAR')],
                row[teams.columns.get_loc('MAX_YEAR')]
                 )

    # games = teamgamelog.TeamGameLog(season='1949-49', season_type_all_star='Regular Season', team_id='1610610037')
    # df = games.get_data_frames()[0]
    # print(df)

if __name__ == "__main__":
   teams = nba_teams_list()
   nba_team_games(teams)
