from nba_api.stats.endpoints import commonteamyears
from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.library.parameters import Season


def nba_teams_list():
    teams = commonteamyears.CommonTeamYears(league_id='00')
    df = teams.get_data_frames()[0]
    print(df)

def nba_team_games():
    games = teamgamelog.TeamGameLog(season='1949-49', season_type_all_star='Regular Season', team_id='1610610037')
    df = games.get_data_frames()[0]
    print(df)



if __name__ == "__main__":
   nba_teams_list()
   nba_team_games()
