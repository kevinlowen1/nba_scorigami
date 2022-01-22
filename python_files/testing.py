import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
from nba_api.stats.endpoints import teamdetails
from nba_api.stats.endpoints import commonteamyears
from nba_api.stats.endpoints import teamgamelog

if __name__ == "__main__":
    # print()
    # print()
    # print('starting game_log for 1610610029_teamGameLog.csv')
    # game_log_pandas = pd.read_csv('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores\\1610610029_teamGameLog.csv', sep=',', quotechar='"')
    # print(game_log_pandas)

    print()
    print()
    print('starting CommonTeamYears for league_id=00')
    teams = commonteamyears.CommonTeamYears(league_id='00')
    df = teams.get_data_frames()[0]
    print(df)

    print()
    print()
    print('starting team_details')
    teams = teamdetails.TeamDetails(team_id=1610610029)
    df = teams.get_data_frames()[0]
    print(df)

    print()
    print()
    print('starting TeamGameLog for 1610610029 with season 47')
    games = teamgamelog.TeamGameLog(season=47, season_type_all_star='Regular Season', team_id=1610610029)
    df = games.get_data_frames()[0]
    print(df)

    
    print()
    print()
    print('starting TeamGameLog for 1610610029 with season 48')
    games = teamgamelog.TeamGameLog(season=48, season_type_all_star='Regular Season', team_id=1610610029)
    df = games.get_data_frames()[0]
    print(df)

    
    print()
    print()
    print('starting TeamGameLog for 1610610029 with season 49')
    games = teamgamelog.TeamGameLog(season=49, season_type_all_star='Regular Season', team_id=1610610029)
    df = games.get_data_frames()[0]
    print(df)