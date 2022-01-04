import config
import mysql.connector
from datetime import datetime
import pandas as pd
import numpy as np
import csv
import mariadb
import os

def mysql_table_creator_scores():
    #logging and printing
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting mysql_table_creator_scores_historical\n")

    mydb = mysql.connector.connect(
        host=config.db_ip,
        user=config.db_username,
        password=config.db_password,
        database="nba",
        port=config.db_port
    )

    mycursor = mydb.cursor()
    mycursor.execute("""CREATE TABLE IF NOT EXISTS nba_scores_raw(Team_ID INT,
                        Game_ID INT,
                        GAME_DATE VARCHAR(255),
                        MATCHUP VARCHAR(255),
                        WL VARCHAR(1),
                        W INT,
                        L INT,
                        W_PCT FLOAT(5,4),
                        MIN INT,
                        FGM INT,
                        FGA INT,
                        FG_PCT FLOAT(5,4),
                        FG3M INT,
                        FG3A INT,
                        FG3_PCT FLOAT(5,4),
                        FTM INT,
                        FTA INT,
                        FT_PCT FLOAT(10,5),
                        OREB INT,
                        DREB INT,
                        REB INT,
                        AST INT,
                        STL INT,
                        BLK INT,
                        TOV INT,
                        PF INT,
                        PTS INT)""")


    f.write("created table for raw table scores in mysql db.\n")
    f.write("starting creation of table for raw scores without extra fields.\n")
    mycursor.execute('''CREATE TABLE IF NOT EXISTS nba_scores_raw_less_fields (Team_ID INT,
                                                    Game_ID INT,
                                                    GAME_DATE VARCHAR(255),
                                                    MATCHUP VARCHAR(255),
                                                    WL VARCHAR(1),
                                                    PTS INT)
                                                    ''')

    f.write("created table for raw table scores in mysql db without extra fields.\n")
    mycursor.execute('''CREATE TABLE IF NOT EXISTS nba_scores_distinct_games (Game_ID INT,
                                                    GAME_DATE VARCHAR(255),
                                                    HOME_TEAM VARCHAR(255),
                                                    AWAY_TEAM VARCHAR(255),
                                                    WINNING_TEAM VARCHAR(255),
                                                    LOSING_TEAM VARCHAR(255),
                                                    WINNING_SCORE INT,
                                                    LOSING_SCORE INT,
                                                    FINAL_SCORE VARCHAR(255))
                                                    ''')

    f.write("created table for distinct games.\n")
    f.close()

def mysql_inserter(file):
    #logging and printing
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting mysql_inserter at: " + str(datetime.now()) + "\n")
    print("starting mysql_inserter for:" + file)

    game_log_pandas = pd.read_csv('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores\\'+file, sep=',', quotechar='"')
    # game_log_pandas = pd.read_csv('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores\\1610610023_teamGameLog.csv', sep=',', quotechar='"')
    game_log_pandas = game_log_pandas.fillna(0)
    game_log_pandas = game_log_pandas.iloc[: , 1:]
    cols = ['Team_ID','Game_ID','W','L','MIN','FGM','FG3A','FGA','FG3M','FTM','FTA','OREB','DREB','REB','AST','STL','BLK','TOV','PF','PTS']
    game_log_pandas[cols] = game_log_pandas[cols].applymap(np.int64)

    # print("here")
    # print(game_log_pandas.columns[(game_log_pandas == 0.0).iloc[0]])

    conn = mariadb.connect(
        host=config.db_ip,
        user=config.db_username,
        password=config.db_password,
        database="nba",
        port=config.db_port
    )
    mycursor = conn.cursor()

    # Here, change your own path to dump the temp file
    game_log_pandas.to_csv('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores_no_index\\'+file, index=False) 
    # game_log_pandas.to_csv('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores_no_index\\1610610023_teamGameLog.csv', index=False)                                                       
    # file = open('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores_no_index\\1610610023_teamGameLog.csv','r')                                                 
    filed = open('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores_no_index\\'+file,'r')
    csv_data = csv.reader(filed)

    my_data = []
    for row in csv_data:
        my_data.append(row)

    with open('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores_no_index\\'+file,'r', newline='') as f:
        next(f)
        reader = csv.reader(f, delimiter=',')
        rows = [[int(row[0]),int(row[1]),row[2],row[3],row[4],int(row[5]),int(row[6]),float(row[7]),int(row[8]),int(row[9]),int(row[10]),float(row[11]),int(row[12]),int(row[13]),float(row[14]),int(row[15]),int(row[16]),float(row[17]),int(row[18]),int(row[19]),int(row[20]),int(row[21]),int(row[22]),int(row[23]),int(row[24]),int(row[25]),int(row[26])] for row in reader if row]

    data = []
    for row in rows:
        data.append(tuple(row))

    mycursor.executemany("INSERT INTO nba_scores_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" , data)
    conn.commit()

    conn.close()
    f.close()


if __name__ == "__main__":
    #logging and printing
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting mysql_uploader at: " + str(datetime.now()) + "\n")
    f.close()

    mysql_table_creator_scores()

    directory = r'C:\\Users\\Kevin.DESKTOP-9D0VMK8\\Documents\\Projects\\nba_scorigami\\game_scores'

    for file in os.listdir(directory):
        mysql_inserter(file)



