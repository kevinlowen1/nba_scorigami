import config
import mysql.connector
from datetime import datetime
import pandas as pd
import numpy as np
import csv
import mariadb
import os
import sys

def mariadb_table_creator_scores():
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

    mycursor.execute("""CREATE INDEX IF NOT EXISTS nba_scores_raw_team_id_index
                        ON nba_scores_raw (TEAM_ID)""")


    f.write("created table for raw table scores in mysql db.\n")
    
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

def mariadb_inserter(file):
    #logging file
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting mysql_inserter at: " + str(datetime.now()) + "\n")

    #connection info for mariadb
    conn = mariadb.connect(
        host=config.db_ip,
        user=config.db_username,
        password=config.db_password,
        database="nba",
        port=config.db_port
    )
    mycursor = conn.cursor()

    #move log for one team to a dataframe
    game_log_pandas = pd.read_csv('C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\game_scores\\'+file, sep=',', quotechar='"')
    #fill blank fields with 0
    game_log_pandas = game_log_pandas.fillna(0)

    #make a list with the dataframe
    data = game_log_pandas.values.tolist()
    #insert list into mariadb
    mycursor.executemany("INSERT INTO nba_scores_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" , data)
    #commit the insert
    conn.commit()
    #close the connection
    conn.close()
    #close the logging
    f.close()

def mariadb_less_fields():
    #logging and printing
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting mariadb_less_fields at: " + str(datetime.now()) + "\n")
    
    #connection info for mariadb
    conn = mariadb.connect(
        host=config.db_ip,
        user=config.db_username,
        password=config.db_password,
        database="nba",
        port=config.db_port
    )
    cur = conn.cursor() 

    # Connect to MariaDB Platform and execute a view creation for less fields games
    try:
        cur.execute("""CREATE
            VIEW IF NOT EXISTS nba_scores_raw_less_fields
            AS  SELECT Team_ID,
                            Game_ID,
                            GAME_DATE,
                            MATCHUP,
                            WL,
                            PTS
            FROM nba_scores_raw""")
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    #close the logging
    f.close()




if __name__ == "__main__":
    #logging and printing
    f = open("C:\\Users\\Kevin.DESKTOP-9D0VMK8\Documents\\Projects\\nba_scorigami\\logs\\log.txt", "a")
    f.write("starting mysql_uploader at: " + str(datetime.now()) + "\n")
    f.close()

    #create tables if they don't exist in the db
    #to-do, switch mysql  --> mariadb connector
    mariadb_table_creator_scores()

    # insert raw scores file by fiile
    directory = r'C:\\Users\\Kevin.DESKTOP-9D0VMK8\\Documents\\Projects\\nba_scorigami\\game_scores'
    for file in os.listdir(directory):
        mariadb_inserter(file)

    #select  scores into separate table with less fields for ease of processing.
    #to-do, need to finish coding this
    mariadb_less_fields()



