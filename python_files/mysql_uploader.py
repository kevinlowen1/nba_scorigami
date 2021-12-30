import config
import mysql.connector
from datetime import datetime

def mysql_table_creator_scores():
    #logging and printing
    f = open("log.txt", "a")
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
                                                    FT_PCT FLOAT(5,4),
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

def mysql_query_runner():
    print()


if __name__ == "__main__":
    #logging and printing
    f = open("log.txt", "a")
    f.write("starting mysql_uploader at: " + str(datetime.now()) + "\n")
    f.close()

    mysql_table_creator_scores()
    mysql_query_runner()



