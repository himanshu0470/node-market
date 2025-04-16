from score.services.database.base import get_db_session 
from score.settings.base import DATA_TEMP_BASE_PATH
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import datetime
import json
from score.services.score.config import ONLYOVER,OPEN,CLOSE,SETTLED,SUSPEND,PLAYER,PLAYERBOUNDARIES,PLAYERBALLSFACED,SESSION,OVERSESSION,FANCYLDO,ONLYOVERLDO,LASTDIGITNUMBER,ONLYOVER,WICKET,PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS,LD_OVER_BALLS, ODDEVEN, TOTALEVENTRUN, queue, MIDOVERSESSION
import time
import math

from score.services.score.config import LD_SESSION_MARKETS, LD_SESSIONLDO_MARKETS, LD_LASTDIGIT_MARKETS, LD_ONLYOVER_MARKETS, LD_ONLYOVERLDO_MARKETS, LD_OVERSESSION_MARKETS, LD_ODDEVEN_MARKETS, LD_DEFAULT_VALUES, LD_PLAYER_MARKETS,LD_PLAYERBOUNDARY_MARKETS,LD_PLAYERBALLSFACED_MARKETS,LD_FOW_MARKETS,LD_TOTALEVENTRUN_MARKETS, LD_MARKET_TEMPLATE, LD_MIDOVERSESSION_MARKETS

def get_commentary_teams(commentary_id, status):
    """Fetch the commentary teams based on the commentary ID."""
    commentary_team = []
    try:

        with get_db_session() as session:

            query = text('SELECT "wrTeamId", "wrShortName" FROM "tblCommentaryTeams" WHERE "wrCommentaryId" = :commentary_id AND "wrTeamStatus" = :status')
            
            result = session.execute(query, {"commentary_id": commentary_id,"status":status})
            commentary_team = [item for row in result.fetchall() for item in row]

    except SQLAlchemyError as e:
        print(f"An error occurred during the database operation: {e}")

    return commentary_team

def get_current_innings(commentary_id):
    """Fetch the current innings based on the commentary ID."""
    current_innings = []
    try:
        with get_db_session() as session:
            query = text('SELECT "wrCurrentInnings" FROM "tblCommentaries" WHERE "wrCommentaryId" = :commentary_id')
            result = session.execute(query, {"commentary_id": commentary_id})
            data = result.fetchall()
            current_innings = [item for row in data for item in row]

    except SQLAlchemyError as e:
        print(f"An error occurred during the database operation: {e}")

    return current_innings[0]
def get_max_overs(commentary_id):
    query = text('SELECT "wrTeamMaxOver" FROM "tblCommentaryTeams" WHERE "wrCommentaryId"=:commentary_id')
    with get_db_session() as session:
        try:
            max_overs_df = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id})
            return int(max_overs_df['wrTeamMaxOver'].iloc[0])
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return None

def get_column_names(table_name):
    """
    Retrieve column names for a given table using the current SQLAlchemy setup.
    """
    try:
        with get_db_session() as session:
            # Query to get column names from PostgreSQL's information_schema
            query = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
            """)
            result = session.execute(query, {"table_name": table_name})
            # Extract column names from the result
            column_names = [row["column_name"] for row in result.mappings()]
            return column_names
    except SQLAlchemyError as e:
        print(f"Error retrieving column names: {e}")
        return []

def get_market_score(commentary_id, commentary_team, over, is_session):
    if is_session:
        query = text('SELECT ovr."wrTeamScore" FROM "tblOvers" ovr INNER JOIN "tblCommentaryTeams" ct ON ovr."wrCommentaryId" = ct."wrCommentaryId" WHERE ovr."wrCommentaryId" = :commentary_id AND ct."wrTeamId" = ovr."wrTeamId" AND ovr."wrOver" = :over AND ct."wrTeamId" != :commentary_team and ovr."wrIsDelete"=False')
    else:
        query = text('SELECT "wrTotalRun" FROM "tblOvers" ovr INNER JOIN "tblCommentaryTeams" ct ON ovr."wrCommentaryId" = ct."wrCommentaryId" WHERE ovr."wrCommentaryId" = :commentary_id AND ct."wrTeamId" = ovr."wrTeamId" AND ovr."wrOver" = :over AND ct."wrTeamId" != :commentary_team and ovr."wrIsDelete"=False')

    with get_db_session() as session:
        try:
            runs_df = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "over": over - 1, "commentary_team":commentary_team})
            if not runs_df.empty:
                if is_session:
                    if runs_df['wrTeamScore'].iloc[0] is not None:
                        runs_arr = runs_df['wrTeamScore'].iloc[0].split('/')
                        total_score = int(runs_arr[0]) 
                    else:
                        return None
                else:
                    total_score = int(runs_df['wrTotalRun'].iloc[0])
            else:
                total_score=None
            return total_score
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return None

def to_boolean(value):
    if isinstance(value, str):
        value_lower = value.lower()
        if value_lower in ["true", "1"]:
            return True
        elif value_lower in ["false", "0"]:
            return False
        else:
            raise ValueError(f"Cannot convert {value} to boolean")
    return bool(value)


def get_balls_per_over(match_type_id):
    query = text('SELECT "wrBallsPerOver" FROM "tblMatchTypes" WHERE "wrMatchTypeId"=:match_type_id')
    with get_db_session() as session:
        try:
            balls_per_over_df = pd.read_sql_query(query, session.bind, params={"match_type_id": int(match_type_id)})
            return int(balls_per_over_df['wrBallsPerOver'].iloc[0])
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return None
        
def insert_logs(commentary_id, api, data):
    query = text('INSERT INTO "tblPythonLogs" ("wrCommentaryId", "wrApi", "wrData", "wrCreatedDate") VALUES (:commentary_id, :api, :data, :created_date)')
    with get_db_session() as session:
        try:
            session.execute(query, {
                "commentary_id": commentary_id,
                "api": api,
                "data": data,
                "created_date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00")
            })
            session.commit()
        except SQLAlchemyError as e:
            print(f"Database insert failed: {e}")
            session.rollback()

def get_ball_by_ball_id(commentary_id, strike_team):
    query = text('SELECT "wrCommentaryBallByBallId" FROM "tblCommentaryBallByBalls" WHERE "wrCommentaryId"=:commentary_id AND "wrTeamId"=:strike_team ORDER BY "wrCommentaryBallByBallId" DESC LIMIT 1')
    
    with get_db_session() as session:
        try:
            result = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "strike_team": strike_team})
            if not result.empty:
                return int(result['wrCommentaryBallByBallId'].iloc[0])
            else:
                return None  # Handle the case where no results are found
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return None

def overs_to_balls(value,match_type_id):
    global LD_OVER_BALLS
    #balls_per_over=get_balls_per_over(match_type_id)
    balls_per_over=LD_OVER_BALLS[f"{match_type_id}"]
    if(int(value)==0):
        total_balls=int(value*10)
    else:
        total_balls=int((round((value-int(value)),1)*10)+(int(value)*balls_per_over))
    
    return total_balls

def balls_to_overs(value,match_type_id):
    global LD_OVER_BALLS
    balls_per_over=LD_OVER_BALLS[f"{match_type_id}"]
    if int(value)==0:
        return 0.0
    else:
        over=((value-balls_per_over)/balls_per_over)+balls_per_over/10    
        return over

def update_database_async(runner_json,market_datalog_json,eventmarket_json):
    with get_db_session() as session:
        query = text('CALL update_prediction(:runner_json, :market_datalog_json, :eventmarket_json)')
        
        session.execute(query, {
            "runner_json": runner_json,
            "market_datalog_json": market_datalog_json,
            "eventmarket_json": eventmarket_json
        })
        
        session.commit()

def send_data_to_socket_async(commentary_id,marketData):
    print("sending data to socket is ", marketData)

    data={
            "commentaryId":commentary_id,
            "marketData":marketData
        
        }
    queue.put(data)

# def send_data_to_socket(data, retries=3,delay=0.1):
#     for attempt in range(retries):
#         try:
#             print("socket sedning starting time is ",time.time())

#             # Emit data to the socket
#             sio.emit('updatedEventMarket', data)
#             print("Data sent successfully")
#             return  # Exit if successful
#         except Exception as e:
#             print(f"Attempt {attempt + 1} failed: {e}")
#             if attempt < retries - 1:
#                 time.sleep(delay)  # Wait before retrying
#     print("All attempts to send data failed.")

# def send_data_to_socket(socket_index, market_data):
#     """
#     Send data using a specific socket connection.
#     """
#     sio = sockets[socket_index]
#     print(f"sending [Socket {socket_index}] starting time is ", time.time())
#     try:
#         sio.emit('updatedEventMarket', market_data)
#         print(f"[Socket {socket_index}] Data sent successfully")
#     except Exception as e:
#         print(f"[Socket {socket_index}] Failed to send data: {e}")

    
def convert_runner_data(row):
    market_runner_row=None
    if isinstance(row, pd.DataFrame):
        market_runner_row = row.iloc[0]
    else:
        # If a single row is passed
        market_runner_row = row
        
    market_runner = [{
        #"eventmarketid": int(market_runner_row['wrEventMarketId'].iloc[0]),
        "runnerId": int(market_runner_row['wrRunnerId']),
        "status": int(market_runner_row['wrSelectionStatus']),
        "runner": market_runner_row['wrRunner'],
        "line": float(market_runner_row['wrLine']),  
        "overRate": float(market_runner_row['wrOverRate']),      
        "underRate": float(market_runner_row['wrUnderRate']), 
        "backPrice": float(market_runner_row['wrBackPrice']),  
        "layPrice": float(market_runner_row['wrLayPrice']),  
        "backSize": float(market_runner_row['wrBackSize']),
        "laySize": float(market_runner_row['wrLaySize']),
        #"lastUpdate": str(market_runner_row['wrLastUpdate'].iloc[0])
        
        
    }]

    return market_runner
    #market_runner_data = json.dumps(market_runner, indent=2)
def convert_event_market(event_market_row,market_runner):
    event_market_data = {
        "marketId": event_market_row['wrID'],
        "eventId": str(event_market_row['wrEventRefID']),
        "marketName": event_market_row['wrMarketName'],
        "status": int(event_market_row['wrStatus']),  
        "isActive": to_boolean(event_market_row['wrIsActive']), 
        "isAllow": to_boolean(event_market_row['wrIsAllow']),
        "runner":market_runner 
    }
    json_data = json.dumps(event_market_data , indent=2)
    return json_data 

def convert_socket_data(event_market_row,market_runner,ball_by_ball_id=0):
    event_market_data = {
        "marketId": event_market_row['wrID'],
        "commentaryId":event_market_row['wrCommentaryId'],
        "marketTypeCategoryId":event_market_row['wrMarketTypeCategoryId'],
        #"ballByBallId":get_ball_by_ball_id(event_market_row['wrCommentaryId'],event_market_row['wrTeamID']),
        #"ballByBallId":int(event_market_row["ballbyballid"]) if event_market_row['wrMarketTypeCategoryId'] not in (PLAYER,PLAYERBOUNDARIES, PLAYERBALLSFACED,WICKET,PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS) else ball_by_ball_id,
        "ballByBallId":int(ball_by_ball_id) ,

        "eventId": event_market_row['wrEventRefID'],
        "marketName": event_market_row['wrMarketName'],
        "status": event_market_row['wrStatus'],  
        "isActive": to_boolean(event_market_row['wrIsActive']), 
        "isSendData": to_boolean(event_market_row['wrIsSendData']), 
        "isAllow": to_boolean(event_market_row['wrIsAllow']),
        "teamId":event_market_row["wrTeamID"],
        "margin":float(event_market_row["wrMargin"]),
        "over":event_market_row["wrOver"] if event_market_row["wrMarketTypeCategoryId"]!=ONLYOVER else event_market_row["onlyovers"],
        "inningsId":event_market_row["wrInningsID"],
        "lineRatio":event_market_row["wrLineRatio"] if event_market_row['wrMarketTypeCategoryId'] not in (PLAYER,PLAYERBOUNDARIES, PLAYERBALLSFACED,WICKET,PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS) else 0,
        "marketTypeId":event_market_row["wrMarketTypeId"],
        "lineType":int(event_market_row["wrLineType"]),
        "rateDiff":int(event_market_row["wrRateDiff"]),
        "predefinedValue":round(float(event_market_row["wrPredefinedValue"]),2),
        "playerScore":int(event_market_row["player_score"]) if event_market_row['wrMarketTypeCategoryId'] in (PLAYER,PLAYERBOUNDARIES, PLAYERBALLSFACED,WICKET,PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS) else 0,
        "isInningRun":to_boolean(event_market_row["wrIsInningRun"]) if event_market_row['wrMarketTypeCategoryId'] is not None else False, 
        "playerId": int(event_market_row["wrPlayerID"]) if event_market_row['wrMarketTypeCategoryId'] in (PLAYER,PLAYERBOUNDARIES, PLAYERBALLSFACED) and event_market_row["wrPlayerID"] is not None else None,
        "runner":market_runner 
    }
    json_data = json.dumps(event_market_data , indent=2)
    return json_data 

def fetch_runner_data(selection_status,row):
    market_runner_row=pd.DataFrame()
    if(selection_status==CLOSE):
        with get_db_session() as session:
            query = text('SELECT "wrRunnerId","wrRunner","wrSelectionStatus" FROM "tblMarketRunners" WHERE "wrEventMarketId" = :wr_event_market_id AND "wrSelectionStatus" IN (:open_status, :suspend_status)')
            params = {
                "wr_event_market_id": row['wrID'],
                "open_status": OPEN,
                "suspend_status": SUSPEND
            }
            market_runner_row = pd.read_sql(query, con=session.bind, params=params)
    elif(selection_status==SETTLED):
        with get_db_session() as session:
            query = text('SELECT "wrRunnerId","wrRunner","wrSelectionStatus"  FROM "tblMarketRunners" WHERE "wrEventMarketId" = :wr_event_market_id AND "wrSelectionStatus" = :close_status')
            params = {
                "wr_event_market_id": row['wrID'],
                "close_status": CLOSE
            }
            market_runner_row = pd.read_sql(query, con=session.bind, params=params)
    else:
        with get_db_session() as session:
            query = text('SELECT "wrRunnerId","wrRunner","wrSelectionStatus"  FROM "tblMarketRunners" WHERE "wrEventMarketId" = :wr_event_market_id AND "wrSelectionStatus" NOT IN (:close_status, :settled_status)')
            params = {
                "wr_event_market_id": row['wrID'],
                "close_status": CLOSE,
                "settled_status": SETTLED
            }
            market_runner_row = pd.read_sql(query, con=session.bind, params=params)

    return market_runner_row



def parse_event_data(row, json_event_data, is_senddata):
    def format_timestamp(value):
        """Convert timestamp to a valid format or return None if NaT."""
        if pd.isna(value) or value in ("NaT","nan"):
            return None
        elif isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%dT%H:%M:%S.%f+00")
        elif isinstance(value, str):
            return value  # Assuming the string is already a valid timestamp
        else:
            return None

    data = {
        "wrID": row["wrID"],
        "wrData": json_event_data,
        "wrIsSendData": is_senddata,
        "wrStatus": int(row["wrStatus"]),
        "wrResult": int(row["wrResult"]) if row["wrStatus"] == SETTLED else None,
        "wrCloseTime": format_timestamp(row["wrCloseTime"]),
        "wrOpenTime": format_timestamp(row["wrOpenTime"]),
        "wrSettledTime": format_timestamp(row["wrSettledTime"]),
        "wrOpenOdds": row["openvalue"] if row['wrMarketTypeCategoryId'] not in {PLAYER, PLAYERBOUNDARIES, PLAYERBALLSFACED, WICKET, PARTNERSHIPBOUNDARIES, WICKETLOSTBALLS} else row["wrOpenOdds"],
        "wrMinOdds": row["minodds"] if row['wrMarketTypeCategoryId'] not in {PLAYER, PLAYERBOUNDARIES, PLAYERBALLSFACED, WICKET, PARTNERSHIPBOUNDARIES, WICKETLOSTBALLS} else row["wrMinOdds"],
        "wrMaxOdds": row["maxodds"] if row['wrMarketTypeCategoryId'] not in {PLAYER, PLAYERBOUNDARIES, PLAYERBALLSFACED, WICKET, PARTNERSHIPBOUNDARIES, WICKETLOSTBALLS} else row["wrMaxOdds"],
        "wrPredefinedValue": row["wrPredefinedValue"]
    }
    return data



def markets_result_settlement(row,commentary_id,commentary_team, partnership_no=0,is_end_innings_result=False):
    query_player=text('SELECT "wrBat_Run" FROM "tblCommentaryPlayers" WHERE "wrCommentaryId" = :commentary_id and "wrTeamId"=:team_id AND "wrCommentaryPlayerId"=:player_id AND "wrBat_WicketType" is NOT Null')
    query_player_boundary=text('SELECT COALESCE("wrBat_FOUR", 0) + COALESCE("wrBat_SIX", 0) AS "wrPlayerBoundary" FROM "tblCommentaryPlayers" WHERE "wrCommentaryId"=:commentary_id AND "wrCommentaryPlayerId"=:player_id AND "wrBat_WicketType" is NOT Null')
    query_player_ball_face=text('SELECT "wrBat_Ball" FROM "tblCommentaryPlayers" WHERE "wrCommentaryId" = :commentary_id and "wrTeamId"=:team_id AND "wrCommentaryPlayerId"=:player_id AND "wrBat_WicketType" is NOT Null')
    query_partnership_boundary=text('SELECT COALESCE("wrTotalFour", 0) + COALESCE("wrTotalSix", 0) AS "wrPartnershipBoundary" FROM "tblCommentaryPartnerships" WHERE "wrCommentaryId"=:commentary_id AND "wrTeamId"=:team_id and "wrOrder"=:partnership_no ORDER BY "wrCommentaryPartnershipId" DESC')
    query_fall_of_wcket=text('SELECT w."wrTeamScore" as wrTeamScore, w.*, w."wrWicketCount" as wrWicketCount, w."wrCommentaryBallByBallId" as wrCommentaryBallByBallId, b."wrOverCount" as wrOverCount FROM "tblCommentaryWickets" w JOIN "tblCommentaryBallByBalls" b ON w."wrCommentaryBallByBallId" = b."wrCommentaryBallByBallId" WHERE w."wrCommentaryId" = :commentary_id and b."wrTeamId"=:team_id AND "wrWicketCount"=:wicket_no and w."wrIsDeletedStatus"=False ORDER BY b."wrOverCount" ASC')
    query_lost_balls=text('SELECT  w."wrTeamScore" as wrTeamScore, w.*, w."wrWicketCount" as wrWicketCount, w."wrCommentaryBallByBallId" as wrCommentaryBallByBallId, b."wrOverCount" as wrOverCount FROM "tblCommentaryWickets" w JOIN "tblCommentaryBallByBalls" b ON w."wrCommentaryBallByBallId" = b."wrCommentaryBallByBallId" WHERE w."wrCommentaryId" = :commentary_id and b."wrTeamId"=:team_id AND "wrWicketCount"=:wicket_no and w."wrIsDeletedStatus"=False ORDER BY b."wrOverCount" ASC')
    result=0
    with get_db_session() as session:
        if row["wrMarketTypeCategoryId"]==PLAYER:
            df=pd.read_sql_query(query_player, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team,"player_id":row["wrPlayerID"]})
            result=int(df['wrBat_Run'].iloc[0]) if not df.empty and df['wrBat_Run'].iloc[0] is not None else 0
        elif row["wrMarketTypeCategoryId"]==PLAYERBOUNDARIES:
            df = pd.read_sql(query_player_boundary, session.bind, params={"commentary_id":commentary_id, "player_id":row["wrPlayerID"]})
            result=int(df["wrPlayerBoundary"].iloc[0]) if not df.empty and df["wrPlayerBoundary"].iloc[0] is not None else 0
        elif row["wrMarketTypeCategoryId"]==PLAYERBALLSFACED:
            df=pd.read_sql_query(query_player_ball_face, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team,"player_id":row["wrPlayerID"]})
            result=int(df['wrBat_Ball'].iloc[0]) if not df.empty and df['wrBat_Ball'].iloc[0] is not None else 0
        elif row["wrMarketTypeCategoryId"] in (SESSION, OVERSESSION):
            if is_end_innings_result:
                result=team_total_score(commentary_id,commentary_team)
            else:
                result=get_market_score(commentary_id,commentary_team,int(row["wrOver"]),True)
        elif row["wrMarketTypeCategoryId"]==ONLYOVER:
            result=get_market_score(commentary_id,commentary_team,int(row["wrOver"]),False)
        elif row["wrMarketTypeCategoryId"]==ONLYOVERLDO:
            runs=get_market_score(commentary_id,commentary_team,int(row["wrOver"]),False)
            if runs is not None:
                if(runs%2==0):
                    result=0
                else:
                    result=1   
            else:
                return None
        elif row["wrMarketTypeCategoryId"]==FANCYLDO:
            if is_end_innings_result:
                runs=team_total_score(commentary_id,commentary_team)
                print("the runs are ",runs)
            else:
                runs=get_market_score(commentary_id,commentary_team,int(row["wrOver"]),True)
            if runs is not None:
                if(int(runs)%2==0):
                    result=0
                else:
                    result=1 
            else:
                return None
            print("result calculated is ", result)
            
        elif row["wrMarketTypeCategoryId"]==PARTNERSHIPBOUNDARIES:
            df = pd.read_sql(query_partnership_boundary, session.bind, params={"commentary_id":commentary_id, "team_id": commentary_team, "partnership_no":int(partnership_no)})
            result=int(df["wrPartnershipBoundary"].iloc[0]) if not df.empty and df['wrPartnershipBoundary'].iloc[0] is not None else 0 
            
        elif row["wrMarketTypeCategoryId"]==WICKET:
            df = pd.read_sql(query_fall_of_wcket, session.bind, params={"commentary_id":commentary_id, "team_id": commentary_team, "wicket_no":int(partnership_no)})
            result=int(df["wrteamscore"].iloc[0]) if not df.empty and df['wrovercount'].iloc[0] is not None else 0 
        
        elif row["wrMarketTypeCategoryId"]==WICKETLOSTBALLS:
            df = pd.read_sql(query_lost_balls, session.bind, params={"commentary_id":commentary_id, "team_id": commentary_team, "wicket_no":int(partnership_no)})
            result=float(df["wrovercount"].iloc[0]) if not df.empty and df['wrovercount'].iloc[0] is not None else 0 
        elif row["wrMarketTypeCategoryId"]==MIDOVERSESSION:
            if is_end_innings_result:
                result=team_total_score(commentary_id,commentary_team)
            else:
                result=get_mid_over_runs(int(row["wrCommentaryId"]), int(row["wrTeamID"]),float(row["wrOver"]))
                
            

    return result

def team_total_score(commentary_id,commentary_team):
    query=text('SELECT ovr."wrTeamScore"  FROM "tblOvers" ovr INNER JOIN "tblCommentaryTeams" ct ON ovr."wrCommentaryId" = ct."wrCommentaryId" WHERE ovr."wrCommentaryId" = :commentary_id AND ct."wrTeamId" = ovr."wrTeamId"  AND ct."wrTeamId" != :commentary_team and ovr."wrIsDelete"=FALSE ORDER BY "wrOver" DESC;')
    with get_db_session() as session:
        try:
            runs_df = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "commentary_team":commentary_team})
            if runs_df.empty:
                return None
            if runs_df['wrTeamScore'].iloc[0] is not None:
                total_score = runs_df['wrTeamScore'].iloc[0].split('/')
             
                return int(total_score[0])
            else:
                return None
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return None  
   
def get_mid_over_runs(commentary_id, team_id, over):
    query=text('Select SUM("wrBall_Run"+"wrBall_ExtraRun") as "TotalRun" from "tblCommentaryBallByBalls" where "wrOverCount"<=:over and "wrCommentaryId"=:commentary_id and "wrTeamId"=:team_id AND "wrIsDeletedStatus"=False' )
    with get_db_session() as session:
        try:
            runs_df = pd.read_sql_query(query, session.bind, params={"over":over,"commentary_id": commentary_id, "team_id":team_id})
            print(runs_df)
            if runs_df.empty:
                return None
            if runs_df['TotalRun'].iloc[0] is not None:
                total_score = runs_df['TotalRun'].iloc[0]
                
                return int(total_score)
            else:
                return None
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return None  
   
def ordinal_suffix(n):
    if 11 <= n % 100 <= 13:  
        return "th"
    if n % 10 == 1:
        return "st"
    if n % 10 == 2:
        return "nd"
    if n % 10 == 3:
        return "rd"
    return "th"

def round_down(value):
    return math.floor(value * 10) / 10

def custom_round(number):
    if number - math.floor(number) == 0.5:
        return math.ceil(number)
    return round(number)

def reset_global_data(commentary_id,match_type_id):
    global LD_SESSION_MARKETS, LD_SESSIONLDO_MARKETS, LD_LASTDIGIT_MARKETS, LD_ONLYOVER_MARKETS, LD_ONLYOVERLDO_MARKETS, LD_OVERSESSION_MARKETS, LD_ODDEVEN_MARKETS, LD_DEFAULT_VALUES, LD_PLAYER_MARKETS,LD_PLAYERBOUNDARY_MARKETS,LD_PLAYERBALLSFACED_MARKETS,LD_FOW_MARKETS,LD_TOTALEVENTRUN_MARKETS, LD_MARKET_TEMPLATE, LD_MIDOVERSESSION_MARKETS

    for global_dict in [
        LD_SESSION_MARKETS,
        LD_SESSIONLDO_MARKETS,
        LD_LASTDIGIT_MARKETS,
        LD_ONLYOVER_MARKETS,
        LD_ONLYOVERLDO_MARKETS,
        LD_DEFAULT_VALUES,
        LD_PLAYER_MARKETS,
        LD_PLAYERBOUNDARY_MARKETS,
        LD_PLAYERBALLSFACED_MARKETS,
        LD_FOW_MARKETS,
        LD_OVERSESSION_MARKETS,
        LD_ODDEVEN_MARKETS,
        LD_TOTALEVENTRUN_MARKETS,
        LD_MARKET_TEMPLATE,
        LD_MIDOVERSESSION_MARKETS
    ]:
        keys_to_delete = [key for key in global_dict.keys() if key.startswith(f"{commentary_id}")]
        
        # Remove the keys
        for key in keys_to_delete:
            del global_dict[key]
