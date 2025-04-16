from datetime import datetime
import os
import time
import json
import traceback
import logging
from typing import OrderedDict
import pandas as pd
import numpy as np
import math
import threading
#from score.services.database.base import DB_CONNECTION as connection
from score.services.database.base import get_db_session 
from score.settings.base import DATA_TEMP_BASE_PATH

from score.services.score.config import CLOSE, OPEN, SUSPEND, NOTCREATED, SETTLED, BALL, OVER, INACTIVE, CANCEL,PLAYER,WICKET,PLAYERBOUNDARIES, PLAYERBOUNDARIES, DYNAMICLINE, STATICLINE,sio
import warnings
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import asyncio
warnings.filterwarnings('ignore')

LOG = logging.getLogger('app')

class PlayerPrediction():

    def __init__(self, commentary_id, match_type_id, event_id, current_team_id, current_score, current_ball):
        self.commentary_id = commentary_id
        self.match_type_id = match_type_id
        self.event_id = event_id   
        self.current_team_id = current_team_id
        self.current_score = current_score
        self.current_ball = float(current_ball)
        self.total_wicket = 0
        self.player_runs_map = {}
        self.market_template_category= PLAYER
        self.fow_template_name = WICKET  
        #self.wrBackSize = 90
        #self.wrLaySize = 110
        self.last_wicket_ball_count = 3
        self.last_wicket_ball = self.last_wicket_ball_count
        self.wicket_ball_mapping = OrderedDict()

    def send_data_to_socket(self, data):
        socket_data=[]
        
        
        #data_dic=json.loads(data)
        #socket_data.append(json.dumps(data_dic,indent=2))
        for value in data:
            try:
                value_str = json.loads(value)
                socket_data_str = json.dumps(value_str, indent=2)
                socket_data.append(socket_data_str)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

        market_data = {"commentaryId":self.commentary_id,
           "marketData":socket_data}

        LOG.info("Sending data to socket %s", market_data)
        sio.emit('updatedEventMarket', market_data)

    def get_market_template(self, market_category):
        with get_db_session() as session:
            if market_category in (PLAYER, PLAYERBOUNDARIES):        
                market_template_query = text('select * from "tblMarketTemplates" where "wrMatchTypeID"=:match_type_id and "wrMarketTypeCategoryId"=:market_category and "wrIsPlayer"=:isplayer and "wrIsActive"=True')
                market_template = pd.read_sql_query(market_template_query,session.bind,params={"match_type_id":int(self.match_type_id), "market_category":market_category, "isplayer":True})
            else:
                market_template_query = text('select * from "tblMarketTemplates" where "wrMatchTypeID"=:match_type_id and "wrMarketTypeCategoryId"=:market_category and "wrIsActive"=True')
                market_template = pd.read_sql_query(market_template_query,session.bind,params={"match_type_id":int(self.match_type_id), "market_category":market_category})
            
        LOG.info("Available market template %s", market_template)        
        
        if len(market_template) != 1:
            return {"status": False, "msg": str(market_category) + " is not available in Market Templates table"}
    
        return {"status": True, "data": market_template}
    
    def get_players_details(self, player_id=None):
        """
        Get the current players who is playing
        """
        with get_db_session() as session:
            if not player_id:
                select_players_query=text('select * from "tblCommentaryPlayers" where "wrCommentaryId"=:commentary_id and "wrTeamId"=:team_id and "wrBat_IsPlay"=:is_play')
                players_details = pd.read_sql_query(select_players_query,session.bind,params={"commentary_id":self.commentary_id, "team_id":self.current_team_id, "is_play":True})
            else:            
                player_id_str = player_id
                select_players_query=text('select * from "tblCommentaryPlayers" where "wrCommentaryId"=:commentary_id and "wrTeamId"=:team_id and "wrCommentaryPlayerId" = ANY(:player_id_str)')
                players_details = pd.read_sql_query(select_players_query,session.bind,params={"commentary_id":self.commentary_id, "team_id":self.current_team_id, "player_id_str":player_id_str})

        if len(players_details) == 0:
            return {"status": False, "msg": "Player details is not available on the CommentaryPlayers"}
    
        return {"status": True, "data": players_details}
    
    def check_existing_player_market(self, player_id,is_close, market_type_category_id):
       # with get_db_session() as session:
       #     exiting_player_event_query = text('select * from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrTeamID"=:current_team_id and "wrPlayerID"=:player_id and "wrMarketTypeCategoryId"=:market_type_category_id and "wrStatus" not in (:CANCEL, :SETTLE)')
       #     players_event_details = pd.read_sql_query(exiting_player_event_query,session.bind,params={"commentary_id":self.commentary_id, "current_team_id":self.current_team_id, "player_id":player_id, "market_type_category_id":market_type_category_id,"CANCEL":CANCEL, "SETTLE":SETTLED})
        player_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))
        players_event_details = player_markets[
                    (player_markets['wrCommentaryId'] == self.commentary_id) &
                    (player_markets['wrTeamID'] == self.current_team_id) &
                    (player_markets['wrPlayerID'] == player_id) &
                    (player_markets['wrMarketTypeCategoryId'] == market_type_category_id) &
                    (~player_markets['wrStatus'].isin([CANCEL]))  
                    ]        
        
        if len(players_event_details) == 0:
            return {"status": False, "msg": "Player Event is not available on the EventMarket"}
        
        elif players_event_details["wrStatus"].iloc[0]==CLOSE and is_close==False:
            return {"status": True, "data": None}
        else:
            return {"status": True, "data": players_event_details}

    def get_open_player_market_byteamid(self, team_id):
       #with get_db_session() as session:
       #    exiting_player_event_query = text('select * from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrTeamID"=:current_team_id and "wrIsPlayer"=:wrIsPlayer and "wrStatus"=:market_status')
       #    players_event_details = pd.read_sql_query(exiting_player_event_query,session.bind,params={"commentary_id":self.commentary_id, "current_team_id":team_id, "wrIsPlayer":True, "market_status": OPEN})
       
       player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))
       result = player_event_markets[
                            (player_event_markets['wrCommentaryId'] == self.commentary_id) &
                            (player_event_markets['wrTeamID'] == team_id) &
                            (player_event_markets['wrIsPlayer'] == True)   &
                            (player_event_markets['wrStatus']==OPEN)
                            ]
       
       if len(result) == 0:
           return {"status": False, "msg": "Player Event is not available on the EventMarket"}
   
       return {"status": True, "data": result}
    
    def create_player_event_market(self, player_info, market_template_result, status): 
        player_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        with get_db_session() as session:

            LOG.info("player_info %s", player_info)
            player_market_name = market_template_result["wrTemplateName"].format(player=player_info['wrPlayerName'])
            player_id = player_info["wrCommentaryPlayerId"]   
            time_now = datetime.now()
    
            if status == OPEN:            
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff") VALUES (:commentary_id, :event_id, :team_id, 1, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': self.commentary_id, 'event_id': self.event_id, 'team_id': self.current_team_id, 'market_name': player_market_name, 'margin': market_template_result["wrMargin"], 'status': status, 'is_predefine_market': market_template_result['wrIsPredefineMarket'], 'is_over': market_template_result['wrIsOver'], 'over': 0, 'is_player': True, 'player_id': player_id, 'is_auto_cancel': market_template_result['wrIsAutoCancel'], 'auto_open_type': market_template_result['wrAutoOpenType'], 'auto_open': market_template_result['wrAutoOpen'], 'auto_close_type': market_template_result['wrAutoCloseType'], 'before_auto_close': market_template_result['wrBeforeAutoClose'], 'auto_suspend_type': market_template_result['wrAutoSuspendType'], 'before_auto_suspend': market_template_result['wrBeforeAutoSuspend'], 'is_ball_start': market_template_result['wrIsBallStart'], 'is_auto_result_set': market_template_result['wrIsAutoResultSet'], 'auto_result_type': market_template_result['wrAutoResultType'], 'auto_result_after_ball': market_template_result['wrAutoResultafterBall'], 'after_wicket_auto_suspend': market_template_result['wrAfterWicketAutoSuspend'], 'after_wicket_not_created': market_template_result['wrAfterWicketNotCreated'], 'is_active': market_template_result['wrIsActive'], 'is_allow': True, 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': market_template_result["wrMarketTypeCategoryId"],"market_type_id":market_template_result["wrMarketTypeId"],"market_template_id":market_template_result["wrID"] ,"create_type":market_template_result["wrCreateType"],"template_type":market_template_result["wrTemplateType"],"delay":market_template_result["wrDelay"], 'create':market_template_result["wrCreate"], 'create_ref_id':market_template_result["wrCreateRefId"],'open_ref_id':market_template_result["wrOpenRefId"],'action_type':market_template_result["wrActionType"], 'open_odd':player_info["wrBatsmanAverage"], 'min_odd':player_info["wrBatsmanAverage"], 'max_odd':player_info["wrBatsmanAverage"], 'line_type':market_template_result["wrLineType"], 'default_back_size':market_template_result["wrDefaultBackSize"], 'default_lay_size':market_template_result["wrDefaultLaySize"], 'default_is_send_data':market_template_result["wrDefaultIsSendData"], 'rate_diff':market_template_result["wrRateDiff"]})
                
            else:
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff") VALUES (:commentary_id, :event_id, :team_id, 1, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': self.commentary_id, 'event_id': self.event_id, 'team_id': self.current_team_id, 'market_name': player_market_name, 'margin': market_template_result["wrMargin"], 'status': status, 'is_predefine_market': market_template_result['wrIsPredefineMarket'], 'template_type': market_template_result['wrTemplateType'], 'is_over': market_template_result['wrIsOver'], 'over': 0, 'is_player': True, 'player_id': player_id, 'is_auto_cancel': market_template_result['wrIsAutoCancel'], 'auto_open_type': market_template_result['wrAutoOpenType'], 'auto_open': market_template_result['wrAutoOpen'], 'auto_close_type': market_template_result['wrAutoCloseType'], 'before_auto_close': market_template_result['wrBeforeAutoClose'], 'auto_suspend_type': market_template_result['wrAutoSuspendType'], 'before_auto_suspend': market_template_result['wrBeforeAutoSuspend'], 'is_ball_start': market_template_result['wrIsBallStart'], 'is_auto_result_set': market_template_result['wrIsAutoResultSet'], 'auto_result_type': market_template_result['wrAutoResultType'], 'auto_result_after_ball': market_template_result['wrAutoResultafterBall'], 'after_wicket_auto_suspend': market_template_result['wrAfterWicketAutoSuspend'], 'after_wicket_not_created': market_template_result['wrAfterWicketNotCreated'], 'is_active': market_template_result['wrIsActive'], 'is_allow': True, 'last_update': time_now, 'market_type_category_id': market_template_result["wrMarketTypeCategoryId"] ,"market_type_id":market_template_result["wrMarketTypeId"],"market_template_id":market_template_result["wrID"],"create_type":market_template_result["wrCreateType"],"delay":market_template_result["wrDelay"], 'create':market_template_result["wrCreate"], 'create_ref_id':market_template_result["wrCreateRefId"],'open_ref_id':market_template_result["wrOpenRefId"],'action_type':market_template_result["wrActionType"], 'open_odd':player_info["wrBatsmanAverage"], 'min_odd':player_info["wrBatsmanAverage"], 'max_odd':player_info["wrBatsmanAverage"],'line_type':market_template_result["wrLineType"], 'default_back_size':market_template_result["wrDefaultBackSize"], 'default_lay_size':market_template_result["wrDefaultLaySize"], 'default_is_send_data':market_template_result["wrDefaultIsSendData"], 'rate_diff':market_template_result["wrRateDiff"]})
            event_market_id = result.fetchone()[0]
    
            LOG.info("Insert Success in the player event market %s", event_market_id)
            session.commit()
            row = { "wrID":event_market_id, "wrCommentaryId": self.commentary_id, "wrEventRefID": self.event_id, "wrTeamID": self.current_team_id, "wrMarketName": player_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": market_template_result['wrIsOver'], "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": player_id, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": True, "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": player_info["wrBatsmanAverage"], "wrMinOdds": player_info["wrBatsmanAverage"], "wrMaxOdds": player_info["wrBatsmanAverage"], "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"] }
            df=pd.DataFrame([row])
            player_markets = pd.concat([player_markets, df], ignore_index=True)
            player_markets=player_markets.fillna(0)
            player_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)

            
            return player_markets.iloc[-1]


    def get_event_market_by_id(self, wrID):        
       # with get_db_session() as session:
       #     existing_event_query = text('select * from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrTeamID"=:current_team_id and "wrID"=:id')
       #     existing_event_market_df = pd.read_sql_query(existing_event_query,session.bind,params={"commentary_id":self.commentary_id, "current_team_id":self.current_team_id, "id":wrID})
        player_event_markets=os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv')

    
        existing_event_market_df = player_event_markets[
                             (player_event_markets['wrCommentaryId'] == self.commentary_id) &
                             (player_event_markets['wrTeamID'] == self.current_team_id) &
                             (player_event_markets['wrID'] == wrID)
                             ]
    
    
    
        if len(existing_event_market_df) != 1:
            return {"status": False, "msg": "Event Market is not available for ID "+str(wrID)}
        return {"status": True, "msg": "", "data": existing_event_market_df}
        
    def check_after_over_close_market(self, auto_close_type, auto_close_over):
        if auto_close_type == 2:
            auto_close_ball = self.over_to_ball(auto_close_over)
            current_ball = self.over_to_ball(self.current_ball)
            if current_ball >= auto_close_ball:
                return True
        return False

    def get_current_team_name_by_teamId(self):
        with get_db_session() as session:
            team_name_query = text('select "wrTeamName" from "tblCommentaryTeams" where "wrCommentaryId"=:commentary_id and "wrTeamId"=:current_team_id')
            team_name_df = pd.read_sql_query(team_name_query,session.bind,params={"commentary_id":self.commentary_id, "current_team_id":self.current_team_id,})
    
            if len(team_name_df) != 1:
                return {"status": False, "msg": "Team names available more than 1 with the wrTeamID "+str(self.current_team_id)}
            return {"status": True, "msg": "", "data": str(team_name_df["wrTeamName"].values[0]).strip()}

    def create_fow_event_market(self, fow_wicket_event_name, market_template_result, status):
        player_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        with get_db_session() as session:
            market_template_result = market_template_result.to_dict("records")[0] 

            time_now = datetime.now()
            if status == OPEN:            
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrRateSource", "wrMarketTemplateId", "wrMarketTypeId", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize", "wrDefaultIsSendData", "wrRateDiff") VALUES (:commentary_id, :event_id, :team_id, 1, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :open_time, :last_update, :market_type_category_id, :create,:create_ref_id,:open_ref_id,:action_type, :rate_source, :market_template_id, :market_type_id, :line_type, :default_back_size , :default_lay_size, :default_is_send_data, :rate_diff) RETURNING "wrID"')
                
                result = session.execute(new_player_event_query, { 'commentary_id': self.commentary_id, 'event_id': self.event_id, 'team_id': self.current_team_id, 'market_name': fow_wicket_event_name, 'margin': market_template_result["wrMargin"], 'status': status, 'is_predefine_market': market_template_result['wrIsPredefineMarket'], 'is_over': market_template_result['wrIsOver'], 'over': market_template_result['wrOver'], 'is_player': False, 'player_id': None, 'is_auto_cancel': market_template_result['wrIsAutoCancel'], 'auto_open_type': market_template_result['wrAutoOpenType'], 'auto_open': market_template_result['wrAutoOpen'], 'auto_close_type': market_template_result['wrAutoCloseType'], 'before_auto_close': market_template_result['wrBeforeAutoClose'], 'auto_suspend_type': market_template_result['wrAutoSuspendType'], 'before_auto_suspend': market_template_result['wrBeforeAutoSuspend'], 'is_ball_start': market_template_result['wrIsBallStart'], 'is_auto_result_set': market_template_result['wrIsAutoResultSet'], 'auto_result_type': market_template_result['wrAutoResultType'], 'auto_result_after_ball': market_template_result['wrAutoResultafterBall'], 'after_wicket_auto_suspend': market_template_result['wrAfterWicketAutoSuspend'], 'after_wicket_not_created': market_template_result['wrAfterWicketNotCreated'], 'is_active': market_template_result['wrIsActive'], 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': market_template_result["wrMarketTypeCategoryId"], 'create':market_template_result["wrCreate"], 'create_ref_id':market_template_result["wrCreateRefId"],'open_ref_id':market_template_result["wrOpenRefId"],'action_type':market_template_result["wrActionType"], 'rate_source':1, 'market_template_id':market_template_result["wrID"],'market_type_id':market_template_result["wrMarketTypeId"], 'line_type':market_template_result["wrLineType"], 'default_back_size':market_template_result["wrDefaultBackSize"], 'default_lay_size':market_template_result["wrDefaultLaySize"], 'default_is_send_data':market_template_result["wrDefaultIsSendData"], 'rate_diff':market_template_result["wrRateDiff"] })
            else:
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrLastUpdate", "wrMarketTypeCategoryId", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrRateSource", "wrMarketTemplateId", "wrMarketTypeId","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize", "wrDefaultIsSendData", "wrRateDiff") VALUES (:commentary_id, :event_id, :team_id, 1, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :last_update, :market_type_category_id, :create,:create_ref_id,:open_ref_id,:action_type, :rate_source, :market_template_id, :market_type_id, :line_type, :default_back_size , :default_lay_size, :default_is_send_data, :rate_diff) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': self.commentary_id, 'event_id': self.event_id, 'team_id': self.current_team_id, 'market_name': fow_wicket_event_name, 'margin': market_template_result["wrMargin"], 'status': status, 'is_predefine_market': market_template_result['wrIsPredefineMarket'], 'template_type': market_template_result['wrTemplateType'], 'is_over': market_template_result['wrIsOver'], 'over': market_template_result['wrOver'], 'is_player': False, 'player_id': None, 'is_auto_cancel': market_template_result['wrIsAutoCancel'], 'auto_open_type': market_template_result['wrAutoOpenType'], 'auto_open': market_template_result['wrAutoOpen'], 'auto_close_type': market_template_result['wrAutoCloseType'], 'before_auto_close': market_template_result['wrBeforeAutoClose'], 'auto_suspend_type': market_template_result['wrAutoSuspendType'], 'before_auto_suspend': market_template_result['wrBeforeAutoSuspend'], 'is_ball_start': market_template_result['wrIsBallStart'], 'is_auto_result_set': market_template_result['wrIsAutoResultSet'], 'auto_result_type': market_template_result['wrAutoResultType'], 'auto_result_after_ball': market_template_result['wrAutoResultafterBall'], 'after_wicket_auto_suspend': market_template_result['wrAfterWicketAutoSuspend'], 'after_wicket_not_created': market_template_result['wrAfterWicketNotCreated'], 'is_active': market_template_result['wrIsActive'], 'last_update': time_now, 'market_type_category_id': market_template_result["wrMarketTypeCategoryId"], 'create':market_template_result["wrCreate"], 'create_ref_id':market_template_result["wrCreateRefId"],'open_ref_id':market_template_result["wrOpenRefId"],'action_type':market_template_result["wrActionType"], 'rate_source':1, 'market_template_id':market_template_result["wrID"],'market_type_id':market_template_result["wrMarketTypeId"], 'line_type':market_template_result["wrLineType"], 'default_back_size':market_template_result["wrDefaultBackSize"], 'default_lay_size':market_template_result["wrDefaultLaySize"] , 'default_is_send_data':market_template_result["wrDefaultIsSendData"], 'rate_diff':market_template_result["wrRateDiff"]})
            fow_event_market_id = result.fetchone()[0]
            row = { "wrID":fow_event_market_id, "wrCommentaryId": self.commentary_id, "wrEventRefID": self.event_id, "wrTeamID": self.current_team_id, "wrMarketName": fow_wicket_event_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": market_template_result['wrIsOver'], "wrOver": market_template_result['wrOver'], "wrIsPlayer": False, "wrPlayerID": None, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": 0, "wrMinOdds": 0, "wrMaxOdds": 0, "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"] }
            df=pd.DataFrame([row])
            player_markets = pd.concat([player_markets, df], ignore_index=True)
            player_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)

            
    
            LOG.info("Insert Success in the Fow event market %s", fow_event_market_id)
            session.commit()

            return player_markets.iloc[-1]

    def get_fow_event_market_by_eventID(self, fow_wicket_event_name, fow_market_template_result_df):
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        msg = ""
        try:
            # fow_event_query = text('''
            #     SELECT *
            #     FROM "tblEventMarkets"
            #     WHERE "wrCommentaryId" = :commentary_id
            #       AND "wrTeamID" = :team_id
            #       AND "wrMarketName" = :market_name
            # ''')
    
            # with get_db_session() as session:
            #     result = session.execute(fow_event_query, {
            #         'commentary_id': self.commentary_id,
            #         'team_id': self.current_team_id,
            #         'market_name': fow_wicket_event_name
            #     })
            #     fow_event_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            

        
            fow_event_df = player_event_markets[
                                 (player_event_markets['wrCommentaryId'] == self.commentary_id) &
                                 (player_event_markets['wrTeamID'] == self.current_team_id) &
                                 (player_event_markets['wrMarketName'] == fow_wicket_event_name)
                                 ]
            
            if fow_event_df.empty:
                autoOpenStatus = float(fow_market_template_result_df.get("wrAutoOpen", 0))
                status = OPEN if autoOpenStatus <= self.current_ball else INACTIVE
                fow_event_market_id = self.create_fow_event_market(fow_wicket_event_name, fow_market_template_result_df, status)
                fow_event_market_response = self.get_event_market_by_id(fow_event_market_id)
                fow_event_market_df = fow_event_market_response.get("data", pd.DataFrame())
                return {"status": True, "msg": "", "data": fow_event_market_df}
    
            return {"status": True, "msg": "", "data": fow_event_df}
    
        except Exception as err:
            LOG.error("Error found in get_fow_event_market_by_eventID: %s", str(err))
            msg = f"Error found in get_fow_event_market_by_eventID: {str(err)}"
    
        return {"status": False, "msg": msg}
        
    def get_fow_event_market(self, fow_market_template_result_df):
        msg = ""
        try:
            current_team_name_response = self.get_current_team_name_by_teamId()
            
            if not current_team_name_response.get("status", False):
                return current_team_name_response
            
            current_team_name = current_team_name_response.get("data")
            wicket_player_details = self.get_wicket_player_details()
            
            expected_wicket_to_fall = len(wicket_player_details) + 1
            fow_wicket_event_name = f"Fall of {expected_wicket_to_fall} wicket {current_team_name.strip()}"
            
            fow_event_market_response = self.get_fow_event_market_by_eventID(fow_wicket_event_name, fow_market_template_result_df)

            return fow_event_market_response
        except Exception as err:
            LOG.error("Get FOW event market %s", err)
            msg = "Error on getting FOW event market " + str(err)
        
        return  {"status": False, "msg": msg}
    
    def get_player_event_market(self, player_id, players_runs, market_template_result, is_close):
        market_template_result = market_template_result.to_dict("records")[0]        
        LOG.info("player id %s %s", player_id, players_runs["wrCommentaryPlayerId"])
        existing_player_event_market = self.check_existing_player_market(player_id, is_close, int(market_template_result.get("wrMarketTypeCategoryId")))

        if existing_player_event_market.get("status") and existing_player_event_market.get("data") is not None:
            player_event_market = existing_player_event_market.get("data", pd.DataFrame() )
        elif existing_player_event_market.get("status") and existing_player_event_market.get("data") is None:
            return None
        else:
            if self.total_wicket>=int(market_template_result.get("wrAfterWicketNotCreated",11)):
                return None
            batting_player = players_runs[players_runs["wrCommentaryPlayerId"] == player_id]
            if batting_player.empty:
                return None
            player_batting_order = batting_player["wrBatterOrder"].values[0]
            current_batting_order = player_batting_order - 2 if player_batting_order > 2 else 0
            current_ball_no = self.over_to_ball(self.current_ball)
            wicket_ball_count = self.wicket_ball_mapping.get(current_batting_order, 3)
            if not (wicket_ball_count <= current_ball_no):
                    return None
            # if player_batting_order - 2 == self.total_wicket:                            
            #     if not (self.last_wicket_ball <= current_ball_no):
            #         return None
            
            players_runs_row = players_runs[players_runs["wrCommentaryPlayerId"] == player_id]
            LOG.info(" players_runs_row %s %s", players_runs_row, players_runs["wrCommentaryPlayerId"])
            if players_runs_row.empty:
                return pd.DataFrame() 
            players_runs_dict = players_runs_row.to_dict("records")
            
            player_info = players_runs_dict[0]
            if player_info.get("wrBat_IsPlay"):
                status = OPEN
            else:
                status = INACTIVE
            autoOpenStatus = float(market_template_result.get("wrAutoOpen"))
                    
            if autoOpenStatus <= self.current_ball:
                status = OPEN 
            else:
                status = INACTIVE

            player_event_market = self.create_player_event_market(player_info, market_template_result, status) 
            
            #player_event_market_response = self.get_event_market_by_id(player_event_market_id)
            #player_event_market = player_event_market_response.get("data", pd.DataFrame())

        return player_event_market

    def get_runner_by_event_market_wrID(self, event_market_wrId):
        """
        Retrieve runner details by event market ID
        """
        LOG.info("event_market_wrId %s %s", event_market_wrId, type(event_market_wrId))
    
        runner_query = text('SELECT "wrRunnerId" FROM "tblMarketRunners" WHERE "wrEventMarketId" = :event_market_wrId')
        with get_db_session() as session:
            result = session.execute(runner_query, {'event_market_wrId': int(event_market_wrId)})
            existing_runner_market_df = pd.DataFrame(result.fetchall(), columns=['wrRunnerId'])
    
        if existing_runner_market_df.empty:
            return {"status": False, "msg": "Runner market is not available for eventID " + str(event_market_wrId)}
    
        return {"status": True, "msg": "", "data": existing_runner_market_df}
    
    def create_player_runner_record(self, player_event_market_details, predicted_player_score, over_value, under_value, wrBat_IsPlay, autoOpenStatus, lineType, defaultBackSize, defaultLaySize):
        """
        Create a new player runner record
        """
        LOG.info("columns %s ", player_event_market_details["wrID"].values)
        
        rateDiff=player_event_market_details["wrRateDiff"].values[0]
        
        event_market_wrId = int(player_event_market_details["wrID"].values[0])
        wrRunner = str(player_event_market_details["wrMarketName"].values[0])
        selection_id = f"{event_market_wrId}01"
        update_time = datetime.now()
        #if(lineType==STATICLINE):
        #    wrBackPrice=predicted_player_score
        #    wrLayPrice=predicted_player_score
        #elif(lineType==DYNAMICLINE):
        wrBackPrice = predicted_player_score + int(rateDiff)
        wrLayPrice=predicted_player_score
        wrBackSize=int(defaultBackSize)
        wrLaySize=int(defaultLaySize)
        if wrBat_IsPlay or autoOpenStatus <= self.current_ball:
            status = OPEN
        else:
            status = INACTIVE
    
        runner_query = text('''
            INSERT INTO "tblMarketRunners" (
                "wrEventMarketId", "wrRunner", "wrLine", "wrSelectionId", "wrLastUpdate",
                "wrSelectionStatus", "wrBackPrice", "wrLayPrice", "wrBackSize", "wrLaySize", "wrOverRate", "wrUnderRate"
            )
            VALUES (:event_market_wrId, :wrRunner, :predicted_player_score, :selection_id, :update_time,
                    :status, :wrBackPrice, :wrLayPrice, :wrBackSize, :wrLaySize, :over_rate, :under_rate)
            RETURNING "wrRunnerId"
        ''')
        
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'event_market_wrId': event_market_wrId,
                'wrRunner': wrRunner,
                'predicted_player_score': predicted_player_score,
                'selection_id': selection_id,
                'update_time': update_time,
                'status': status,
                'wrBackPrice': wrBackPrice,
                'wrLayPrice': wrLayPrice,
                'wrBackSize': wrBackSize,
                'wrLaySize': wrLaySize,
                'under_rate':under_value,
                'over_rate':over_value
            })
            runner_id = result.fetchone()[0]
            LOG.info("Insert Success in the player event runner market %s", runner_id)
            session.commit()
    
        return {"status": True, "data": runner_id}

    def create_fow_runner_record(self, fow_event_market_dict, fow_expected_score,over_value,under_value, autoOpenStatus,lineType, defaultBackSize, defaultLaySize):
        """
        Create a new FOW runner record
        """
        rateDiff=fow_event_market_dict["wrRateDiff"].values[0]
        
        event_market_wrId = int(fow_event_market_dict["wrID"])
        wrRunner = str(fow_event_market_dict["wrMarketName"])
        selection_id = f"{event_market_wrId}01"
        update_time = datetime.now()
        wrBackSize=int(defaultBackSize)
        wrLaySize=int(defaultLaySize)
        #if(lineType==STATICLINE):
        #    wrBackPrice=fow_expected_score
        #    wrLayPrice=fow_expected_score
        
        #elif(lineType==DYNAMICLINE):
        wrBackPrice = fow_expected_score + int(rateDiff)
        wrLayPrice=fow_expected_score
        if autoOpenStatus <= self.current_ball:
            status = OPEN
        else:
            status = INACTIVE
    
        runner_query = text('''
            INSERT INTO "tblMarketRunners" (
                "wrEventMarketId", "wrRunner", "wrLine", "wrSelectionId", "wrLastUpdate",
                "wrSelectionStatus", "wrBackPrice", "wrLayPrice", "wrBackSize", "wrLaySize", "wrOverRate", "wrUnderRate"
            )
            VALUES (:event_market_wrId, :wrRunner, :fow_expected_score, :selection_id, :update_time,
                    :status, :wrBackPrice, :wrLayPrice, :wrBackSize, :wrLaySize, :over_rate, :under_rate)
            RETURNING "wrRunnerId"
        ''')
        
            
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'event_market_wrId': event_market_wrId,
                'wrRunner': wrRunner,
                'fow_expected_score': fow_expected_score,
                'selection_id': selection_id,
                'update_time': update_time,
                'status': status,
                'wrBackPrice': wrBackPrice,
                'wrLayPrice':wrLayPrice,
                'wrBackSize': wrBackSize,
                'wrLaySize': wrLaySize,
                'over_rate':over_value,
                'under_rate':under_value
            })
            runner_id = result.fetchone()[0]
            LOG.info("Insert Success in the FOW event runner market %s", runner_id)
            session.commit()
    
        return {"status": True, "data": runner_id}
    
    def update_player_runner_record(self, event_market_wrId, predicted_player_score,over_value,under_value ,wrBat_IsPlay, autoOpenStatus, lineType,rateDiff):
        """
        Update the player runner record
        """
        LOG.info("Player event_market_wrId %s", event_market_wrId)
        update_time = datetime.now()
        #if(lineType==STATICLINE):
        #   wrBackPrice=predicted_player_score
        #    wrLayPrice=predicted_player_score
        #elif (lineType==DYNAMICLINE):
        wrBackPrice = predicted_player_score + int(rateDiff)
        wrLayPrice=predicted_player_score
        
        print("the predicted player score is " + str(predicted_player_score))
        if wrBat_IsPlay:
            status = OPEN
        else:
            status = INACTIVE
    
        if autoOpenStatus <= self.current_ball:
            status = OPEN
        else:
            status = INACTIVE
        
            
            
        runner_query = text('''
            UPDATE "tblMarketRunners"
            SET "wrLine" = :predicted_player_score,
                "wrLastUpdate" = :update_time,
                "wrSelectionStatus" = :status,
                "wrBackPrice" = :wrBackPrice,
                "wrLayPrice" = :wrLayPrice,
                "wrOverRate"=:over_rate,
                "wrUnderRate"=:under_rate
            WHERE "wrEventMarketId" = :event_market_wrId
            RETURNING "wrRunnerId"
        ''')
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'predicted_player_score': predicted_player_score,
                'update_time': update_time,
                'status': status,
                'wrBackPrice': wrBackPrice,
                'event_market_wrId': event_market_wrId,
                'under_rate':under_value,
                'over_rate':over_value,
                'wrLayPrice': wrLayPrice
            })
            updated_rwrRunnerId = result.fetchone()[0]
            LOG.info("Update is Success in the player event runner market %s", updated_rwrRunnerId)
            session.commit()

    def update_fow_runner_record(self, event_market_wrId, predicted_fow_score,over_value,under_value ,autoOpenStatus, lineType, rateDiff):
        """
        Update the FOW runner record
        """
        LOG.info("FOW event_market_wrId %s", event_market_wrId)
        update_time = datetime.now()
        #if(lineType==STATICLINE):
        #    wrBackPrice=predicted_fow_score
        #    wrLayPrice=predicted_fow_score
        #elif(lineType==DYNAMICLINE):    
        wrBackPrice = predicted_fow_score + int(rateDiff)
        wrLayPrice=predicted_fow_score
        if autoOpenStatus <= self.current_ball:
            status = OPEN
        else:
            status = INACTIVE
        runner_query = text('''
            UPDATE "tblMarketRunners"
            SET "wrLine" = :predicted_fow_score,
                "wrLastUpdate" = :update_time,
                "wrSelectionStatus" = :status,
                "wrBackPrice" = :wrBackPrice,
                "wrLayPrice" = :wrLayPrice,
                "wrOverRate"=:over_rate,
                "wrUnderRate"=:under_rate
            WHERE "wrEventMarketId" = :event_market_wrId
            RETURNING "wrRunnerId"
        ''')
        
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'predicted_fow_score': predicted_fow_score,
                'update_time': update_time,
                'status': status,
                'wrBackPrice': wrBackPrice,
                'event_market_wrId': event_market_wrId,
                'over_rate':over_value,
                'under_rate':under_value,
                'wrLayPrice':wrLayPrice
            })
            updated_rwrRunnerId = result.fetchone()[0]
            LOG.info("Update is Success in the FOW event runner market %s", updated_rwrRunnerId)
            session.commit()

    def get_player_rules_by_playerId(self, wrPlayerId, players_runs):
        for _, player in players_runs.iterrows():
            if player["wrCommentaryPlayerId"] == wrPlayerId:
                return player
        return pd.DataFrame()
    
    def run_player_prediction(self, commentaryPlayers, players_runs):
        wrPlayerId_request = commentaryPlayers["player_id"]
        
        player_rules = self.get_player_rules_by_playerId(wrPlayerId_request, players_runs)
        
        # LOG.info("player_rules %s ", player_rules)
        if player_rules.empty:
            return {"status": False, "msg": f"Player Rules is empty for {wrPlayerId_request}"}
        
        ### Business rule for player run prediction
        predefined_batsman_avg = player_rules["wrBatsmanAverage"]

        player_predicted_score = predefined_batsman_avg + commentaryPlayers["batRun"]

        return {"status": True, "data": player_predicted_score} 
    
    def run_player_boundaries_prediction(self, commentaryPlayers, players_runs):
        wrPlayerId_request = commentaryPlayers["player_id"]
        
        player_rules = self.get_player_rules_by_playerId(wrPlayerId_request, players_runs)
        
        # LOG.info("player_rules %s ", player_rules)
        if player_rules.empty:
            return {"status": False, "msg": f"Player Rules is empty for {wrPlayerId_request}"}
        
        ### Business rule for player run prediction
        predefined_batsman_boundaries = player_rules["wrBoundary"]

        boundaries_predicted_value = predefined_batsman_boundaries + commentaryPlayers["current_boundaries"]

        return {"status": True, "data": boundaries_predicted_value } 

    def update_player_market_status(self, wrBatterId, status, market_type_category_id,defaultIsSendData=None, current_team_id=None):
        """
        Update the Market Status for the players
        """
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        time_now = datetime.now()
        if not current_team_id:
            current_team_id = self.current_team_id
        is_send_data=True
        if status == CLOSE:
            runner_query = text(' UPDATE "tblEventMarkets" SET "wrStatus" = :status,"wrIsSendData"=:is_senddata  ,"wrCloseTime" = :time_now, "wrLastUpdate" = :time_now WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrPlayerID" = :playerId AND "wrIsPlayer" = :isPlayer and "wrStatus" not in (:cancel,:settle) and "wrMarketTypeCategoryId"=:market_type_category_id RETURNING "wrID" ')
        elif status == OPEN:
            runner_query = text(' UPDATE "tblEventMarkets" SET "wrStatus" = :status, "wrIsSendData"=:is_senddata, "wrOpenTime" = :time_now, "wrLastUpdate" = :time_now WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrPlayerID" = :playerId AND "wrIsPlayer" = :isPlayer and "wrStatus" not in (:cancel,:settle) and "wrMarketTypeCategoryId"=:market_type_category_id  RETURNING "wrID" ')
            is_send_data=bool(defaultIsSendData)
        else:
            runner_query = text('UPDATE "tblEventMarkets" SET "wrStatus" = :status, "wrIsSendData"=:is_senddata, "wrLastUpdate" = :time_now WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrPlayerID" = :playerId AND "wrIsPlayer" = :isPlayer and "wrStatus" not in (:cancel,:settle) and "wrMarketTypeCategoryId"=:market_type_category_id RETURNING "wrID" ')
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'status': status,
                'time_now': time_now,
                'commentaryId': self.commentary_id,
                'teamId': current_team_id,
                'playerId': wrBatterId,
                'isPlayer': True,
                "is_senddata":is_send_data,
                "cancel":CANCEL,
                "settle":SETTLED,
                "market_type_category_id":market_type_category_id
            })
            try:
                updated_rwMarketId = result.fetchone()[0]
                LOG.info("Update is Success in the player event market %s", updated_rwMarketId)

                filtered_row = player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId]
                filtered_row.at[filtered_row.index[0], 'wrStatus'] = status
                filtered_row.at[filtered_row.index[0], 'wrIsSendData'] = is_send_data
                filtered_row.at[filtered_row.index[0], 'wrLastUpdate'] = time_now
                player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId] = filtered_row

                player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)
                session.commit()
                
            except Exception as err:
                LOG.info("Wicket Player Event market is not found %s", wrBatterId)

    def update_player_market_results(self, wrPlayerRun, wrBatterId, status, marketTypeCategoryId):
        """
        Update the Market Results for the players
        """
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        settled_time = datetime.now()
        runner_query = text('UPDATE "tblEventMarkets" SET "wrStatus" = :status, "wrResult" = :wrResult, "wrIsResult" = :wrIsResult, "wrSettledTime" = :settledTime WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrPlayerID" = :playerId AND "wrIsPlayer" = :isPlayer and "wrMarketTypeCategoryId"=:market_type_category_id and "wrStatus" = :close RETURNING "wrID" ')
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'status': status,
                'wrResult': wrPlayerRun,
                'wrIsResult': False,
                'settledTime': settled_time,
                'commentaryId': self.commentary_id,
                'teamId': self.current_team_id,
                'playerId': wrBatterId,
                'isPlayer': True,
                'market_type_category_id': marketTypeCategoryId,
                'close':CLOSE
            })
            updated_rwMarketId = result.fetchone()[0]
            if updated_rwMarketId is not None:
                filtered_row = player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId]
                filtered_row.at[filtered_row.index[0], 'wrStatus'] = status
                filtered_row.at[filtered_row.index[0], 'wrResult'] = wrPlayerRun
                filtered_row.at[filtered_row.index[0], 'wrIsResult'] = False
                filtered_row.at[filtered_row.index[0], 'wrSettledTime'] = settled_time
    
                player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId] = filtered_row
                
            player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)
                
            LOG.info("Update is Success in the player event market results %s", updated_rwMarketId)

    def update_fow_market_results(self, eventID, wrteam_score, status):
        """
        Update the Market Results for the players
        """
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        settled_time = datetime.now()
    
        runner_query = text('UPDATE "tblEventMarkets" SET "wrStatus" = :status, "wrResult" = :wrResult, "wrIsResult" = :wrIsResult, "wrSettledTime" = :settledTime WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrID" = :eventID and "wrStatus" = :status RETURNING "wrID" ')
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'status': status,
                'wrResult': wrteam_score,
                'wrIsResult': False,
                'settledTime': settled_time,
                'commentaryId': self.commentary_id,
                'teamId': self.current_team_id,
                'eventID': eventID,
                'close':CLOSE
            })
    
            updated_rwMarketId = result.fetchone()[0]
            filtered_row = player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId]
            filtered_row.at[filtered_row.index[0], 'wrStatus'] = status
            filtered_row.at[filtered_row.index[0], 'wrResult'] = wrteam_score
            filtered_row.at[filtered_row.index[0], 'wrIsResult'] = False
            filtered_row.at[filtered_row.index[0], 'wrSettledTime'] = settled_time

            player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId] = filtered_row
            
            player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)
            
            LOG.info("Update is Success in the FOW event market results %s", updated_rwMarketId)
        session.commit()
    def update_fow_market_status(self, wrEventId, status, defaultIsSendData=None):
        """
        Update the FOW Market Status for the team
        """
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        is_send_data=True
        time_now = datetime.now()
        if status == CLOSE:
            runner_query = text('UPDATE "tblEventMarkets" SET "wrStatus" = :status, "wrCloseTime" = :closeTime, "wrLastUpdate" = :lastUpdate WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrID" = :wrEventId RETURNING "wrID" ')
            is_send_data
        elif status == OPEN:
            is_send_data=bool(defaultIsSendData)
            runner_query = text('UPDATE "tblEventMarkets" SET "wrStatus" = :status,"wrIsSendData"=:is_senddata , "wrOpenTime" = :openTime, "wrLastUpdate" = :lastUpdate WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrID" = :wrEventId RETURNING "wrID" ')
        else:
            runner_query = text('UPDATE "tblEventMarkets" SET "wrStatus" = :status, "wrLastUpdate" = :lastUpdate WHERE "wrCommentaryId" = :commentaryId AND "wrTeamID" = :teamId AND "wrID" = :wrEventId RETURNING "wrID" ')
        with get_db_session() as session:
            result = session.execute(runner_query, {
                'status': status,
                'closeTime': time_now if status == CLOSE else None,
                'openTime': time_now if status == OPEN else None,
                'lastUpdate': time_now,
                'commentaryId': self.commentary_id,
                'teamId': self.current_team_id,
                'wrEventId': wrEventId,
                "is_senddata":is_send_data 
            })
    
            updated_rwMarketId = result.fetchone()[0]
            
            filtered_row = player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId]
            filtered_row.at[filtered_row.index[0], 'wrStatus'] = status
            filtered_row.at[filtered_row.index[0], 'wrIsSendData'] = is_send_data
            filtered_row.at[filtered_row.index[0], 'wrLastUpdate'] = time_now
            if status==CLOSE:
                filtered_row.at[filtered_row.index[0], 'wrCloseTime'] = time_now
            elif status==OPEN:
                filtered_row.at[filtered_row.index[0], 'wrOpenTime'] = time_now
                
            player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId] = filtered_row

            player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)
            
            LOG.info("Update is Success in the FOW event market %s", updated_rwMarketId)
        session.commit()

    def over_to_ball(self, over_number):
        # Split the over number into integer and decimal parts
        over, ball = divmod(over_number, 1)
        
        # Calculate the total number of balls
        total_balls = int(over) * 6 + round(float(ball * 10))
        
        return total_balls

    def get_player_id_from_commentary(self, player_id):
        try:
            player_query = text('''
                SELECT "wrCommentaryPlayerId" 
                FROM "tblCommentaryPlayers" 
                WHERE "wrCommentaryId" = :commentaryId 
                  AND "wrTeamId" = :teamId 
                  AND "wrCommentaryPlayerId" = :playerId
            ''')
    
            with get_db_session() as session:
                result = session.execute(player_query, {
                    'commentaryId': self.commentary_id,
                    'teamId': self.current_team_id,
                    'playerId': player_id
                })
                player_id = result.scalar()
                LOG.info("Retrieved player id %s", player_id)
                return player_id
    
        except Exception as err:
            LOG.error("Error while fetching player ID: %s", err)
        
        return None

    def overs_to_balls(self, value):
        if(int(value)==0):
            total_balls=int(value*10)
        else:
            total_balls=int((round((value-int(value)),1)*10)+(int(value)*6))
        
        return total_balls

    def get_wicket_player_details(self):
        player_runner_query = text('''
            SELECT w."wrBatterId" as wrBatterId, w."wrPlayerRun" as wrPlayerRun, w."wrTeamScore" as wrTeamScore,  
                   w."wrWicketCount" as wrWicketCount, w."wrCommentaryBallByBallId" as wrCommentaryBallByBallId, 
                   b."wrOverCount" as wrOverCount
            FROM "tblCommentaryWickets" w
            JOIN "tblCommentaryBallByBalls" b
            
            ON w."wrCommentaryBallByBallId" = b."wrCommentaryBallByBallId"
            
            WHERE w."wrCommentaryId" = :commentaryId and b."wrTeamId"=:teamId
            ORDER BY b."wrOverCount" ASC  
        ''')
        player_wicket_df = pd.DataFrame()
        try:
            with get_db_session() as session:
                result = session.execute(player_runner_query, {'commentaryId': self.commentary_id, 'teamId':self.current_team_id})
                player_wicket_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
            return player_wicket_df
    
        except Exception as err:
            LOG.error("Error fetching wicket player details: %s", str(err))
            return player_wicket_df
    
        return player_wicket_df
    
    def player_wicket_update(self, market_template_result_df, player_wicket_df):
        """
        Update the player wicket results based on the market template
        """
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        # use the list for getting the Closed market player Id
        player_ids_list = []

        autoResultaferball = market_template_result_df["wrAutoResultafterBall"].values[0]        
        defaultIsSendData=market_template_result_df["wrDefaultIsSendData"].values[0]
        marketTypeCategoryId=int(market_template_result_df["wrMarketTypeCategoryId"].values[0])
        self.total_wicket = len(player_wicket_df)
        if len(player_wicket_df) == 0:
            return player_ids_list
        
        #query=text('select "wrStatus" from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrPlayerID"=:player_id and "wrMarketTypeCategoryId"=:market_type_category_id')
        boundary_result_query=text('SELECT COALESCE("wrBat_FOUR", 0) + COALESCE("wrBat_SIX", 0) AS "wrPlayerBoundary" FROM "tblCommentaryPlayers" WHERE "wrCommentaryId"=:commentary_id AND "wrCommentaryPlayerId"=:player_id')
        
        for _, row in player_wicket_df.iterrows():
            try:
                LOG.debug("Wicket row %s", row)
                wrPlayerRun = row["wrplayerrun"]
                wrBatterId = row["wrbatterid"]
                player_out_ball = row["wrovercount"]
                wrBatterId = self.get_player_id_from_commentary(wrBatterId)
                wicket_ball = self.overs_to_balls(float(player_out_ball))
                suspend_ball_no = wicket_ball
                close_ball_no = wicket_ball + 1
                auto_result_set_ball = wicket_ball + autoResultaferball
                
                current_ball_over = self.overs_to_balls(self.current_ball)
                
                #with get_db_session() as session:
                #    result = pd.read_sql(query, session.bind, params={"commentary_id":self.commentary_id, "player_id":wrBatterId, "market_type_category_id": marketTypeCategoryId})
                result = player_event_markets[
                            (player_event_markets['wrCommentaryId'] == self.commentary_id) &
                            (player_event_markets['wrPlayerID'] == wrBatterId) &
                            (player_event_markets['wrMarketTypeCategoryId'] == marketTypeCategoryId)  
                            ]
                if not result.empty:
                    print("the values are ",result)
                    LOG.info("Current ball %s suspend ball %s result ball %s", current_ball_over, suspend_ball_no, auto_result_set_ball)
                    if int(result["wrStatus"].iloc[0])!=CLOSE and ((current_ball_over == suspend_ball_no) or (wicket_ball < current_ball_over < close_ball_no) ):    
                        player_ids_list.append({"player_id": wrBatterId, "team_id": self.current_team_id, "batRun": wrPlayerRun})
                        self.update_player_market_status(wrBatterId, SUSPEND,marketTypeCategoryId, defaultIsSendData)
                    elif current_ball_over == close_ball_no:        
                        self.update_player_market_status(wrBatterId, CLOSE,marketTypeCategoryId, defaultIsSendData)
                        player_ids_list.append({"player_id": wrBatterId, "team_id": self.current_team_id, "batRun": wrPlayerRun})
                    elif current_ball_over == auto_result_set_ball and int(result["wrStatus"].iloc[0])==CLOSE: 
                        if marketTypeCategoryId==PLAYERBOUNDARIES:
                            with get_db_session() as session:
                                player_boundary_df = pd.read_sql(boundary_result_query, session.bind, params={"commentary_id":self.commentary_id, "player_id":wrBatterId})
                                boundaries=int(player_boundary_df["wrPlayerBoundary"].iloc[0])
                                self.update_player_market_results(boundaries, wrBatterId, SETTLED, marketTypeCategoryId)
    
                        elif marketTypeCategoryId==PLAYER:
                            self.update_player_market_results(wrPlayerRun, wrBatterId, SETTLED, marketTypeCategoryId)
                    elif current_ball_over > auto_result_set_ball and int(result["wrStatus"].iloc[0])==CLOSE:
                        self.update_player_market_status(wrBatterId, SETTLED,marketTypeCategoryId , defaultIsSendData)
                
                    else:
                        LOG.info("Player Wicket after close update is already done")
                else:
                    LOG.info("Player wicket empty dataframe")
            except Exception as err:
                LOG.error("Error found while updating player wicket status %s", str(err))
                traceback.print_exc()

        return player_ids_list
    
    def update_event_data(self, event_market_wrId, event_data_dict, defaultIsSendData):
        try:
            player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

            wrdata_query = text('UPDATE "tblEventMarkets" SET "wrData" = :wrData, "wrIsSendData" = :wrIsSendData, "wrLastUpdate" = :wrLastUpdate WHERE "wrCommentaryId" = :wrCommentaryId AND "wrID" = :wrID RETURNING "wrID" ')
            time_now = datetime.now()
            data=json.loads(event_data_dict)
            LOG.debug("Updated wrData %s", event_data_dict)
            with get_db_session() as session:
                result = session.execute(wrdata_query, {
                    'wrData': event_data_dict,
                    'wrIsSendData': defaultIsSendData if data["status"]==OPEN else True,
                    'wrLastUpdate': time_now,
                    'wrCommentaryId': self.commentary_id,
                    'wrID': event_market_wrId
                })
                updated_rwMarketId = result.fetchone()[0]
                
                filtered_row = player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId]
                filtered_row.at[filtered_row.index[0], 'wrData'] = event_data_dict
                filtered_row.at[filtered_row.index[0], 'wrIsSendData'] = defaultIsSendData if data["status"]==OPEN else True
                filtered_row.at[filtered_row.index[0], 'wrLastUpdate'] = time_now
                player_event_markets.loc[player_event_markets['wrID'] == updated_rwMarketId] = filtered_row
                player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)
                
                session.commit()
            LOG.info("Update is Success for WrData %s", updated_rwMarketId)
        except Exception as err:
            LOG.error("Not able to update wrData %s", str(err))

    def get_socket_data(self, event_market_wrId):
        status = False
        result = {}
        try:
            # Raw SQL query to fetch event and market runner data
            wrDataQuery = text('''
                SELECT eventMarket."wrID" as marketId, eventMarket."wrCommentaryId" as commentaryId, eventMarket."wrEventRefID" as eventId,
                       eventMarket."wrMarketName" as marketName, eventMarket."wrStatus" as status, eventMarket."wrIsActive" as isActive,
                       eventMarket."wrIsAllow" as isAllow, eventMarket."wrIsSendData" as isSendData, eventMarket."wrTeamID" as teamId,
                       eventMarket."wrMargin" as margin, eventMarket."wrOver" as over, eventMarket."wrInningsID" as inningsId, eventMarket."wrLineRatio" as lineRatio,
                       eventMarket."wrMarketTypeCategoryId" as marketTypeCategoryId,
                       eventMarket."wrMarketTypeId" as marketTypeId,
                       
                       (SELECT "wrCommentaryBallByBallId"
                         FROM "tblCommentaryBallByBalls"
                         WHERE "wrCommentaryId" = eventMarket."wrCommentaryId"
                           AND "wrTeamId" = eventMarket."wrTeamID"
                         ORDER BY "wrCommentaryBallByBallId" DESC
                         LIMIT 1) AS ballByBallId,
                       eventMarket."wrLineType" as lineType,
                       eventMarket."wrRateDiff" as rateDiff,
                       marketRunner."wrRunnerId" as runnerId, marketRunner."wrSelectionStatus" as runnerStatus, marketRunner."wrLine" as line, 
                       marketRunner."wrOverRate" as overRate, marketRunner."wrUnderRate" as underRate, marketRunner."wrBackPrice" as backPrice,
                       marketRunner."wrLayPrice" as layPrice,  marketRunner."wrBackSize" as backSize,  marketRunner."wrLaySize" as laySize,
                       
                       eventMarket."wrDefaultIsSendData" as defaultIsSendData
                
                FROM "tblEventMarkets" eventMarket
                JOIN "tblMarketRunners" marketRunner
                ON eventMarket."wrID" = marketRunner."wrEventMarketId"
                WHERE eventMarket."wrCommentaryId" = :commentary_id AND eventMarket."wrID" = :event_market_wrId
            ''')
    
            with get_db_session() as session:
                wr_data_df = pd.read_sql(wrDataQuery, session.bind, params={"commentary_id":self.commentary_id, "event_market_wrId":event_market_wrId})
            if wr_data_df.empty or len(wr_data_df) != 1:
                status = False
                return {"status": status, "msg": "there is no event data to send for scocket"}
            wr_data_df["overrate"].fillna(0, inplace=True)
            wr_data_df["underrate"].fillna(0, inplace=True)
            wr_data_df["over"].fillna(0, inplace=True)
            defaultIsSendData=wr_data_df["defaultissenddata"].iloc[0]
            selected_runner_cols = ["runnerId", "status", "line", "overRate", "underRate", "backPrice", "layPrice", "backSize", "laySize"]
            selected_event_cols = ["marketId", "commentaryId", "eventId", "marketName", "status", "isActive", "isAllow", "isSendData", "teamId", "margin", "over", "inningsId", "lineRatio","marketTypeCategoryId","marketTypeId","ballByBallId","lineType", "rateDiff"]
            wr_data_df.columns = ["marketId", "commentaryId", "eventId", "marketName", "status",
                                "isActive", "isAllow", "isSendData", "teamId", "margin", "over",
                                "inningsId", "lineRatio","marketTypeCategoryId","marketTypeId","ballByBallId", "lineType","rateDiff","runnerId", "runner_status", "line", "overRate", "underRate", "backPrice", "layPrice", "backSize", "laySize","defaultIsSendData"]
            
            
            selected_event_wrdata_cols=["marketId", "eventId", "marketName", "status", "isActive", "isAllow"]
            wr_data=wr_data_df[selected_event_wrdata_cols]
            wr_data_dict = wr_data.to_dict("records")[0]
            
            
            event_data = wr_data_df[selected_event_cols]        
            runner_data = wr_data_df[selected_runner_cols]

            event_data_dict = event_data.to_dict("records")[0]
            event_data_dict["runner"] = runner_data.to_dict("records")
            
            wr_data_dict["runner"] = runner_data.to_dict("records")
            
            result = json.dumps(event_data_dict)
            self.update_event_data(event_market_wrId, json.dumps(wr_data_dict), bool(defaultIsSendData))
            status = True
    
        except Exception as err:
            LOG.error("Error fetching wrData in get_socket_data: %s", str(err))
    
        return {"status": status, "data": result}
    
    def get_fow_wicket_event_details(self, current_team_name_response, total_wicket):
        fow_event_name = f"Fall of {total_wicket} wicket {current_team_name_response}"
        player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        # fow_event_query = text('SELECT * FROM "tblEventMarkets" WHERE "wrCommentaryId" = :commentary_id AND "wrTeamID" = :team_id AND "wrMarketName" = :event_name')
    
        # with get_db_session() as session:
        #     result = session.execute(fow_event_query, {
        #         'commentary_id': self.commentary_id,
        #         'team_id': self.current_team_id,
        #         'event_name': fow_event_name
        #     })
        
        #     result_list = [dict(row) for row in result.fetchall()]
        # fow_event_df = pd.DataFrame(result_list)
        fow_event_df = player_event_markets[
                             (player_event_markets['wrCommentaryId'] == self.commentary_id) &
                             (player_event_markets['wrTeamID'] == self.current_team_id) &
                             (player_event_markets['wrMarketName'] == fow_event_name)
                             ]
        return fow_event_df
    
    def find_team_ids_by_commentaryid(self):
        unique_team_id = []
        try:
            with get_db_session() as session:
                bowler_team_query = text('select DISTINCT "wrTeamId" from "tblCommentaryPlayers" where "wrCommentaryId"=:commentary_id and "wrTeamId" != :current_team_id')
                unique_team_id = pd.read_sql_query(bowler_team_query,session.bind,params={"commentary_id":self.commentary_id, "current_team_id": self.current_team_id})
            
                if len(unique_team_id) == 0:
                    return {"status": False, "msg": "Bowler team detail is not found"}  
                elif len(unique_team_id) > 0:
                    unique_team_id = unique_team_id["wrTeamId"].values[0]
                                
        except Exception as err:
            LOG.error("Error in finding bowler team id from tblCommentaryPlayers table.")
            return {"status": False, "msg": "Bowler team detail is not found"}  
        return {"status": True, "data": unique_team_id}
    
    def close_bowling_team_open_markets(self, bowling_team_id):        
        closed_market_socket_data = []
        if bowling_team_id:
            try:
                bowling_team_id = int(bowling_team_id)
            except Exception as err:
                LOG.error("Bowling team ID value is not valid %s", bowling_team_id)
                return False
            
            bowling_team_open_player_markets = self.get_open_player_market_byteamid(bowling_team_id)
            if not bowling_team_open_player_markets["status"]:
                return []
            
            data = bowling_team_open_player_markets["data"]
            for idx, player_info in data.iterrows():
                try:
                    event_market_wrId = player_info["wrID"]
                    wrBatterId = player_info["wrPlayerID"]
                    self.update_player_market_status(wrBatterId, CLOSE,int(player_info["wrMarketTypeCategoryId"]) ,True,current_team_id=bowling_team_id)
                    wr_data = self.get_socket_data(event_market_wrId)
                    if not wr_data.get("status"):
                        LOG.error("Get Socket data is not succesfull %s", wr_data)
                    
                    closed_market_socket_data.append(wr_data.get("data"))
                except Exception as err:
                    pass
        else:
            pass
        return closed_market_socket_data

    
    def fow_market_update(self, market_template_result_df):
        """
        Update the player wicket results based on the market template
        """
        current_team_name_response = self.get_current_team_name_by_teamId()
        
        if not current_team_name_response.get("status", False):
            return current_team_name_response 
                 
        # Use pandas to execute the query and read the results into a DataFrame
        fow_wicket_df = self.get_wicket_player_details()

        autoResultaferball = market_template_result_df["wrAutoResultafterBall"].values[0]        
        defaultIsSendData=market_template_result_df["wrDefaultIsSendData"].values[0]   
        self.total_wicket = len(fow_wicket_df)
        if len(fow_wicket_df) == 0:
            return {"status": True, "msg": "FOW wicket is not available."}        
               
        for _, row in fow_wicket_df.iterrows():
            try:
                LOG.debug("FOW Wicket row %s", row)
                wicket_no = int(row["wrwicketcount"])
                # ## this should be removed once wicket count rectified
                # wicket_no = int(wicket_no - 1)
                fow_event_wicket_df = self.get_fow_wicket_event_details(current_team_name_response.get("data"), wicket_no)   

                if len(fow_event_wicket_df) != 1:
                    LOG.debug("FOW Event market is not available.")
                    continue
            
                fow_eventID = int(fow_event_wicket_df["wrID"].values[0])
                wrStatus = fow_event_wicket_df["wrStatus"].values[0]     
                if wrStatus == 5:
                    continue 
                
                wrteam_run = int(row["wrteamscore"])
                player_out_ball = row["wrovercount"]   
                wicket_ball = self.overs_to_balls(float(player_out_ball))
                suspend_ball_no = wicket_ball
                close_ball_no = wicket_ball + 1
                auto_result_set_ball = wicket_ball + autoResultaferball
                
                current_ball_over = self.overs_to_balls(self.current_ball)
                LOG.info("FOW Current ball %s suspend ball %s result ball %s", current_ball_over, suspend_ball_no, auto_result_set_ball)
                if current_ball_over == suspend_ball_no:
                    self.update_fow_market_status(fow_eventID, SUSPEND)
                elif current_ball_over == close_ball_no:        
                    self.update_fow_market_status(fow_eventID, CLOSE)
                elif current_ball_over == auto_result_set_ball:                
                    self.update_fow_market_results(fow_eventID, wrteam_run, SETTLED)
                elif current_ball_over > auto_result_set_ball:
                    self.update_fow_market_status(fow_eventID, SETTLED)
                else:
                    LOG.info("FOW Wicket after close update is already done")
            except Exception as err:
                LOG.error("Error found in FOW wicket update %s", str(err))

    def run_fow_market(self, player_runs_details, fow_market_template_result_df):
        status = False
        try:
            LOG.info("Running FOW prediction : %s", player_runs_details)
            
            # market_template_result = self.get_market_template(wicket)

            # if not market_template_result.get("status"):
            #    return {"status": true, "msg": none}

            # fow_market_template_result_df = market_template_result.get("data")
            
            
            
            self.fow_market_update(fow_market_template_result_df)

            player_id_list = [player_info["player_id"] for player_info in player_runs_details]
            
            LOG.info("player_id_list to retrieve %s", player_id_list)
            players_runs = self.get_players_details(player_id=player_id_list)

            if not players_runs.get("status"):
                return players_runs
            
            fow_event_market_details = self.get_fow_event_market(fow_market_template_result_df)

            if not fow_event_market_details.get("status"):
                LOG.error("FOW event market is not available and not able to create %s", fow_market_template_result_df) 
                return {"status": status, "msg": "FOW event market is not available and not able to create"}   
            
            fow_event_market_df = fow_event_market_details.get("data", pd.DataFrame())

            if len(fow_event_market_df) != 1:
                LOG.error("More than 1 or 0 FOW event market is available %s", len(fow_event_market_df)) 
                return {"status": status, "msg": "More than 1 or 0 FOW event market is available "+ str(len(fow_event_market_df))}              

                    
            fow_event_market_dict = fow_event_market_df.to_dict("records")[0]
            fow_eventID = str(fow_event_market_dict["wrID"])
            # create FOW Market Runner
            auto_close_type = fow_event_market_dict.get("wrAutoCloseType")
            auto_close_over = fow_event_market_dict.get("wrBeforeAutoClose")
            after_wicket_not_create = fow_event_market_dict.get("wrAfterWicketNotCreated", 11)
        
            wrAutoOpen = float(fow_market_template_result_df.get("wrAutoOpen", 0))            

            if (self.total_wicket >= after_wicket_not_create):
                print("FOW market: after_wicket_not_create ", after_wicket_not_create, " should not create new FOW market")
                return {"status": True, "msg": ""} 
            
            players_runs_df = players_runs["data"]
            players_runs_df[["wrBatsmanAverage", "wrBat_Run"]] = players_runs_df[["wrBatsmanAverage", "wrBat_Run"]].astype(float)
            players_runs_df[["wrBatsmanAverage", "wrBat_Run"]].fillna(0, inplace=True)
            LOG.info("players_runs_df batsman average %s", players_runs_df[["wrBatsmanAverage", "wrBat_Run"]])
            ### Business rule for player run prediction      
            players_runs_df["player_predicted_score"] = np.sum(players_runs_df[["wrBatsmanAverage", "wrBat_Run"]], axis=1)

            LOG.info("Team current score %s, players individual score %s", self.current_score, players_runs_df["player_predicted_score"].values)
            fow_expected_score = round(self.current_score + players_runs_df["player_predicted_score"].sum())
            
    

            self.update_event_market_ods(fow_event_market_details["wrID"].values[0], fow_expected_score)
            if fow_expected_score > 0:
                fow_expected_score = round(fow_expected_score / 2)
            LOG.info("fow_expected_score %s", fow_expected_score)           

            over_value,under_value=self.get_over_under_rates(fow_expected_score,fow_event_market_details["wrMargin"].values[0])

            after_over_close_market_status = self.check_after_over_close_market(auto_close_type, auto_close_over)
            
            if (after_over_close_market_status)  & (self.total_wicket <= after_wicket_not_create):        
                self.update_fow_market_status(fow_eventID, CLOSE)        
                return {"status": True, "msg": "Successfully updated FoW market"}
        
            fow_runner_details = self.get_runner_by_event_market_wrID(fow_eventID)

            if not fow_runner_details.get("status"):
                        
                fow_runner_details = self.create_fow_runner_record(fow_event_market_dict, fow_expected_score,over_value,under_value, wrAutoOpen, fow_event_market_dict["wrLineType"], fow_event_market_dict["wrBackSize"], fow_event_market_dict["wrLaySize"])

                if not fow_runner_details.get("status"):
                    LOG.error("FoW runner is not able to create")
                    return {"status": False, "msg": "FoW runner is not able to create"}
                    
            else:
                
                fow_runner_details = fow_runner_details.get("data")                

                self.update_fow_runner_record(fow_eventID, fow_expected_score,over_value,under_value ,wrAutoOpen, fow_event_market_dict["wrLineType"], fow_event_market_dict["wrRateDiff"])                    

                if wrAutoOpen <= self.current_ball:
                    status = OPEN 
                    self.update_fow_market_status(fow_eventID, OPEN, fow_event_market_details["wrDefaultIsSendData"].values[0])
            
            wr_data = self.get_socket_data(fow_eventID)

            if not wr_data.get("status"):
                LOG.error("Get Socket data is not succesfull %s", wr_data)
            
            self.send_data_to_socket(wr_data)
            player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))
            player_event_markets.loc[player_event_markets['wrID'] == fow_event_market_details["wrID"].values[0]] = fow_event_market_details.values[0]
            player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)

            status = True
        except Exception as err:
            status = False
            LOG.error("Error in FOW Market %s", err)
            traceback.print_exc()
        
        return {"status": status, "msg": ""}

    def run_player_market(self, player_runs_details, market_template_result_df):
        status = False
        try:
            socket_data = []
            stime=time.time()
            LOG.info("Running run_player_prediction : %s", player_runs_details)
        
            #market_template_result = self.get_market_template(template_name)

            # if not market_template_result.get("status"):
            #   return {"status": True, "msg": None, "data":None}

            # market_template_result_df = market_template_result.get("data")
            ptime=time.time()
            players_runs = self.get_players_details()

            if not players_runs.get("status"):
               return {"status": status, "msg": players_runs.get("msg"), "data":None}

            # check autocreate event
            create_type_id = market_template_result_df["wrCreateType"].values
            if create_type_id[0] == 1:
                autoCreateBall = market_template_result_df["wrCreate"]

                if self.current_ball < float(autoCreateBall):
                    LOG.debug("current_ball is %s and market template autocreate ball is %s", self.current_ball, autoCreateBall)                    
                    return {"status": status, "msg": f"AutoStart ball is yet to start. the current ball {self.current_ball} is behing the condition of autocreate ball {autoCreateBall}", "data":None}

            players_runs = players_runs.get("data")
            
            player_wicket_df = self.get_wicket_player_details()
            
            market_template_result_dict = market_template_result_df.to_dict("records")[0] 
            if not player_wicket_df.empty:
                # after 3rd ball to create a new player run market by default.
                # this can be changed to take from market template later.
                self.wicket_ball_mapping = OrderedDict()
                self.total_wicket = len(player_wicket_df)
                for _, wicket_row in player_wicket_df.iterrows():
                    player_wrOverCount = wicket_row["wrovercount"]
                    player_wrWicketCount = wicket_row["wrwicketcount"]
                    wicket_ball = self.overs_to_balls(float(player_wrOverCount) + float(self.last_wicket_ball_count / 10))
                    self.wicket_ball_mapping[player_wrWicketCount] = wicket_ball
                    
                last_row_wrOverCount = player_wicket_df.iloc[-1]["wrovercount"]
                
                self.last_wicket_ball = self.overs_to_balls(float(last_row_wrOverCount) + float(self.last_wicket_ball_count / 10))
                
            try:
                LOG.debug("last wicket ball is %s", self.last_wicket_ball)
                closed_market_player_id = self.player_wicket_update(market_template_result_df, player_wicket_df)
                if not player_wicket_df.empty:
                    wicket_query=text('Select "wrID" from "tblEventMarkets" where "wrCommentaryId"=:commentaryid and "wrPlayerID"=:player_id and "wrStatus" not in (:close)')
                    
                    with get_db_session() as session:
                        result = pd.read_sql_query(wicket_query, session.bind, params={'commentaryid': self.commentary_id, 'player_id':int(player_wicket_df["wrbatterid"].iloc[-1]), "close":CLOSE})
                    if not result.empty:
                        wicket_data=self.get_socket_data(int(result['wrID'].iloc[0]))
                    
                        if not wicket_data.get("status"):
                            LOG.error("Get Socket data is not succesfull %s", wicket_data)                   
                        socket_data.append(wicket_data.get("data"))
            except Exception as err:
                print("error in updating wicket players events ", err)
            
                    
            for closed_player_details in closed_market_player_id:
                try:
                    closed_player_id = closed_player_details.get("player_id", 0)
                    # get the closed market player details
                    closed_player_runs_details = self.get_players_details(player_id=[closed_player_id])
                    if not closed_player_runs_details.get("status"):
                        continue
                    closed_player_runs_details = closed_player_runs_details.get("data")
                    # get closed market 
                    closed_player_event_market_details = self.get_player_event_market(closed_player_id, closed_player_runs_details, market_template_result_df, True)  
                    if closed_player_event_market_details is None or closed_player_event_market_details.empty:
                        continue
                    closed_event_market_wrId = str(closed_player_event_market_details["wrID"].values[0])

                    wr_data = self.get_socket_data(closed_event_market_wrId)

                    if not wr_data.get("status"):
                        LOG.error("Get Socket data is not succesfull %s", wr_data)
                        continue
                    
                    socket_data.append(wr_data.get("data"))
                except Exception as err:
                    LOG.error("Error in getting closed player market details %s", err)
            
            # find the bowling teamid based on the current innings
            
            # bowling_team_details = [player_info for player_info in player_runs_details if player_info["team_id"] != self.current_team_id]
            
            bowling_team_details = self.find_team_ids_by_commentaryid()
            if bowling_team_details["status"]:
                bowling_team_id = bowling_team_details["data"]
                # close the previous innings open market
                closed_makert_socket_data = self.close_bowling_team_open_markets(bowling_team_id)
                if closed_makert_socket_data:
                    socket_data.extend(closed_makert_socket_data)
               
            for _, player_info in enumerate(player_runs_details):
                try:
                    ptime=time.time()
                    if self.current_team_id != player_info.get("team_id"):
                        LOG.debug("Current team id is not matching %s %s", self.current_team_id, player_info)
                        continue
                    auto_close_over_or_wicket_status = False
                    
                    
                    market_template_result_dict = market_template_result_df.to_dict("records")[0]                    

                    auto_close_type = market_template_result_dict.get("wrAutoCloseType")
                    auto_close_over = market_template_result_dict.get("wrBeforeAutoClose")
                    after_wicket_not_create = market_template_result_dict.get("wrAfterWicketNotCreated", 11)
                    wrAutoOpen = float(market_template_result_dict.get("wrAutoOpen", 0))
                    
                    
                    #if (self.total_wicket >= after_wicket_not_create):
                    #    print("Player market: after_wicket_not_create ", after_wicket_not_create, " should not create new Player market")
                    #    return {"status": True, "msg": ""} 
                    
                    
                    player_id = player_info["player_id"]                     
                    player_event_market_details = self.get_player_event_market(player_id, players_runs, market_template_result_df, False)      
                    if player_event_market_details is None or player_event_market_details.empty:
                        continue
                    event_market_wrId = str(player_event_market_details["wrID"].values[0])
                    wrBatterId = str(player_event_market_details["wrPlayerID"].values[0])

                    LOG.info("player_event_market_details %s", player_event_market_details)  

                    if player_event_market_details.empty:
                        LOG.error("Player event market not availabel for %s", player_info)
                        continue
                    
                    if int(market_template_result_df["wrMarketTypeCategoryId"].values[0])==PLAYER:
                    
                        predicted_value = self.run_player_prediction(player_info, players_runs)
                    
                    elif int(market_template_result_df["wrMarketTypeCategoryId"].values[0])==PLAYERBOUNDARIES:
                        predicted_value = self.run_player_boundaries_prediction(player_info, players_runs)
                        
                    
                    if not predicted_value.get("status"):                        
                        LOG.error("Player score is not able to predict. Probably the player would have got out.!")
                        
                        wr_data = self.get_socket_data(event_market_wrId)

                        if not wr_data.get("status"):
                            LOG.error("Get Socket data is not succesfull %s", wr_data)
                        
                        socket_data.append(wr_data.get("data"))
                        continue

                    predicted_value = round(predicted_value.get("data"))
                    
                    

                    self.update_event_market_ods(player_event_market_details["wrID"].values[0], predicted_value)

                    over_value,under_value=self.get_over_under_rates(predicted_value,player_event_market_details["wrMargin"].values[0])
                    

                    

                    wrBat_IsPlay = player_info.get("wrBat_IsPlay")

                    after_over_close_market_status = self.check_after_over_close_market(auto_close_type, auto_close_over)
                    
                    if (after_over_close_market_status)  or (self.total_wicket >= after_wicket_not_create):
                        auto_close_over_or_wicket_status = True
                        self.update_player_market_status(wrBatterId, CLOSE,int(market_template_result_df["wrMarketTypeCategoryId"].values[0]))
                        wr_data = self.get_socket_data(event_market_wrId)

                        if not wr_data.get("status"):
                            LOG.error("Get Socket data is not succesfull %s", wr_data)
                        
                        socket_data.append(wr_data.get("data"))
                        continue
                    
                    player_runner_details = self.get_runner_by_event_market_wrID(event_market_wrId)
                    print("player runner details are = ", player_runner_details)
                    if auto_close_over_or_wicket_status:
                        wr_data = self.get_socket_data(event_market_wrId)

                        if not wr_data.get("status"):
                            LOG.error("Get Socket data is not succesfull %s", wr_data)
                        
                        socket_data.append(wr_data.get("data"))
                        continue

                    if not player_runner_details.get("status"):
                        
                        player_runner_details = self.create_player_runner_record(player_event_market_details, predicted_value,over_value,under_value, wrBat_IsPlay, wrAutoOpen,  player_event_market_details["wrLineType"].values[0], player_event_market_details["wrDefaultBackSize"].values[0], player_event_market_details["wrDefaultLaySize"].values[0])

                        if not player_runner_details.get("status"):
                            LOG.error("Player runner is not able to create")
                            continue
                    else:
                        
                        player_runner_details = player_runner_details.get("data")                                  

                        self.update_player_runner_record(event_market_wrId, predicted_value,over_value,under_value ,wrBat_IsPlay, wrAutoOpen, player_event_market_details["wrLineType"].values[0], player_event_market_details["wrRateDiff"].values[0])                    
                        
                        if wrAutoOpen <= self.current_ball:
                            status = OPEN 
                            
                            self.update_player_market_status(wrBatterId, OPEN, int(market_template_result_df["wrMarketTypeCategoryId"].values[0]), player_event_market_details["wrDefaultIsSendData"].values[0])

                    wr_data = self.get_socket_data(event_market_wrId)

                    if not wr_data.get("status"):
                        LOG.error("Get Socket data is not succesfull %s", wr_data)                   
                    socket_data.append(wr_data.get("data"))

                    status = True
                    #player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

                    #player_event_markets.loc[player_event_markets['wrID'] == player_event_market_details["wrID"].values[0]] = player_event_market_details.values[0]
                    #player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)
                    
                except Exception as err:
                    LOG.error("Error found in for loop run_player_prediction : %s %s", player_info, err) 
                    traceback.print_exc()
                    status = False
            if socket_data:
                
                return {"status": status, "msg": "", "data":socket_data}

        except Exception as err:
            LOG.error("Error found in run_player_prediction : %s", err) 
            traceback.print_exc()
            status = False
        
        return {"status": status, "msg": "", "data":None}
    
    def get_over_under_rates(self,predicted_value,margin):
        new_predicted_value=int(predicted_value)+0.5
         
         
        over_value=margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
        over_value=1/over_value
         
        under_value=margin / (1 + math.exp(+(predicted_value - new_predicted_value)))
        under_value=1/under_value
        
        over_value=round(over_value,2)
        under_value=round(under_value,2)
        
        return over_value,under_value
    
    
    def run(self,player_runs_details):
        try:
            global LD_MARKET_TEMPLATE
            market_template=LD_MARKET_TEMPLATE[f"{self.commentary_id}_{self.commentary_team}_{self.match_type_id}"]
            async def safe_task(func, *args):
                try:
                    # Measure the start time
                    start_time = time.perf_counter()
                    await func(*args)
                    # Measure the end time
                    end_time = time.perf_counter()
                    # Log the execution time for the task
                    duration = end_time - start_time
                    print(f"Execution time for {func.__name__}: {duration:.2f} seconds")
                except Exception as e:
                    # Log the error with the function name and traceback
                    print(f"Error in {func.__name__}: {str(e)}")
                    traceback.print_exc()

            await self.update_player_market_status(commentary_id, match_type_id, OPEN, commentary_team, False)

            tasks = [
                safe_task(self.run_player_market, player_runs_details, market_template[market_template['wrMarketTemplateCategoryId'] == PLAYER].iloc[0]),
                safe_task(self.run_player_market, player_runs_details, market_template[market_template['wrMarketTemplateCategoryId'] == PLAYERBOUNDARIES].iloc[0]),
                safe_task(self.run_fow_market, player_runs_details, market_template[market_template['wrMarketTemplateCategoryId'] == WICKET].iloc[0])
  
                ]

            await asyncio.gather(*tasks)
        except Exception as err:
            print("Error ", err)
            traceback.print_exc()
            return {"status": False, "msg": str(err)}
       
    #def update_player_market_ballstart(self, commentary_id, event_market_ids, status):
        # try:
        #     time_now = datetime.now()
        #     player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))

        #     with get_db_session() as session:
        #         # SQL queries with named placeholders
        #         event_market_query = text('''
        #             UPDATE "tblEventMarkets"
        #             SET "wrStatus" = :status, "wrLastUpdate" = :last_update,
        #             "wrIsSendData"=:is_send_data
        #             WHERE "wrCommentaryId" = :commentary_id
        #               AND "wrStatus" NOT IN (:close, :settled, :cancel, :inactive)
        #               AND "wrMarketTypeCategoryId" IN (:player, :wicket, :player_boundaries)
        #               AND "wrID" = :event_market_id
        #             RETURNING "wrID"
        #         ''')
    
        #         runner_query = text('''
        #             UPDATE "tblMarketRunners"
        #             SET "wrSelectionStatus" = :status, "wrLastUpdate" = :last_update
        #             WHERE "wrEventMarketId" = :event_market_id
        #               AND "wrSelectionStatus" NOT IN (:close, :settled, :cancel, :inactive)
        #         ''')
        #         socket_data=[]
        #         if status in (SUSPEND, OPEN):
        #             for event_market_id in event_market_ids:
        #                 # Update tblEventMarkets
        #                 session.execute(event_market_query, {
        #                     'status': status,
        #                     'last_update': time_now,
        #                     'commentary_id': commentary_id,
        #                     'close': CLOSE,
        #                     'settled': SETTLED,
        #                     'cancel': CANCEL,
        #                     'inactive': INACTIVE,
        #                     'player': PLAYER,
        #                     'wicket': WICKET,
        #                     'player_boundaries':PLAYERBOUNDARIES,
        #                     'event_market_id': event_market_id,
        #                     'is_send_data':False if status==OPEN else True
        #                 })
                        
        #                 # Update tblMarketRunners
        #                 session.execute(runner_query, {
        #                     'status': status,
        #                     'last_update': time_now,
        #                     'event_market_id': event_market_id,
        #                     'close': CLOSE,
        #                     'settled': SETTLED,
        #                     'cancel': CANCEL,
        #                     'inactive': INACTIVE
        #                 })
    
        #                 # Log success
        #                 LOG.info("Update is Success in the player event market %s", event_market_id)
                        
        #                 # Commit after each update
        #                 session.commit()
                        
        #                 filtered_row = player_event_markets.loc[player_event_markets['wrID'] == event_market_id]
        #                 filtered_row.at[filtered_row.index[0], 'wrStatus'] = status
        #                 filtered_row.at[filtered_row.index[0], 'wrIsSendData'] = False if status==OPEN else True
        #                 filtered_row.at[filtered_row.index[0], 'wrLastUpdate'] = time_now
        #                 player_event_markets.loc[player_event_markets['wrID'] == event_market_id] = filtered_row

                        
        #                 # Get socket data and send it
        #                 wr_data = self.get_socket_data(event_market_id)
                        
        #                 if not wr_data.get("status"):
        #                     LOG.error("Get Socket data is not successful %s", wr_data)
        #                 socket_data.append(wr_data.get("data"))
        #             player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)

        #             if socket_data!=[]:
        #                 self.send_data_to_socket(socket_data)
                    
        #     return {"status": True}
    
        # except Exception as err:
        #     LOG.error("Error updating player market: %s", str(err))
        #     traceback.print_exc()
        #    return {"status": False, "msg": str(err)}
        #print("ok")
        #return {"status": True, "msg": ""}

    
    def update_event_market_ods(self,market_id, predicted_value):
        try:
            time_now = datetime.now()

            player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))
            filtered_row = player_event_markets.loc[player_event_markets['wrID'] == market_id]

            if filtered_row.at[filtered_row.index[0], 'wrMinOdds'] == 0 or filtered_row.at[filtered_row.index[0], 'wrMinOdds'] > predicted_value:
                filtered_row.at[filtered_row.index[0], 'wrMinOdds'] = predicted_value
            if filtered_row.at[filtered_row.index[0], 'wrMaxOdds'] == 0 or filtered_row.at[filtered_row.index[0], 'wrMaxOdds'] < predicted_value:
                filtered_row.at[filtered_row.index[0], 'wrMaxOdds'] = predicted_value
            filtered_row.at[filtered_row.index[0], 'wrLastUpdate'] = time_now
            with get_db_session() as session:
                # SQL queries with named placeholders
                event_market_query = text('''
                    UPDATE "tblEventMarkets"
                    SET "wrMinOdds" = :min_odd, "wrMaxOdds" = :max_odd, "wrLastUpdate" = :last_update
                    WHERE "wrID" = :market_id
                ''')
                session.execute(event_market_query, {
                    'min_odd':float(filtered_row.at[filtered_row.index[0], 'wrMinOdds']),
                    'max_odd': float(filtered_row.at[filtered_row.index[0], 'wrMaxOdds']),
                    'market_id': int(market_id),
                    
                    'last_update': time_now
                    
                })
                session.commit()
                
                player_event_markets.loc[player_event_markets['wrID'] == market_id] = filtered_row
                player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)

        except Exception as err:
            LOG.error("Error updating player market: %s", str(err))
                
    
            return {"status": True}
    
        except Exception as err:
            LOG.error("Error updating player market: %s", str(err))
            return {"status": False, "msg": str(err)}
        
    
    def update_player_markets_line(self,players_data):
        try:
            player_event_markets=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'))
            for item in players_data:
                for index,row in player_event_markets.iterrows():
                    if(item["market_type_category_id"]==row["wrMarketTypeCategoryId"] and item["commentary_player_id"]==row["wrPlayerID"] and row["wrStatus"] not in (CLOSE,SETTLED)):
                        row["wrIsAllow"]=item["is_allow"]
                        row["wrIsSendData"]=item["is_senddata"]
                        row["wrIsActive"]=item["is_active"]
                        row["wrData"]=item["data"]
                        row["wrRateDiff"]=item["rate_diff"]
                        row["wrDefaultLaySize"] = item["lay_size"]
                        row["wrDefaultBackSize"] = item["back_size"]
                        player_event_markets.loc[index] = row
            
            player_event_markets.to_csv(os.path.join(DATA_TEMP_BASE_PATH, str(self.commentary_id)+"_"+str(self.match_type_id)+"_"+str(self.current_team_id)+ '_playereventmarkets.csv'), index=False)

            return {"status":True, "msg":""}
        except Exception as err:
            LOG.error("Error updating player markets line: %s", str(err))
            return {"status": False, "msg": str(err)}