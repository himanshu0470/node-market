# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 18:56:08 2024

@author: hurai
"""

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
from score.services.database.base import get_db_session 
from score.settings.base import DATA_TEMP_BASE_PATH
from score.services.score.config import LD_MARKET_TEMPLATE, LD_PLAYER_MARKETS, LD_PLAYERBOUNDARY_MARKETS, LD_FOW_MARKETS, LD_PLAYERBALLSFACED_MARKETS, LD_DEFAULT_VALUES, LD_PARTNERSHIPBOUNDARIES_MARKETS, LD_WICKETLOSTBALLS_MARKETS

from score.services.score.config import CLOSE, OPEN, SUSPEND, NOTCREATED, SETTLED, BALL, OVER, INACTIVE, CANCEL,PLAYER,WICKET,PLAYERBOUNDARIES, PLAYERBOUNDARIES, PLAYERBALLSFACED, PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS, DYNAMICLINE, STATICLINE,sio
import warnings
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import csv
from score.services.score import common
import asyncio
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from multiprocessing import Pool

warnings.filterwarnings('ignore')

LOG = logging.getLogger('app')


def check_existing_player_market(commentary_id,commentary_team,player_id, market_type_category_id):
    with get_db_session() as session:
        exiting_player_event_query = text('select * from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrTeamID"=:current_team_id and "wrPlayerID"=:player_id and "wrMarketTypeCategoryId"=:market_type_category_id and "wrStatus" not in (:CANCEL, :SETTLE)')
        players_event_details = pd.read_sql_query(exiting_player_event_query,session.bind,params={"commentary_id":commentary_id, "current_team_id":commentary_team, "player_id":player_id, "market_type_category_id":market_type_category_id,"CANCEL":CANCEL, "SETTLE":SETTLED})
        
    
    if players_event_details is not None and len(players_event_details) > 0:
        return players_event_details.iloc[-1]
        
    else:
        return None

def create_player_event_market(commentary_id,commentary_team,match_type_id, player_info, market_template_result, status, batsman_avg,event_id): 
    player_id = player_info["wrCommentaryPlayerId"]  
    player_market=check_existing_player_market(commentary_id,commentary_team,player_id, PLAYER)
    if player_market is not None and len(player_market)>0:
        return player_market
    else:
        global LD_PLAYER_MARKETS
        key = f"{commentary_id}_{commentary_team}"
        if key not in LD_PLAYER_MARKETS:
            column_names=common.get_column_names("tblEventMarkets")
    
            player_markets=pd.DataFrame(columns=column_names)
            player_markets = player_markets.assign(
                player_score=0,
                suspend_over=0,
                close_over=0
                
            )
        else:
            player_markets=LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]
        with get_db_session() as session:
    
            LOG.info("player_info %s", player_info)
            player_market_name = market_template_result["wrTemplateName"].format(player=player_info['wrPlayerName'])
             
            time_now = datetime.now()
            market_template_result["wrPredefinedValue"] =batsman_avg
            current_innings = common.get_current_innings(commentary_id)
            if status == OPEN:            
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff,:predefined_value) RETURNING "wrID"')
                #result = session.execute(new_player_event_query, { 'commentary_id':  commentary_id, 'event_id':  event_id, 'team_id':  commentary_team, 'market_name': player_market_name, 'margin': market_template_result["wrMargin"], 'status': status, 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': 0, 'is_player': True, 'player_id': player_id, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': market_template_result['wrAutoOpenType'], 'auto_open': market_template_result['wrAutoOpen'], 'auto_close_type': market_template_result['wrAutoCloseType'], 'before_auto_close': market_template_result['wrBeforeAutoClose'], 'auto_suspend_type': market_template_result['wrAutoSuspendType'], 'before_auto_suspend': market_template_result['wrBeforeAutoSuspend'], 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': market_template_result['wrAutoResultType'], 'auto_result_after_ball': market_template_result['wrAutoResultafterBall'], 'after_wicket_auto_suspend': market_template_result['wrAfterWicketAutoSuspend'], 'after_wicket_not_created': market_template_result['wrAfterWicketNotCreated'], 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': True, 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': market_template_result["wrMarketTypeCategoryId"],"market_type_id":market_template_result["wrMarketTypeId"],"market_template_id":market_template_result["wrID"] ,"create_type":market_template_result["wrCreateType"],"template_type":market_template_result["wrTemplateType"],"delay":market_template_result["wrDelay"], 'create':market_template_result["wrCreate"], 'create_ref_id':market_template_result["wrCreateRefId"],'open_ref_id':market_template_result["wrOpenRefId"],'action_type':market_template_result["wrActionType"], 'open_odd':player_info["wrBatsmanAverage"], 'min_odd':player_info["wrBatsmanAverage"], 'max_odd':player_info["wrBatsmanAverage"], 'line_type':market_template_result["wrLineType"], 'default_back_size':market_template_result["wrDefaultBackSize"], 'default_lay_size':market_template_result["wrDefaultLaySize"], 'default_is_send_data':market_template_result["wrDefaultIsSendData"], 'rate_diff':market_template_result["wrRateDiff"], "predefined_value":market_template_result["wrPredefinedValue"]})
                result = session.execute(new_player_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(player_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': int(player_id), 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'template_type': int(market_template_result["wrTemplateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': float(player_info["wrBatsmanAverage"]), 'min_odd': float(player_info["wrBatsmanAverage"]), 'max_odd': float(player_info["wrBatsmanAverage"]), 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]) })
            else:
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff, :predefined_value) RETURNING "wrID"')
                #result = session.execute(new_player_event_query, { 'commentary_id':  commentary_id, 'event_id':  event_id, 'team_id':  commentary_team, 'market_name': player_market_name, 'margin': market_template_result["wrMargin"], 'status': status, 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': market_template_result['wrTemplateType'], 'is_over': market_template_result['wrIsOver'], 'over': 0, 'is_player': True, 'player_id': player_id, 'is_auto_cancel':bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': market_template_result['wrAutoOpenType'], 'auto_open': market_template_result['wrAutoOpen'], 'auto_close_type': market_template_result['wrAutoCloseType'], 'before_auto_close': market_template_result['wrBeforeAutoClose'], 'auto_suspend_type': market_template_result['wrAutoSuspendType'], 'before_auto_suspend': market_template_result['wrBeforeAutoSuspend'], 'is_ball_start':bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': market_template_result['wrAutoResultType'], 'auto_result_after_ball': market_template_result['wrAutoResultafterBall'], 'after_wicket_auto_suspend': market_template_result['wrAfterWicketAutoSuspend'], 'after_wicket_not_created': market_template_result['wrAfterWicketNotCreated'], 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': True, 'last_update': time_now, 'market_type_category_id': market_template_result["wrMarketTypeCategoryId"] ,"market_type_id":market_template_result["wrMarketTypeId"],"market_template_id":market_template_result["wrID"],"create_type":market_template_result["wrCreateType"],"delay":market_template_result["wrDelay"], 'create':market_template_result["wrCreate"], 'create_ref_id':market_template_result["wrCreateRefId"],'open_ref_id':market_template_result["wrOpenRefId"],'action_type':market_template_result["wrActionType"], 'open_odd':player_info["wrBatsmanAverage"], 'min_odd':player_info["wrBatsmanAverage"], 'max_odd':player_info["wrBatsmanAverage"],'line_type':market_template_result["wrLineType"], 'default_back_size':market_template_result["wrDefaultBackSize"], 'default_lay_size':market_template_result["wrDefaultLaySize"], 'default_is_send_data':market_template_result["wrDefaultIsSendData"], 'rate_diff':market_template_result["wrRateDiff"], "predefined_value":market_template_result["wrPredefinedValue"] })
                result = session.execute(new_player_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings), 'market_name': str(player_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': int(market_template_result['wrTemplateType']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': int(player_id), 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': float(player_info["wrBatsmanAverage"]), 'min_odd': float(player_info["wrBatsmanAverage"]), 'max_odd': float(player_info["wrBatsmanAverage"]), 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]) })
            event_market_id = result.fetchone()[0]
    
            LOG.info("Insert Success in the player event market %s", event_market_id)
            session.commit()
            row = { "wrID":event_market_id, "wrCommentaryId":  commentary_id, "wrEventRefID":  event_id, "wrTeamID":  commentary_team,"wrInningsID":int(current_innings) ,"wrMarketName": player_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": bool(market_template_result['wrIsOver']), "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": player_id, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": bool(market_template_result['wrIsDefaultBetAllowed']), "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": player_info["wrBatsmanAverage"], "wrMinOdds": player_info["wrBatsmanAverage"], "wrMaxOdds": player_info["wrBatsmanAverage"], "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"],"wrPredefinedValue":market_template_result["wrPredefinedValue"],  "player_score":0,"suspend_over":0,"close_over":0 }
            df=pd.DataFrame([row])
            player_markets = pd.concat([player_markets, df], ignore_index=True)
            player_markets.fillna(0,inplace=True)
            #LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]=player_markets
            
            
            rateDiff=market_template_result["wrRateDiff"]
            
            wrRunner = player_market_name
            selection_id = f"{event_market_id}01"
            update_time = datetime.now()
            #if(lineType==STATICLINE):
            #    wrBackPrice=predicted_player_score
            #    wrLayPrice=predicted_player_score
            #elif(lineType==DYNAMICLINE):
            wrBackPrice = round(batsman_avg) + int(rateDiff)
            wrLayPrice=round(batsman_avg)
            wrBackSize=int(market_template_result["wrDefaultBackSize"])
            wrLaySize=int(market_template_result["wrDefaultLaySize"])
            margin=1+(float(row["wrMargin"])/100)
            over_value=margin / (1 + math.exp(-(batsman_avg - int(batsman_avg)+0.5)))
            over_value=1/over_value
             
            under_value=margin / (1 + math.exp(+(batsman_avg - int(batsman_avg)+0.5)))
            under_value=1/under_value
            
            
        
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
                    'event_market_wrId': event_market_id,
                    'wrRunner': wrRunner,
                    'predicted_player_score': batsman_avg,
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
            return player_markets.iloc[-1]

def create_player_boundary_event_market(commentary_id,commentary_team,match_type_id, player_info, market_template_result, status, boundary,event_id): 
    player_id = player_info["wrCommentaryPlayerId"]  
    player_market=check_existing_player_market(commentary_id,commentary_team,player_id, PLAYERBOUNDARIES)
    if player_market is not None:
        return player_market
    else:
        global LD_PLAYERBOUNDARY_MARKETS
        key = f"{commentary_id}_{commentary_team}"
        if key not in LD_PLAYERBOUNDARY_MARKETS:
            column_names=common.get_column_names("tblEventMarkets")
    
            player_markets=pd.DataFrame(columns=column_names)
            player_markets = player_markets.assign(
                player_score=0,
                suspend_over=0,
                close_over=0
                
            )
        else:
            player_markets=LD_PLAYERBOUNDARY_MARKETS[f"{commentary_id}_{commentary_team}"]
        with get_db_session() as session:
    
            LOG.info("player_info %s", player_info)
            player_market_name = market_template_result["wrTemplateName"].format(player=player_info['wrPlayerName'])
             
            time_now = datetime.now()
            market_template_result["wrPredefinedValue"] =boundary
            current_innings = common.get_current_innings(commentary_id)
            if status == OPEN:            
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff,:predefined_value) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(player_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': int(player_id), 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'template_type': int(market_template_result["wrTemplateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': float(player_info["wrBoundary"]), 'min_odd': float(player_info["wrBoundary"]), 'max_odd': float(player_info["wrBoundary"]), 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]) })
            else:
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff, :predefined_value) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(player_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': int(market_template_result['wrTemplateType']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': int(player_id), 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': float(player_info["wrBoundary"]), 'min_odd': float(player_info["wrBoundary"]), 'max_odd': float(player_info["wrBoundary"]), 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]) })
            event_market_id = result.fetchone()[0]
    
            LOG.info("Insert Success in the player event market %s", event_market_id)
            session.commit()
            row = { "wrID":event_market_id, "wrCommentaryId":  commentary_id, "wrEventRefID":  event_id, "wrTeamID":  commentary_team, "wrInningsID":current_innings,"wrMarketName": player_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": bool(market_template_result['wrIsOver']), "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": player_id, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": bool(market_template_result['wrIsDefaultBetAllowed']), "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": player_info["wrBatsmanAverage"], "wrMinOdds": player_info["wrBatsmanAverage"], "wrMaxOdds": player_info["wrBatsmanAverage"], "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"],"wrPredefinedValue":market_template_result["wrPredefinedValue"], "player_score":0,"suspend_over":0,"close_over":0 }
            df=pd.DataFrame([row])
            player_markets = pd.concat([player_markets, df], ignore_index=True)
            player_markets.fillna(0,inplace=True)
            #LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]=player_markets
            
            
            rateDiff=market_template_result["wrRateDiff"]
            
            wrRunner = player_market_name
            selection_id = f"{event_market_id}01"
            update_time = datetime.now()
            #if(lineType==STATICLINE):
            #    wrBackPrice=predicted_player_score
            #    wrLayPrice=predicted_player_score
            #elif(lineType==DYNAMICLINE):
            wrBackPrice = round(boundary) + int(rateDiff)
            wrLayPrice=round(boundary)
            wrBackSize=int(market_template_result["wrDefaultBackSize"])
            wrLaySize=int(market_template_result["wrDefaultLaySize"])
            margin=1+(float(row["wrMargin"])/100)
            over_value=margin / (1 + math.exp(-(boundary - int(boundary)+0.5)))
            over_value=1/over_value
             
            under_value=margin / (1 + math.exp(+(boundary - int(boundary)+0.5)))
            under_value=1/under_value
            
            
        
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
                    'event_market_wrId': event_market_id,
                    'wrRunner': wrRunner,
                    'predicted_player_score': boundary,
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
                LOG.info("Insert Success in the player boundary event runner market %s", runner_id)
                session.commit()
            
            return player_markets.iloc[-1]

def create_player_balls_faced_event_market(commentary_id,commentary_team,match_type_id, player_info, market_template_result, status, boundary,event_id): 
    player_id = player_info["wrCommentaryPlayerId"]  
    player_market=check_existing_player_market(commentary_id,commentary_team,player_id, PLAYERBALLSFACED)
    if player_market is not None:
        return player_market
    else:
        global LD_PLAYERBALLSFACED_MARKETS
        key = f"{commentary_id}_{commentary_team}"
        if key not in LD_PLAYERBALLSFACED_MARKETS:
            column_names=common.get_column_names("tblEventMarkets")
    
            player_markets=pd.DataFrame(columns=column_names)
            player_markets = player_markets.assign(
                player_score=0,
                suspend_over=0,
                close_over=0
                
            )
        else:
            player_markets=LD_PLAYERBALLSFACED_MARKETS[f"{commentary_id}_{commentary_team}"]
        with get_db_session() as session:
    
            LOG.info("player_info %s", player_info)
            player_market_name = market_template_result["wrTemplateName"].format(player=player_info['wrPlayerName'])
             
            time_now = datetime.now()
            market_template_result["wrPredefinedValue"] =boundary
            current_innings = common.get_current_innings(commentary_id)
            if status == OPEN:            
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff,:predefined_value) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team),'innings_id':int(current_innings), 'market_name': str(player_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': int(player_id), 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'template_type': int(market_template_result["wrTemplateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': float(player_info["wrBoundary"]), 'min_odd': float(player_info["wrBoundary"]), 'max_odd': float(player_info["wrBoundary"]), 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]) })
            else:
                new_player_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff, :predefined_value) RETURNING "wrID"')
                result = session.execute(new_player_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(player_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': int(market_template_result['wrTemplateType']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': int(player_id), 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': float(player_info["wrBoundary"]), 'min_odd': float(player_info["wrBoundary"]), 'max_odd': float(player_info["wrBoundary"]), 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]) })
            event_market_id = result.fetchone()[0]
    
            LOG.info("Insert Success in the player event market %s", event_market_id)
            session.commit()
            row = { "wrID":event_market_id, "wrCommentaryId":  commentary_id, "wrEventRefID":  event_id, "wrTeamID":  commentary_team, "wrInningsID":current_innings,"wrMarketName": player_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": bool(market_template_result['wrIsOver']), "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": player_id, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": bool(market_template_result['wrIsDefaultBetAllowed']), "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": player_info["wrBatsmanAverage"], "wrMinOdds": player_info["wrBatsmanAverage"], "wrMaxOdds": player_info["wrBatsmanAverage"], "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"],"wrPredefinedValue":market_template_result["wrPredefinedValue"],  "player_score":0, "suspend_over":0,"close_over":0 }
            df=pd.DataFrame([row])
            player_markets = pd.concat([player_markets, df], ignore_index=True)
            player_markets.fillna(0,inplace=True)
            #LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]=player_markets
            
            
            rateDiff=market_template_result["wrRateDiff"]
            
            wrRunner = player_market_name
            selection_id = f"{event_market_id}01"
            update_time = datetime.now()
            #if(lineType==STATICLINE):
            #    wrBackPrice=predicted_player_score
            #    wrLayPrice=predicted_player_score
            #elif(lineType==DYNAMICLINE):
            wrBackPrice = round(boundary) + int(rateDiff)
            wrLayPrice=round(boundary)
            wrBackSize=int(market_template_result["wrDefaultBackSize"])
            wrLaySize=int(market_template_result["wrDefaultLaySize"])
            margin=1+(float(row["wrMargin"])/100)
            over_value=margin / (1 + math.exp(-(boundary - int(boundary)+0.5)))
            over_value=1/over_value
             
            under_value=margin / (1 + math.exp(+(boundary - int(boundary)+0.5)))
            under_value=1/under_value
            
            
        
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
                    'event_market_wrId': event_market_id,
                    'wrRunner': wrRunner,
                    'predicted_player_score': boundary,
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
                LOG.info("Insert Success in the player boundary event runner market %s", runner_id)
                session.commit()
            
            return player_markets.iloc[-1]



def get_player_rules_by_playerId(wrPlayerId, players_runs):
    for _, player in players_runs.iterrows():
        if player["wrCommentaryPlayerId"] == wrPlayerId:
            return player
    return pd.DataFrame()


def get_wicket_player_details(commentary_id,current_team_id):
    player_runner_query = text('''
        SELECT w."wrBatterId" as wrBatterId, w."wrPlayerRun" as wrPlayerRun, w."wrTeamScore" as wrTeamScore,  
               w."wrWicketCount" as wrWicketCount, w."wrCommentaryBallByBallId" as wrCommentaryBallByBallId, 
               b."wrOverCount" as wrOverCount
        FROM "tblCommentaryWickets" w
        JOIN "tblCommentaryBallByBalls" b
        
        ON w."wrCommentaryBallByBallId" = b."wrCommentaryBallByBallId"
        
        WHERE w."wrCommentaryId" = :commentaryId and b."wrTeamId"=:teamId AND w."wrIsDeletedStatus"=False
        ORDER BY b."wrOverCount" ASC  
    ''')
    player_wicket_df = pd.DataFrame()
    try:
        with get_db_session() as session:
            result = session.execute(player_runner_query, {'commentaryId': commentary_id, 'teamId':current_team_id})
            player_wicket_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
        return player_wicket_df

    except Exception as err:
        LOG.error("Error fetching wicket player details: %s", str(err))
        return player_wicket_df

    return player_wicket_df



def get_players_details(commentary_id,current_team_id,player_id=None):
    """
    Get the current players who is playing
    """
    with get_db_session() as session:
        if not player_id:
            select_players_query=text('select * from "tblCommentaryPlayers" where "wrCommentaryId"=:commentary_id and "wrTeamId"=:team_id and "wrBat_IsPlay"=:is_play')
            players_details = pd.read_sql_query(select_players_query,session.bind,params={"commentary_id": commentary_id, "team_id": current_team_id, "is_play":True})
        else:            
            player_id_str = player_id
            select_players_query=text('select * from "tblCommentaryPlayers" where "wrCommentaryId"=:commentary_id and "wrTeamId"=:team_id and "wrCommentaryPlayerId" = ANY(:player_id_str)')
            players_details = pd.read_sql_query(select_players_query,session.bind,params={"commentary_id": commentary_id, "team_id": current_team_id, "player_id_str":player_id_str})

    if len(players_details) == 0:
        return {"status": False, "msg": "Player details is not available on the CommentaryPlayers"}

    return {"status": True, "data": players_details}


def get_player_markets(commentary_id,commentary_team,market_type_category_id):
    with get_db_session() as session:
        exiting_player_event_query = text('select * from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrTeamID"=:current_team_id and "wrMarketTypeCategoryId"=:market_type_category_id and "wrStatus" not in (:CANCEL, :SETTLE)')
        players_event_details = pd.read_sql_query(exiting_player_event_query,session.bind,params={"commentary_id":commentary_id, "current_team_id":commentary_team,  "market_type_category_id":market_type_category_id,"CANCEL":CANCEL, "SETTLE":SETTLED})
        return players_event_details

def process_player_markets(current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_details, market_template, ball_by_ball_id):
    print("starting time for player is", time.time())
    global LD_PLAYER_MARKETS, LD_DEFAULT_VALUES
    
    stime=time.time()
    key = f"{commentary_id}_{commentary_team}"
    
    if key in LD_PLAYER_MARKETS:
        print("player local data in start is ",LD_PLAYER_MARKETS[key])
    all_players_event_markets = pd.DataFrame()
    runner_data = []
    event_data = []
    market_datalog = []
    socket_data = []
    players_runs = get_players_details(commentary_id, commentary_team)

    if not players_runs.get("status"):
        return {"status": False, "msg": players_runs.get("msg"), "data": None}

    players_runs = players_runs.get("data")
    is_empty = None
    wickets=pd.DataFrame()
    if player_details:
        with get_db_session() as session:
            query = text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets" w '
                         'INNER JOIN "tblCommentaryBallByBalls" b '
                         'ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" '
                         'WHERE w."wrCommentaryId"=:commentary_id AND w."wrTeamId"=:team_id ORDER BY w."wrCommentaryBallByBallId" DESC')
            wickets = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team})
           

    for player_info in player_details:
        player_id = player_info["player_id"]

        if key in LD_PLAYER_MARKETS:
            event_markets = LD_PLAYER_MARKETS[key]
            check_markets_wicket = event_markets
            players_event_market = event_markets[
                (event_markets['wrCommentaryId'] == commentary_id) &
                (event_markets['wrTeamID'] == commentary_team) &
                (event_markets['wrPlayerID'] == player_id) &
                (event_markets['wrMarketTypeCategoryId'] == PLAYER) &
                (~event_markets['wrStatus'].isin([CANCEL]))
            ]
            is_empty = False

            if not players_event_market.empty:
                all_players_event_markets = event_markets
                continue
        else:
            is_empty = True

        market_template_row = market_template.iloc[0]
        player_wicket_df = get_wicket_player_details(commentary_id, commentary_team)

        create_type_id = market_template_row["wrCreateType"]
        if create_type_id == 1:
            if wickets.empty:
                autoCreateBall = market_template_row["wrCreate"]
                if current_ball < float(autoCreateBall):
                    return {"status": False, "msg": f"AutoStart ball is yet to start. Current ball {current_ball} is behind auto-create ball {autoCreateBall}", "data": None}
            else:
                if key in LD_PLAYER_MARKETS:
                    last_wicket = check_markets_wicket["suspend_over"].max()
                    if last_wicket > 0:
                        last_wicket_balls = common.overs_to_balls(last_wicket, match_type_id)
                        create_balls = last_wicket_balls + market_template_row["wrCreate"] * 10
                        autoCreateBall = int(create_balls / 6) + (create_balls % 6) / 10
                        if current_ball < float(autoCreateBall):
                            continue

        total_wicket = len(player_wicket_df)
        players_runs_row = players_runs[players_runs["wrCommentaryPlayerId"] == player_id]
        if players_runs_row.empty:
            continue

        players_runs_dict = players_runs_row.to_dict("records")[0]
        player_info.update(players_runs_dict)

        status = OPEN if player_info.get("wrBat_IsPlay") else INACTIVE
        autoOpenStatus = float(market_template_row["wrAutoOpen"])
        status = OPEN if autoOpenStatus <= current_ball else INACTIVE

        player_rules = get_player_rules_by_playerId(player_id, players_runs)
        if total_wicket >= int(market_template_row["wrAfterWicketNotCreated"]) or current_ball >= float(market_template_row["wrBeforeAutoClose"]):
            continue

        default_data = LD_DEFAULT_VALUES[key]
        predefined_value = int(default_data["default_player_runs"].iloc[0]) if player_rules["wrBatsmanAverage"] == 0 else player_rules["wrBatsmanAverage"]
        if predefined_value==0:
            predefined_value= 20 
        player_event_market = create_player_event_market(
            commentary_id, commentary_team, match_type_id, player_info, market_template_row, status, predefined_value, event_id
        )
        player_event_market = pd.DataFrame([player_event_market])

        if is_empty:
            all_players_event_markets = player_event_market.assign(player_score=0, suspend_over=0, close_over=0)
        else:
            all_players_event_markets = pd.concat([event_markets, player_event_market], ignore_index=True)

        LD_PLAYER_MARKETS[key] = all_players_event_markets

    if all_players_event_markets.empty and key in LD_PLAYER_MARKETS:
        all_players_event_markets = LD_PLAYER_MARKETS[key]


    for index, row in all_players_event_markets.iterrows():
        if row["wrStatus"] != SETTLED:
            suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
            close_balls = common.overs_to_balls(row["close_over"], match_type_id)
            current_over_balls = common.overs_to_balls(current_ball, match_type_id)

            market_runner = []
            is_update = False
            is_SendData = True
            predicted_value = fixed_line_value=over_value=under_value=back_price=lay_price=0
            lay_size = back_size=100
            selection_status = NOTCREATED
            player_info = next((player for player in player_details if player['player_id'] == row["wrPlayerID"]), None)

            if player_info is None and ((row["wrStatus"] not in (SUSPEND, CLOSE)) or (row["wrStatus"] == SUSPEND and suspend_balls == 0)):
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = SUSPEND

            elif player_info is not None and row["wrStatus"] == SUSPEND and player_info["player_name"] in row["wrMarketName"]:
                row["wrStatus"] = OPEN
                row["suspend_over"] = 0
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = OPEN

            elif current_over_balls >= suspend_balls + 2 and row["wrStatus"] == SUSPEND and suspend_balls > 0:
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                selection_status = CLOSE
                row["wrCloseTime"] = datetime.now().isoformat()

            elif current_ball >= row["wrAutoOpen"] and row["wrStatus"] == INACTIVE:
                row["wrStatus"] = OPEN
                selection_status = OPEN

            elif player_info is not None and player_info["player_name"] not in row["wrMarketName"]:
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)

            if current_ball >= float(row["wrBeforeAutoClose"]):
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                row["wrCloseTime"] = datetime.now().isoformat()

            if row["wrIsPlayer"] and row["wrStatus"] != SETTLED:
                margin = 1 + (float(row["wrMargin"]) / 100)
                if player_info is None:
                    wrdata = json.loads(row["wrData"])
                    runner = wrdata["runner"][0]
                    predicted_value = runner["line"]
                else:
                    predefined_batsman_avg = row["wrPredefinedValue"]
                    if predefined_batsman_avg == 0:
                        default_data = LD_DEFAULT_VALUES[key]
                        predicted_value = int(default_data["default_player_runs"].iloc[0]) + player_info["batRun"]
                    else:
                        predicted_value = predefined_batsman_avg + player_info["batRun"]
                
                predicted_value=common.round_down(predicted_value)
                if player_info is not None:
                    row["player_score"] = player_info["batRun"]

                new_predicted_value = int(predicted_value) + 0.5

                over_value = margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
                over_value = 1 / over_value

                under_value = margin / (1 + math.exp(predicted_value - new_predicted_value))
                under_value = 1 / under_value

                over_value = round(over_value, 2)
                under_value = round(under_value, 2)
                lay_price = common.custom_round(predicted_value)
                back_price = lay_price + row["wrRateDiff"]
                fixed_line_value = round(predicted_value, 1)
                lay_size = row["wrDefaultLaySize"]
                back_size = row["wrDefaultBackSize"]

                if row["wrMinOdds"] == 0 or row["wrMinOdds"] > back_price:
                    row["wrMinOdds"] = back_price
                if row["wrMaxOdds"] == 0 or row["wrMaxOdds"] < back_price:
                    row["wrMaxOdds"] = back_price
                if row["wrOpenOdds"] == 0:
                    row["wrOpenOdds"] = back_price

                if row["wrStatus"] == CLOSE:
                    selection_status = CLOSE
                elif row["wrStatus"] == OPEN:
                    selection_status = OPEN
                    is_SendData = row["wrDefaultIsSendData"]

            if current_over_balls >= close_balls + row["wrAutoResultafterBall"] and row["wrStatus"] == CLOSE or (player_info is None and current_ball >= float(row["wrBeforeAutoClose"]) and row["wrStatus"] == CLOSE):
                row["wrResult"] = common.markets_result_settlement(row, commentary_id, commentary_team)
                row["wrStatus"] = SETTLED
                selection_status = SETTLED
                row["wrSettledTime"] = datetime.now().isoformat()

            market_runner_row = common.fetch_runner_data(selection_status, row)
            if len(market_runner_row) > 0:
                runner_data_dict = {
                    "wrRunner": market_runner_row["wrRunner"].iloc[0],
                    "wrLine": fixed_line_value,
                    "wrOverRate": over_value,
                    "wrUnderRate": under_value,
                    "wrBackPrice": back_price,
                    "wrLayPrice": lay_price,
                    "wrBackSize": back_size,
                    "wrLaySize": lay_size,
                    "wrSelectionStatus": int(row["wrStatus"]),
                    "wrEventMarketId": int(row["wrID"]),
                    "wrRunnerId": int(market_runner_row["wrRunnerId"].iloc[0])
                }
                row["wrIsSendData"] = is_SendData
                runner_data.append(runner_data_dict)
                market_runner_row = pd.DataFrame([runner_data_dict])
                market_runner = common.convert_runner_data(market_runner_row)
                json_event_market = common.convert_event_market(row, market_runner)
                json_socket_data = common.convert_socket_data(row, market_runner, ball_by_ball_id)

                event_data.append(common.parse_event_data(row, json_event_market, is_SendData))

                market_datalog.append({
                    "wrCommentaryId": commentary_id,
                    "wrEventMarketId": row["wrID"],
                    "wrData": json_event_market,
                    "wrUpdateType": 1,
                    "wrIsSendData": is_SendData
                })

                row["wrData"] = json_event_market
                socket_data.append(json_socket_data)
            all_players_event_markets.loc[index] = row
            LD_PLAYER_MARKETS[key] = all_players_event_markets

    if runner_data and market_datalog and event_data:
        runner_json = json.dumps(runner_data)
        market_datalog_json = json.dumps(market_datalog)
        eventmarket_json = json.dumps(event_data)
        common.send_data_to_socket_async(commentary_id, socket_data)
        common.update_database_async(runner_json, market_datalog_json, eventmarket_json)
    print("Execution time for player runs is", time.time() - stime)



def process_player_boundary_markets(current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_details, market_template, ball_by_ball_id):
    print("starting time for player boundary is", time.time())

    global LD_PLAYERBOUNDARY_MARKETS, LD_DEFAULT_VALUES
    stime=time.time()
    key = f"{commentary_id}_{commentary_team}"
    all_players_event_markets = pd.DataFrame()
    runner_data = []
    event_data = []
    market_datalog = []
    socket_data = []
    players_runs = get_players_details(commentary_id, commentary_team)

    if not players_runs.get("status"):
        return {"status": False, "msg": players_runs.get("msg"), "data": None}

    players_runs = players_runs.get("data")
    is_empty = False
    check_markets_wicket = pd.DataFrame()
    wickets=pd.DataFrame()
    if player_details:
        with get_db_session() as session:
            query = text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets" w '
                         'INNER JOIN "tblCommentaryBallByBalls" b '
                         'ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" '
                         'WHERE w."wrCommentaryId"=:commentary_id AND w."wrTeamId"=:team_id ORDER BY w."wrCommentaryBallByBallId" DESC')
            wickets = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team})
            
    for player_info in player_details:
        player_id = player_info["player_id"]

        if key in LD_PLAYERBOUNDARY_MARKETS:
            event_markets = LD_PLAYERBOUNDARY_MARKETS[key]
            check_markets_wicket = event_markets
            players_event_market = event_markets[
                (event_markets['wrCommentaryId'] == commentary_id) &
                (event_markets['wrTeamID'] == commentary_team) &
                (event_markets['wrPlayerID'] == player_id) &
                (event_markets['wrMarketTypeCategoryId'] == PLAYERBOUNDARIES) &
                (~event_markets['wrStatus'].isin([CANCEL]))
            ]
            is_empty = False

            if not players_event_market.empty:
                all_players_event_markets = event_markets
                continue
        else:
            is_empty = True

        market_template_row = market_template.iloc[0]
        player_wicket_df = get_wicket_player_details(commentary_id, commentary_team)

        create_type_id = market_template_row["wrCreateType"]
        if create_type_id == 1:
           
            if wickets.empty:
                autoCreateBall = market_template_row["wrCreate"]
                if current_ball < float(autoCreateBall):
                    return {"status": False, "msg": f"AutoStart ball is yet to start. Current ball {current_ball} is behind auto-create ball {autoCreateBall}", "data": None}
            else:
                if key in LD_PLAYERBOUNDARY_MARKETS:
                    last_wicket = check_markets_wicket["suspend_over"].max()
                    if last_wicket > 0:
                        last_wicket_balls = common.overs_to_balls(last_wicket, match_type_id)
                        create_balls = last_wicket_balls + market_template_row["wrCreate"] * 10
                        autoCreateBall = int(create_balls / 6) + (create_balls % 6) / 10
                        if current_ball < float(autoCreateBall):
                            continue

        total_wicket = len(player_wicket_df)
        if total_wicket >= int(market_template_row["wrAfterWicketNotCreated"]) or current_ball >= float(market_template_row["wrBeforeAutoClose"]):
            continue

        players_runs_row = players_runs[players_runs["wrCommentaryPlayerId"] == player_id]
        if players_runs_row.empty:
            continue

        players_runs_dict = players_runs_row.to_dict("records")[0]
        player_info.update(players_runs_dict)

        status = OPEN if player_info.get("wrBat_IsPlay") else INACTIVE
        autoOpenStatus = float(market_template_row["wrAutoOpen"])
        status = OPEN if autoOpenStatus <= current_ball else INACTIVE

        player_rules = get_player_rules_by_playerId(player_id, players_runs)
        default_data = LD_DEFAULT_VALUES[key]
        predefined_value = int(default_data["default_player_boundaries"].iloc[0]) if player_rules["wrBoundary"] == 0 else player_rules["wrBoundary"]
        player_event_market = create_player_boundary_event_market(
            commentary_id, commentary_team, match_type_id, player_info, market_template_row, status, predefined_value, event_id
        )

        player_event_market = pd.DataFrame([player_event_market])
        if is_empty:
            all_players_event_markets = player_event_market.assign(player_score=0, suspend_over=0, close_over=0)
        else:
            all_players_event_markets = pd.concat([event_markets, player_event_market], ignore_index=True)

        LD_PLAYERBOUNDARY_MARKETS[key] = all_players_event_markets

    if all_players_event_markets.empty and key in LD_PLAYERBOUNDARY_MARKETS:
        all_players_event_markets = LD_PLAYERBOUNDARY_MARKETS[key]

    for index, row in all_players_event_markets.iterrows():
        if row["wrStatus"] != SETTLED:
            suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
            close_balls = common.overs_to_balls(row["close_over"], match_type_id)
            current_over_balls = common.overs_to_balls(current_ball, match_type_id)

            market_runner = []
            is_update = False
            is_SendData = True
            predicted_value = fixed_line_value=over_value=under_value=back_price=lay_price=0
            lay_size = back_size=100
            selection_status = NOTCREATED
            player_info = next((player for player in player_details if player['player_id'] == row["wrPlayerID"]), None)

            if player_info is None and ((row["wrStatus"] not in (SUSPEND, CLOSE)) or (row["wrStatus"] == SUSPEND and suspend_balls == 0)):
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = SUSPEND

            elif player_info is not None and row["wrStatus"] == SUSPEND and player_info["player_name"] in row["wrMarketName"]:
                row["wrStatus"] = OPEN
                row["suspend_over"] = 0
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = OPEN

            elif current_over_balls >= suspend_balls + 2 and row["wrStatus"] == SUSPEND and suspend_balls > 0:
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                selection_status = CLOSE
                row["wrCloseTime"] = datetime.now().isoformat()

            elif current_ball >= row["wrAutoOpen"] and row["wrStatus"] == INACTIVE:
                row["wrStatus"] = OPEN
                selection_status = OPEN

            elif player_info is not None and player_info["player_name"] not in row["wrMarketName"]:
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)

            if current_ball >= float(row["wrBeforeAutoClose"]):
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                row["wrCloseTime"] = datetime.now().isoformat()

            if row["wrIsPlayer"] and row["wrStatus"] != SETTLED:
                margin = 1 + (float(row["wrMargin"]) / 100)
                if player_info is None:
                    wrdata = json.loads(row["wrData"])
                    runner = wrdata["runner"][0]
                    predicted_value = runner["line"]
                else:
                    predefined_boundaries = row["wrPredefinedValue"]
                    if predefined_boundaries == 0:
                        default_data = LD_DEFAULT_VALUES[key]
                        predicted_value = int(default_data["default_player_boundaries"].iloc[0]) + player_info["current_boundaries"]
                    else:
                        predicted_value = predefined_boundaries + player_info["current_boundaries"]
                predicted_value=common.round_down(predicted_value)
                if player_info is not None:
                    row["player_score"] = player_info["current_boundaries"]

                new_predicted_value = int(predicted_value) + 0.5

                over_value = margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
                over_value = 1 / over_value

                under_value = margin / (1 + math.exp(predicted_value - new_predicted_value))
                under_value = 1 / under_value

                over_value = round(over_value, 2)
                under_value = round(under_value, 2)
                lay_price = common.custom_round(predicted_value)
                back_price = lay_price + row["wrRateDiff"]
                fixed_line_value = round(predicted_value, 1)
                lay_size = row["wrDefaultLaySize"]
                back_size = row["wrDefaultBackSize"]

                # if row["wrMinOdds"] == 0 or row["wrMinOdds"] > back_price:
                #     row["wrMinOdds"] = back_price
                # if row["wrMaxOdds"] == 0 or row["wrMaxOdds"] < back_price:
                #     row["wrMaxOdds"] = back_price
                # if row["wrOpenOdds"] == 0:
                #     row["wrOpenOdds"] = back_price
                row["wrMinOdds"] = min(row["wrMinOdds"] or back_price, back_price)
                row["wrMaxOdds"] = max(row["wrMaxOdds"] or back_price, back_price)
                row["wrOpenOdds"] = row["wrOpenOdds"] or back_price

                if row["wrStatus"] == CLOSE:
                    selection_status = CLOSE
                elif row["wrStatus"] == OPEN:
                    selection_status = OPEN
                    is_SendData = row["wrDefaultIsSendData"]

            if current_over_balls >= close_balls + row["wrAutoResultafterBall"] and row["wrStatus"] == CLOSE or (player_info is None and current_ball >= float(row["wrBeforeAutoClose"]) and row["wrStatus"] == CLOSE):
                row["wrResult"] = common.markets_result_settlement(row, commentary_id, commentary_team)
                row["wrStatus"] = SETTLED
                selection_status = SETTLED
                row["wrSettledTime"] = datetime.now().isoformat()

            market_runner_row = common.fetch_runner_data(selection_status, row)
            if len(market_runner_row) > 0:
                runner_data_dict = {
                    "wrRunner": market_runner_row["wrRunner"].iloc[0],
                    "wrLine": fixed_line_value,
                    "wrOverRate": over_value,
                    "wrUnderRate": under_value,
                    "wrBackPrice": back_price,
                    "wrLayPrice": lay_price,
                    "wrBackSize": back_size,
                    "wrLaySize": lay_size,
                    "wrSelectionStatus": int(row["wrStatus"]),
                    "wrEventMarketId": int(row["wrID"]),
                    "wrRunnerId": int(market_runner_row["wrRunnerId"].iloc[0])
                }
                row["wrIsSendData"] = is_SendData
                runner_data.append(runner_data_dict)
                market_runner_row = pd.DataFrame([runner_data_dict])
                market_runner = common.convert_runner_data(market_runner_row)
                json_event_market = common.convert_event_market(row, market_runner)
                json_socket_data = common.convert_socket_data(row, market_runner, ball_by_ball_id)

                event_data.append(common.parse_event_data(row, json_event_market, is_SendData))

                market_datalog.append({
                    "wrCommentaryId": commentary_id,
                    "wrEventMarketId": row["wrID"],
                    "wrData": json_event_market,
                    "wrUpdateType": 1,
                    "wrIsSendData": is_SendData
                })

                row["wrData"] = json_event_market
                socket_data.append(json_socket_data)

            all_players_event_markets.loc[index] = row
            LD_PLAYERBOUNDARY_MARKETS[key] = all_players_event_markets

    if runner_data and market_datalog and event_data:
        runner_json = json.dumps(runner_data)
        market_datalog_json = json.dumps(market_datalog)
        eventmarket_json = json.dumps(event_data)
        common.send_data_to_socket_async(commentary_id, socket_data)
        common.update_database_async(runner_json, market_datalog_json, eventmarket_json)
    print("Execution time for player boundaries is", time.time() - stime)


def process_player_balls_faced_markets(current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_details, market_template, ball_by_ball_id):
    print("starting time for playerball face is", time.time())

    global LD_PLAYERBALLSFACED_MARKETS, LD_DEFAULT_VALUES
    stime=time.time()
    key = f"{commentary_id}_{commentary_team}"
    all_players_event_markets = pd.DataFrame()
    runner_data = []
    event_data = []
    market_datalog = []
    socket_data = []
    players_runs = get_players_details(commentary_id, commentary_team)

    if not players_runs.get("status"):
        return {"status": False, "msg": players_runs.get("msg"), "data": None}

    players_runs = players_runs.get("data")
    is_empty = False
    check_markets_wicket = pd.DataFrame()
    wickets=pd.DataFrame()
    if player_details:
        with get_db_session() as session:
            query = text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets" w '
                         'INNER JOIN "tblCommentaryBallByBalls" b '
                         'ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" '
                         'WHERE w."wrCommentaryId"=:commentary_id AND w."wrTeamId"=:team_id ORDER BY w."wrCommentaryBallByBallId" DESC')
            wickets = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team})
            
    for player_info in player_details:
        player_id = player_info["player_id"]

        if key in LD_PLAYERBALLSFACED_MARKETS:
            event_markets = LD_PLAYERBALLSFACED_MARKETS[key]
            check_markets_wicket = event_markets
            players_event_market = event_markets[
                (event_markets['wrCommentaryId'] == commentary_id) &
                (event_markets['wrTeamID'] == commentary_team) &
                (event_markets['wrPlayerID'] == player_id) &
                (event_markets['wrMarketTypeCategoryId'] == PLAYERBALLSFACED) &
                (~event_markets['wrStatus'].isin([CANCEL]))
            ]
            is_empty = False

            if not players_event_market.empty:
                all_players_event_markets = event_markets
                continue
        else:
            is_empty = True

        market_template_row = market_template.iloc[0]
        player_wicket_df = get_wicket_player_details(commentary_id, commentary_team)

        create_type_id = market_template_row["wrCreateType"]
        if create_type_id == 1:
            if wickets.empty:
                autoCreateBall = market_template_row["wrCreate"]
                if current_ball < float(autoCreateBall):
                    return {"status": False, "msg": f"AutoStart ball is yet to start. Current ball {current_ball} is behind auto-create ball {autoCreateBall}", "data": None}
            else:
                if key in LD_PLAYERBALLSFACED_MARKETS:
                    last_wicket = check_markets_wicket["suspend_over"].max()
                    if last_wicket > 0:
                        last_wicket_balls = common.overs_to_balls(last_wicket, match_type_id)
                        create_balls = last_wicket_balls + market_template_row["wrCreate"] * 10
                        autoCreateBall = int(create_balls / 6) + (create_balls % 6) / 10
                        if current_ball < float(autoCreateBall):
                            continue

        total_wicket = len(player_wicket_df)
        if total_wicket >= int(market_template_row["wrAfterWicketNotCreated"]) or current_ball >= float(market_template_row["wrBeforeAutoClose"]):
            continue

        players_runs_row = players_runs[players_runs["wrCommentaryPlayerId"] == player_id]
        if players_runs_row.empty:
            continue

        players_runs_dict = players_runs_row.to_dict("records")[0]
        player_info.update(players_runs_dict)

        status = OPEN if player_info.get("wrBat_IsPlay") else INACTIVE
        autoOpenStatus = float(market_template_row["wrAutoOpen"])
        status = OPEN if autoOpenStatus <= current_ball else INACTIVE

        player_rules = get_player_rules_by_playerId(player_id, players_runs)
        default_data = LD_DEFAULT_VALUES[key]
        predefined_value = int(default_data["default_ball_faced"].iloc[0]) if player_rules["wrPlayerBallFaced"] == 0 else player_rules["wrPlayerBallFaced"]
        if predefined_value==0:
            predefined_value= 15
        player_event_market = create_player_balls_faced_event_market(
            commentary_id, commentary_team, match_type_id, player_info, market_template_row, status, predefined_value, event_id
        )

        player_event_market = pd.DataFrame([player_event_market])
        if is_empty:
            all_players_event_markets = player_event_market.assign(player_score=0, suspend_over=0, close_over=0)
        else:
            all_players_event_markets = pd.concat([event_markets, player_event_market], ignore_index=True)

        LD_PLAYERBALLSFACED_MARKETS[key] = all_players_event_markets

    if all_players_event_markets.empty and key in LD_PLAYERBALLSFACED_MARKETS:
        all_players_event_markets = LD_PLAYERBALLSFACED_MARKETS[key]

    for index, row in all_players_event_markets.iterrows():
        if row["wrStatus"] != SETTLED:
            suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
            close_balls = common.overs_to_balls(row["close_over"], match_type_id)
            current_over_balls = common.overs_to_balls(current_ball, match_type_id)

            market_runner = []
            is_update = False
            is_SendData = True
            predicted_value = fixed_line_value=over_value=under_value=back_price=lay_price=0
            lay_size = back_size=100
            selection_status = NOTCREATED
            player_info = next((player for player in player_details if player['player_id'] == row["wrPlayerID"]), None)

            if player_info is None and ((row["wrStatus"] not in (SUSPEND, CLOSE)) or (row["wrStatus"] == SUSPEND and suspend_balls == 0)):
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = SUSPEND

            elif player_info is not None and row["wrStatus"] == SUSPEND and player_info["player_name"] in row["wrMarketName"]:
                row["wrStatus"] = OPEN
                row["suspend_over"] = 0
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = OPEN

            elif current_over_balls >= suspend_balls + 2 and row["wrStatus"] == SUSPEND and suspend_balls > 0:
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                selection_status = CLOSE
                row["wrCloseTime"] = datetime.now().isoformat()

            elif current_ball >= row["wrAutoOpen"] and row["wrStatus"] == INACTIVE:
                row["wrStatus"] = OPEN
                selection_status = OPEN

            elif player_info is not None and player_info["player_name"] not in row["wrMarketName"]:
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)

            if current_ball >= float(row["wrBeforeAutoClose"]):
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                row["wrCloseTime"] = datetime.now().isoformat()

            if row["wrIsPlayer"] and row["wrStatus"] != SETTLED:
                margin = 1 + (float(row["wrMargin"]) / 100)

                if player_info is None:
                    wrdata = json.loads(row["wrData"])
                    runner = wrdata["runner"][0]
                    predicted_value = runner["line"]
                else:
                    predefined_balls_faced = row["wrPredefinedValue"]
                    if predefined_balls_faced == 0:
                        default_data = LD_DEFAULT_VALUES[key]
                        predicted_value = int(default_data["default_ball_faced"].iloc[0]) + player_info["balls_faced"]
                    else:
                        predicted_value = predefined_balls_faced + player_info["balls_faced"]

                if player_info is not None:
                    row["player_score"] = player_info["balls_faced"]
                predicted_value=common.round_down(predicted_value)
                new_predicted_value = int(predicted_value) + 0.5

                over_value = margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
                over_value = 1 / over_value

                under_value = margin / (1 + math.exp(predicted_value - new_predicted_value))
                under_value = 1 / under_value

                over_value = round(over_value, 2)
                under_value = round(under_value, 2)
                lay_price = common.custom_round(predicted_value)
                back_price = lay_price + row["wrRateDiff"]
                fixed_line_value = round(predicted_value, 1)
                lay_size = row["wrDefaultLaySize"]
                back_size = row["wrDefaultBackSize"]

                row["wrMinOdds"] = min(row["wrMinOdds"] or back_price, back_price)
                row["wrMaxOdds"] = max(row["wrMaxOdds"] or back_price, back_price)
                row["wrOpenOdds"] = row["wrOpenOdds"] or back_price

                if row["wrStatus"] == CLOSE:
                    selection_status = CLOSE
                elif row["wrStatus"] == OPEN:
                    selection_status = OPEN
                    is_SendData = row["wrDefaultIsSendData"]

            if current_over_balls >= close_balls + row["wrAutoResultafterBall"] and row["wrStatus"] == CLOSE or (player_info is None and current_ball >= float(row["wrBeforeAutoClose"]) and row["wrStatus"] == CLOSE):
                row["wrResult"] = common.markets_result_settlement(row, commentary_id, commentary_team)
                row["wrStatus"] = SETTLED
                selection_status = SETTLED
                row["wrSettledTime"] = datetime.now().isoformat()

            market_runner_row = common.fetch_runner_data(selection_status, row)
            if len(market_runner_row) > 0:
                runner_data_dict = {
                    "wrRunner": market_runner_row["wrRunner"].iloc[0],
                    "wrLine": fixed_line_value,
                    "wrOverRate": over_value,
                    "wrUnderRate": under_value,
                    "wrBackPrice": back_price,
                    "wrLayPrice": lay_price,
                    "wrBackSize": back_size,
                    "wrLaySize": lay_size,
                    "wrSelectionStatus": int(row["wrStatus"]),
                    "wrEventMarketId": int(row["wrID"]),
                    "wrRunnerId": int(market_runner_row["wrRunnerId"].iloc[0])
                }
                row["wrIsSendData"] = is_SendData
                runner_data.append(runner_data_dict)
                market_runner_row = pd.DataFrame([runner_data_dict])
                market_runner = common.convert_runner_data(market_runner_row)
                json_event_market = common.convert_event_market(row, market_runner)
                json_socket_data = common.convert_socket_data(row, market_runner, ball_by_ball_id)

                event_data.append(common.parse_event_data(row, json_event_market, is_SendData))

                market_datalog.append({
                    "wrCommentaryId": commentary_id,
                    "wrEventMarketId": row["wrID"],
                    "wrData": json_event_market,
                    "wrUpdateType": 1,
                    "wrIsSendData": is_SendData
                })

                row["wrData"] = json_event_market
                socket_data.append(json_socket_data)

            all_players_event_markets.loc[index] = row
            LD_PLAYERBALLSFACED_MARKETS[key] = all_players_event_markets

    if runner_data and market_datalog and event_data:
        runner_json = json.dumps(runner_data)
        market_datalog_json = json.dumps(market_datalog)
        eventmarket_json = json.dumps(event_data)
        common.send_data_to_socket_async(commentary_id, socket_data)
        common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for balls face is", time.time() - stime)





def check_existing_fow_partnership_market(commentary_id,commentary_team,fow_market_name, market_type_category_id):
    with get_db_session() as session:
        exiting_player_event_query = text('select * from "tblEventMarkets" where "wrCommentaryId"=:commentary_id and "wrTeamID"=:current_team_id  and "wrMarketTypeCategoryId"=:market_type_category_id and "wrMarketName"=:market_name and "wrStatus" not in (:CANCEL, :SETTLE)')
        players_event_details = pd.read_sql_query(exiting_player_event_query,session.bind,params={"commentary_id":commentary_id, "current_team_id":commentary_team,  "market_type_category_id":market_type_category_id,"market_name":fow_market_name,"CANCEL":CANCEL, "SETTLE":SETTLED})
    
    
    if len(players_event_details) > 0:
        return players_event_details
    else:
        return None

def get_players_predefined(commentary_id,commentary_team):
     
    with get_db_session() as session:
        players_predefined_query=text('select SUM("wrBatsmanAverage") AS "wrPlayersAverage" from "tblCommentaryPlayers" where "wrCommentaryId"=:commentary_id and "wrTeamId"=:team_id and "wrBat_IsPlay"=:is_play')
        players_predefined = pd.read_sql_query(players_predefined_query,session.bind,params={"commentary_id": commentary_id, "team_id": commentary_team, "is_play":True})
       
        return players_predefined["wrPlayersAverage"].iloc[0] if not players_predefined.empty else 0


def create_fow_event_market(commentary_id,commentary_team,match_type_id, market_template_result, status, predefined,event_id,wicket_count,current_score): 
    markets_to_create=None
    global LD_FOW_MARKETS
    key = f"{commentary_id}_{commentary_team}"
    if key not in LD_FOW_MARKETS:
        column_names=common.get_column_names("tblEventMarkets")

        fow_markets=pd.DataFrame(columns=column_names)
    else:
        fow_markets=LD_FOW_MARKETS[f"{commentary_id}_{commentary_team}"]
        
    if wicket_count==0:
        markets_to_create=market_template_result["wrHowManyOpenMarkets"]
    else:
        markets_to_create=1
    market_name=market_template_result["wrTemplateName"]
    for i in range(markets_to_create):
        new_wicket=None
        if wicket_count==0:
            new_wicket=i+1
            
        else:
            if not fow_markets.empty:
                new_wicket=int(fow_markets["wrWicketNo"].max())+1
        
            else:
                new_wicket=wicket_count+market_template_result["wrHowManyOpenMarkets"]
        fow_market_name = market_name.format(wicket=f"{new_wicket}{common.ordinal_suffix(new_wicket)}")
        team_name=common.get_commentary_teams(commentary_id,1)
        fow_market_name= fow_market_name + " - "+team_name[1]
        fow_market=check_existing_fow_partnership_market(commentary_id,commentary_team, fow_market_name,WICKET)
        #if fow_market is not None:
        #    return fow_market
        #else:
        if fow_market is None:    
            with get_db_session() as session:
        
                
                line_odd=0
                time_now = datetime.now()
                market_template_result["wrPredefinedValue"] =predefined
                current_innings = common.get_current_innings(commentary_id)
                if status == OPEN:            
                    new_fow_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue", "wrWicketNo") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff,:predefined_value, :wicket_no) RETURNING "wrID"')
                    result = session.execute(new_fow_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team),'innings_id':int(current_innings), 'market_name': str(fow_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': 0, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'template_type': int(market_template_result["wrTemplateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': line_odd, 'min_odd': line_odd, 'max_odd': line_odd, 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]), "wicket_no":int(new_wicket) })
                else:
                    new_fow_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue", "wrWicketNo") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff, :predefined_value, :wicket_no) RETURNING "wrID"')
                    result = session.execute(new_fow_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(fow_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': int(market_template_result['wrTemplateType']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': 0, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': line_odd, 'min_odd': line_odd, 'max_odd': line_odd, 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]), "wicket_no":int(new_wicket) })
                event_market_id = result.fetchone()[0]
        
                LOG.info("Insert Success in the player event market %s", event_market_id)
                session.commit()
                #row = { "wrID":event_market_id, "wrCommentaryId":  commentary_id, "wrEventRefID":  event_id, "wrTeamID":  commentary_team, "wrInningsID":current_innings,"wrMarketName": fow_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": bool(market_template_result['wrIsOver']), "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": 0, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": bool(market_template_result['wrIsDefaultBetAllowed']), "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": line_odd, "wrMinOdds": line_odd, "wrMaxOdds": line_odd, "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"],"wrPredefinedValue":market_template_result["wrPredefinedValue"],"wrSettledTime":None,"wrCloseTime":None , "wrWicketNo":new_wicket ,"player_score":0, "suspend_over":0,"close_over":0 }
                #df=pd.DataFrame([row])
                df= pd.read_sql_query(text('select * from "tblEventMarkets" where "wrID"=:id'), session.bind, params={"id":event_market_id})

                if key in LD_FOW_MARKETS:
                    fow_market=LD_FOW_MARKETS[f"{commentary_id}_{commentary_team}"]
                print("before merging the fow are ", fow_market)
                print("before merging fetched row is ", df)
                fow_market = pd.concat([fow_market, df], ignore_index=True)
                fow_market=fow_market.fillna(0)
                #LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]=player_markets
                
                
                rateDiff=market_template_result["wrRateDiff"]
                
                wrRunner = fow_market_name
                selection_id = f"{event_market_id}01"
                update_time = datetime.now()
                #if(lineType==STATICLINE):
                #    wrBackPrice=predicted_player_score
                #    wrLayPrice=predicted_player_score
                #elif(lineType==DYNAMICLINE):
                wrBackPrice = round(predefined) + int(rateDiff)
                wrLayPrice=round(predefined)
                wrBackSize=int(market_template_result["wrDefaultBackSize"])
                wrLaySize=int(market_template_result["wrDefaultLaySize"])
                margin=1+(float(df["wrMargin"].iloc[0])/100)
                over_value=0
                 
                under_value=0
                
                
            
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
                        'event_market_wrId': event_market_id,
                        'wrRunner': wrRunner,
                        'predicted_player_score': predefined,
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
                    LOG.info("Insert Success in the player boundary event runner market %s", runner_id)
                    session.commit()
                
                LD_FOW_MARKETS[f"{commentary_id}_{commentary_team}"]= fow_market
    return LD_FOW_MARKETS[f"{commentary_id}_{commentary_team}"]



def process_fow_markets(current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_details, market_template, ball_by_ball_id):
    print("starting time for fow is", time.time())

    global LD_FOW_MARKETS, LD_DEFAULT_VALUES
    stime=time.time()
    key = f"{commentary_id}_{commentary_team}"
    all_fow_event_markets = pd.DataFrame()
    runner_data = []
    event_data = []
    market_datalog = []
    socket_data = []
    market_template_row = market_template.iloc[0]
    market_name = market_template_row["wrTemplateName"]
    new_wicket = None
    is_create = False

    if key in LD_FOW_MARKETS:
        all_fow_event_markets = LD_FOW_MARKETS[key]

    player_wicket_df = get_wicket_player_details(commentary_id, commentary_team)
    total_wicket = len(player_wicket_df)
    new_wicket = total_wicket + market_template_row["wrHowManyOpenMarkets"] if total_wicket > 0 else total_wicket + 1

    fow_market_name = market_name.format(wicket=f"{new_wicket}{common.ordinal_suffix(new_wicket)}")
    team_name = common.get_commentary_teams(commentary_id,1)
    fow_market_name = fow_market_name + " - " + team_name[1]

    if (key not in LD_FOW_MARKETS) or (not all_fow_event_markets.empty and fow_market_name not in all_fow_event_markets["wrMarketName"].values) or (all_fow_event_markets.empty):
        create_type_id = market_template_row["wrCreateType"]
        if create_type_id == 1:
            with get_db_session() as session:
                query = text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets" w '
                             'INNER JOIN "tblCommentaryBallByBalls" b '
                             'ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" '
                             'WHERE w."wrCommentaryId"=:commentary_id AND w."wrTeamId"=:team_id ORDER BY w."wrCommentaryBallByBallId" DESC')
                wickets = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team})
                if wickets.empty:
                    if ((key not in LD_FOW_MARKETS ) or (key in LD_FOW_MARKETS  and all_fow_event_markets.empty)):
                        autoCreateBall = market_template_row["wrCreate"]
                        if current_ball < float(autoCreateBall):
                            return {"status": False, "msg": f"AutoStart ball is yet to start. Current ball {current_ball} is behind auto-create ball {autoCreateBall}", "data": None}
                        else:
                            is_create = True
                else:
                    last_wicket = wickets["wrOverCount"].iloc[0]
                    last_wicket_balls = common.overs_to_balls(last_wicket, match_type_id)
                    create_balls = last_wicket_balls + market_template_row["wrCreate"] * 10
                    autoCreateBall = int(create_balls / 6) + (create_balls % 6) / 10
                    if current_ball >= float(autoCreateBall):
                        is_create = True

        if new_wicket > int(market_template_row["wrAfterWicketNotCreated"]) or current_ball >= float(market_template_row["wrBeforeAutoClose"]):
            is_create = False

        autoOpenStatus = float(market_template_row["wrAutoOpen"])
        status = OPEN if autoOpenStatus <= current_ball else INACTIVE
        predefined_value = 20

        if is_create:
            fow_event_markets = create_fow_event_market(
                commentary_id,
                commentary_team,
                match_type_id,
                market_template_row,
                status,
                predefined_value,
                event_id,
                total_wicket,
                current_score
            )
            fow_event_markets = fow_event_markets.assign(player_score=0, suspend_over=0, close_over=0)
            all_fow_event_markets = fow_event_markets
        LD_FOW_MARKETS[key] = all_fow_event_markets

   # players_predefined = get_players_predefined(commentary_id, commentary_team)
    all_fow_event_markets = all_fow_event_markets.sort_values(by="wrWicketNo").reset_index(drop=True)
    is_wicket = False
    for index, row in all_fow_event_markets.iterrows():
        if row["wrStatus"] != SETTLED:
            suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
            close_balls = common.overs_to_balls(row["close_over"], match_type_id)
            current_over_balls = common.overs_to_balls(current_ball, match_type_id)

            market_runner = []
            is_update = False
            is_SendData = True
            predicted_value = fixed_line_value=over_value=under_value=back_price=lay_price=0
            lay_size = back_size=100
            selection_status = NOTCREATED

            if row["wrWicketNo"] <= total_wicket and total_wicket != 0 and ((row["wrStatus"] not in (SUSPEND, CLOSE)) or (row["wrStatus"] == SUSPEND and suspend_balls == 0)):
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                is_wicket = True
                selection_status = SUSPEND

            elif (total_wicket < row["wrWicketNo"] or total_wicket == 0) and row["wrStatus"] == SUSPEND:
                row["wrStatus"] = OPEN
                row["suspend_over"] = 0
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = OPEN

            elif current_over_balls >= suspend_balls + 2 and row["wrStatus"] == SUSPEND and suspend_balls > 0:
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                selection_status = CLOSE
                row["wrCloseTime"] = datetime.now().isoformat()

            elif current_ball >= row["wrAutoOpen"] and row["wrStatus"] == INACTIVE:
                row["wrStatus"] = OPEN
                selection_status = OPEN
                row["wrOpenTime"]=datetime.now().isoformat()

            if is_wicket:
                row["wrStatus"] = SUSPEND
                selection_status = SUSPEND

            if current_ball >= float(row["wrBeforeAutoClose"]):
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                row["wrCloseTime"] = datetime.now().isoformat()

            all_fow_event_markets.loc[index] = row

            if row["wrStatus"] != SETTLED:
                row["player_score"] = current_score
                margin = 1 + (float(row["wrMargin"]) / 100)
                
                values = all_fow_event_markets.loc[0:index, 'wrPredefinedValue'][
                    (all_fow_event_markets.loc[0:index, 'wrStatus'].isin([OPEN, SUSPEND])) &
                    (all_fow_event_markets.loc[0:index, 'suspend_over'] == 0)
                ]
                
                predicted_value = current_score + int(values.sum())
                predicted_value=common.round_down(predicted_value)
                new_predicted_value = int(predicted_value) + 0.5

                over_value = margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
                over_value = 1 / over_value

                under_value = margin / (1 + math.exp(predicted_value - new_predicted_value))
                under_value = 1 / under_value

                over_value = round(over_value, 2)
                under_value = round(under_value, 2)
                lay_price = common.custom_round(predicted_value)
                back_price = lay_price + row["wrRateDiff"]
                fixed_line_value = round(predicted_value, 1)
                lay_size = row["wrDefaultLaySize"]
                back_size = row["wrDefaultBackSize"]

                row["wrMinOdds"] = min(row["wrMinOdds"] or back_price, back_price)
                row["wrMaxOdds"] = max(row["wrMaxOdds"] or back_price, back_price)
                row["wrOpenOdds"] = row["wrOpenOdds"] or back_price

                if row["wrStatus"] == CLOSE:
                    selection_status = CLOSE
                elif row["wrStatus"] == OPEN:
                    selection_status = OPEN
                    is_SendData = row["wrDefaultIsSendData"]
            if (current_over_balls >= close_balls + row["wrAutoResultafterBall"] and row["wrStatus"] == CLOSE) or (row["wrWicketNo"] <= total_wicket and total_wicket != 0 and current_ball >= float(row["wrBeforeAutoClose"]) and row["wrStatus"] == CLOSE):
                row["wrResult"] = common.markets_result_settlement(row, commentary_id, commentary_team, row["wrWicketNo"])
                row["wrStatus"] = SETTLED
                selection_status = SETTLED
                row["wrSettledTime"] = datetime.now().isoformat()
            market_runner_row = common.fetch_runner_data(selection_status, row)
            if len(market_runner_row) > 0:
                runner_data_dict = {
                    "wrRunner": market_runner_row["wrRunner"].iloc[0],
                    "wrLine": fixed_line_value,
                    "wrOverRate": over_value,
                    "wrUnderRate": under_value,
                    "wrBackPrice": back_price,
                    "wrLayPrice": lay_price,
                    "wrBackSize": back_size,
                    "wrLaySize": lay_size,
                    "wrSelectionStatus": int(row["wrStatus"]),
                    "wrEventMarketId": int(row["wrID"]),
                    "wrRunnerId": int(market_runner_row["wrRunnerId"].iloc[0])
                }

                row["wrIsSendData"] = is_SendData
                runner_data.append(runner_data_dict)
                market_runner_row = pd.DataFrame([runner_data_dict])
                market_runner = common.convert_runner_data(market_runner_row)
                json_event_market = common.convert_event_market(row, market_runner)
                json_socket_data = common.convert_socket_data(row, market_runner, ball_by_ball_id)

                event_data.append(common.parse_event_data(row, json_event_market, is_SendData))

                market_datalog.append({
                    "wrCommentaryId": commentary_id,
                    "wrEventMarketId": row["wrID"],
                    "wrData": json_event_market,
                    "wrUpdateType": 1,
                    "wrIsSendData": is_SendData
                })

                row["wrData"] = json_event_market
                socket_data.append(json_socket_data)
            all_fow_event_markets.loc[index] = row
            LD_FOW_MARKETS[key] = all_fow_event_markets
    if runner_data and market_datalog and event_data:
        runner_json = json.dumps(runner_data)
        market_datalog_json = json.dumps(market_datalog)
        eventmarket_json = json.dumps(event_data)
        common.send_data_to_socket_async(commentary_id, socket_data)
        common.update_database_async(runner_json, market_datalog_json, eventmarket_json)
    print("Execution time for fow is", time.time() - stime)


def create_partnership_event_market(commentary_id,commentary_team,match_type_id, market_template_result, status, predefined,event_id,wicket_count,current_score): 
    markets_to_create=None
    global LD_PARTNERSHIPBOUNDARIES_MARKETS
    key = f"{commentary_id}_{commentary_team}"
    if key not in LD_PARTNERSHIPBOUNDARIES_MARKETS:
        column_names=common.get_column_names("tblEventMarkets")

        partnership_markets=pd.DataFrame(columns=column_names)
    else:
        partnership_markets=LD_PARTNERSHIPBOUNDARIES_MARKETS[f"{commentary_id}_{commentary_team}"]
        
    if wicket_count==0:
        markets_to_create=market_template_result["wrHowManyOpenMarkets"]
    else:
        markets_to_create=1
    market_name=market_template_result["wrTemplateName"]
    new_wicket=1
    if wicket_count>0:
        new_wicket=int(partnership_markets["wrWicketNo"].max())+1
    partnership_market_name = market_name.format(wicket=f"{new_wicket}{common.ordinal_suffix(new_wicket)}")
    team_name=common.get_commentary_teams(commentary_id,1)
    partnership_market_name= partnership_market_name + " - "+team_name[1]
    partnership_market=check_existing_fow_partnership_market(commentary_id,commentary_team, partnership_market_name,PARTNERSHIPBOUNDARIES)
    #if fow_market is not None:
    #    return fow_market
    #else:
    if partnership_market is None:    
        with get_db_session() as session:
    
            
            line_odd=0
            time_now = datetime.now()
            market_template_result["wrPredefinedValue"] =predefined
            current_innings = common.get_current_innings(commentary_id)
            if status == OPEN:            
                new_partnership_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue", "wrWicketNo") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff,:predefined_value, :wicket_no) RETURNING "wrID"')
                result = session.execute(new_partnership_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team),'innings_id':int(current_innings), 'market_name': str(partnership_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': 0, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'template_type': int(market_template_result["wrTemplateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': line_odd, 'min_odd': line_odd, 'max_odd': line_odd, 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]), "wicket_no":int(new_wicket) })
            else:
                new_partnership_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue", "wrWicketNo") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff, :predefined_value, :wicket_no) RETURNING "wrID"')
                result = session.execute(new_partnership_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(partnership_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': int(market_template_result['wrTemplateType']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': 0, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': line_odd, 'min_odd': line_odd, 'max_odd': line_odd, 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]), "wicket_no":int(new_wicket) })
            event_market_id = result.fetchone()[0]
    
            LOG.info("Insert Success in the player event market %s", event_market_id)
            session.commit()
            
            #row = { "wrID":event_market_id, "wrCommentaryId":  commentary_id, "wrEventRefID":  event_id, "wrTeamID":  commentary_team, "wrInningsID":current_innings,"wrMarketName": partnership_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": bool(market_template_result['wrIsOver']), "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": 0, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": bool(market_template_result['wrIsDefaultBetAllowed']), "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": line_odd, "wrMinOdds": line_odd, "wrMaxOdds": line_odd, "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"],"wrPredefinedValue":market_template_result["wrPredefinedValue"],"wrSettledTime":None,"wrCloseTime":None , "wrWicketNo":new_wicket ,"player_score":0, "suspend_over":0,"close_over":0 }
            #df=pd.DataFrame([row])
            df= pd.read_sql_query(text('select * from "tblEventMarkets" where "wrID"=:id'), session.bind, params={"id":event_market_id})
            if key in LD_PARTNERSHIPBOUNDARIES_MARKETS:
                partnership_market=LD_PARTNERSHIPBOUNDARIES_MARKETS[f"{commentary_id}_{commentary_team}"]
            partnership_market = pd.concat([partnership_market, df], ignore_index=True)
            partnership_market=partnership_market.fillna(0)
            #LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]=player_markets
            
            
            rateDiff=market_template_result["wrRateDiff"]
            
            wrRunner = partnership_market_name
            selection_id = f"{event_market_id}01"
            update_time = datetime.now()
            #if(lineType==STATICLINE):
            #    wrBackPrice=predicted_player_score
            #    wrLayPrice=predicted_player_score
            #elif(lineType==DYNAMICLINE):
            wrBackPrice = round(predefined) + int(rateDiff)
            wrLayPrice=round(predefined)
            wrBackSize=int(market_template_result["wrDefaultBackSize"])
            wrLaySize=int(market_template_result["wrDefaultLaySize"])
            margin=1+(float(df["wrMargin"].iloc[0])/100)
            over_value=0
             
            under_value=0
            
            
        
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
                    'event_market_wrId': event_market_id,
                    'wrRunner': wrRunner,
                    'predicted_player_score': predefined,
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
                LOG.info("Insert Success in the player boundary event runner market %s", runner_id)
                session.commit()
            
            LD_PARTNERSHIPBOUNDARIES_MARKETS[f"{commentary_id}_{commentary_team}"]= partnership_market
    return LD_PARTNERSHIPBOUNDARIES_MARKETS[f"{commentary_id}_{commentary_team}"]



def process_partnership_markets(current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_details, partnership_details, market_template, ball_by_ball_id):
    print("starting time for partnership is", time.time())

    global LD_PARTNERSHIPBOUNDARIES_MARKETS, LD_DEFAULT_VALUES
    stime=time.time()
    key = f"{commentary_id}_{commentary_team}"
    all_partnership_event_markets = pd.DataFrame()
    runner_data = []
    event_data = []
    market_datalog = []
    socket_data = []
    market_template_row = market_template.iloc[0]
    market_name = market_template_row["wrTemplateName"]
    new_wicket = 1
    is_create = False

    if key in LD_PARTNERSHIPBOUNDARIES_MARKETS:
        all_partnership_event_markets = LD_PARTNERSHIPBOUNDARIES_MARKETS[key]

    player_wicket_df = get_wicket_player_details(commentary_id, commentary_team)
    total_wicket = len(player_wicket_df)
    new_wicket = total_wicket + 1 if total_wicket > 0 else 1

    partnership_market_name = market_name.format(wicket=f"{new_wicket}{common.ordinal_suffix(new_wicket)}")
    team_name = common.get_commentary_teams(commentary_id,1)
    partnership_market_name = partnership_market_name + " - " + team_name[1]

    if (key not in LD_PARTNERSHIPBOUNDARIES_MARKETS) or (not all_partnership_event_markets.empty and partnership_market_name not in all_partnership_event_markets["wrMarketName"].values) or (all_partnership_event_markets.empty):

        create_type_id = market_template_row["wrCreateType"]
        if create_type_id == 1:
            with get_db_session() as session:
                query = text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets" w '
                             'INNER JOIN "tblCommentaryBallByBalls" b '
                             'ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" '
                             'WHERE w."wrCommentaryId"=:commentary_id AND w."wrTeamId"=:team_id ORDER BY w."wrCommentaryBallByBallId" DESC')
                wickets = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team})
                if wickets.empty:
                    autoCreateBall = market_template_row["wrCreate"]
                    if current_ball < float(autoCreateBall):
                        return {"status": False, "msg": f"AutoStart ball is yet to start. Current ball {current_ball} is behind auto-create ball {autoCreateBall}", "data": None}
                    else:
                        is_create = True
                else:
                    last_wicket = wickets["wrOverCount"].iloc[0]
                    last_wicket_balls = common.overs_to_balls(last_wicket, match_type_id)
                    create_balls = last_wicket_balls + market_template_row["wrCreate"] * 10
                    autoCreateBall = int(create_balls / 6) + (create_balls % 6) / 10
                    if current_ball >= float(autoCreateBall):
                        is_create = True

        if new_wicket > int(market_template_row["wrAfterWicketNotCreated"]) or current_ball >= float(market_template_row["wrBeforeAutoClose"]):
            is_create = False

        autoOpenStatus = float(market_template_row["wrAutoOpen"])
        status = OPEN if autoOpenStatus <= current_ball else INACTIVE
        predefined_value = 2

        if is_create:
            partnership_event_markets = create_partnership_event_market(
                commentary_id,
                commentary_team,
                match_type_id,
                market_template_row,
                status,
                predefined_value,
                event_id,
                total_wicket,
                current_score
            )
            partnership_event_markets = partnership_event_markets.assign(player_score=0, suspend_over=0, close_over=0)
            all_partnership_event_markets = partnership_event_markets
        LD_PARTNERSHIPBOUNDARIES_MARKETS[key] = all_partnership_event_markets

    all_partnership_event_markets = all_partnership_event_markets.sort_values(by="wrWicketNo").reset_index(drop=True)
    is_wicket = False

    for index, row in all_partnership_event_markets.iterrows():
        if row["wrStatus"] != SETTLED:
            suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
            close_balls = common.overs_to_balls(row["close_over"], match_type_id)
            current_over_balls = common.overs_to_balls(current_ball, match_type_id)

            market_runner = []
            is_update = False
            is_SendData = True
            predicted_value = fixed_line_value=over_value=under_value=back_price=lay_price=0
            lay_size = back_size=100
            selection_status = NOTCREATED

            if row["wrWicketNo"] <= total_wicket and total_wicket != 0 and ((row["wrStatus"] not in (SUSPEND, CLOSE)) or (row["wrStatus"] == SUSPEND and suspend_balls == 0)):
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                is_wicket = True
                selection_status = SUSPEND

            elif (total_wicket < row["wrWicketNo"] or total_wicket == 0) and row["wrStatus"] == SUSPEND:
                row["wrStatus"] = OPEN
                row["suspend_over"] = 0
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = OPEN

            elif current_over_balls >= suspend_balls + 2 and row["wrStatus"] == SUSPEND and suspend_balls > 0:
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                selection_status = CLOSE
                row["wrCloseTime"] = datetime.now().isoformat()

            elif current_ball >= row["wrAutoOpen"] and row["wrStatus"] == INACTIVE:
                row["wrStatus"] = OPEN
                selection_status = OPEN
                row["wrOpenTime"]=datetime.now().isoformat()

            if is_wicket:
                row["wrStatus"] = SUSPEND
                selection_status = SUSPEND

            if current_ball >= float(row["wrBeforeAutoClose"]):
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                row["wrCloseTime"] = datetime.now().isoformat()

            all_partnership_event_markets.loc[index] = row

            if row["wrStatus"] != SETTLED:
                row["player_score"] = current_score
                margin = 1 + (float(row["wrMargin"]) / 100)

                partnership_boundaries = next((boundaries for boundaries in partnership_details if boundaries['partnership_no'] == row["wrWicketNo"]), None)

                predicted_value = row["wrPredefinedValue"] + (int(partnership_boundaries["partnership_boundaries"]) if partnership_boundaries is not None else 0)
                predicted_value=common.round_down(predicted_value)
                new_predicted_value = int(predicted_value) + 0.5

                over_value = margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
                over_value = 1 / over_value

                under_value = margin / (1 + math.exp(predicted_value - new_predicted_value))
                under_value = 1 / under_value

                over_value = round(over_value, 2)
                under_value = round(under_value, 2)
                lay_price = common.custom_round(predicted_value)
                back_price = lay_price + row["wrRateDiff"]
                fixed_line_value = round(predicted_value, 1)
                lay_size = row["wrDefaultLaySize"]
                back_size = row["wrDefaultBackSize"]

                row["wrMinOdds"] = min(row["wrMinOdds"] or back_price, back_price)
                row["wrMaxOdds"] = max(row["wrMaxOdds"] or back_price, back_price)
                row["wrOpenOdds"] = row["wrOpenOdds"] or back_price

                if row["wrStatus"] == CLOSE:
                    selection_status = CLOSE
                elif row["wrStatus"] == OPEN:
                    selection_status = OPEN
                    is_SendData = row["wrDefaultIsSendData"]

            if (current_over_balls >= close_balls + row["wrAutoResultafterBall"] and row["wrStatus"] == CLOSE) or (row["wrWicketNo"] <= total_wicket and total_wicket != 0 and current_ball >= float(row["wrBeforeAutoClose"]) and row["wrStatus"] == CLOSE):
                row["wrResult"] = common.markets_result_settlement(row, commentary_id, commentary_team, row["wrWicketNo"])
                row["wrStatus"] = SETTLED
                selection_status = SETTLED
                row["wrSettledTime"] = datetime.now().isoformat()

            market_runner_row = common.fetch_runner_data(selection_status, row)
            if len(market_runner_row) > 0:
                runner_data_dict = {
                    "wrRunner": market_runner_row["wrRunner"].iloc[0],
                    "wrLine": fixed_line_value,
                    "wrOverRate": over_value,
                    "wrUnderRate": under_value,
                    "wrBackPrice": back_price,
                    "wrLayPrice": lay_price,
                    "wrBackSize": back_size,
                    "wrLaySize": lay_size,
                    "wrSelectionStatus": int(row["wrStatus"]),
                    "wrEventMarketId": int(row["wrID"]),
                    "wrRunnerId": int(market_runner_row["wrRunnerId"].iloc[0])
                }

                row["wrIsSendData"] = is_SendData
                runner_data.append(runner_data_dict)
                market_runner_row = pd.DataFrame([runner_data_dict])
                market_runner = common.convert_runner_data(market_runner_row)
                json_event_market = common.convert_event_market(row, market_runner)

                json_socket_data = common.convert_socket_data(row, market_runner,ball_by_ball_id)
                
                event_data.append(common.parse_event_data(row,json_event_market,is_SendData))
                
            
                market_datalog.append({
                    "wrCommentaryId": commentary_id,
                    "wrEventMarketId": row["wrID"],
                    "wrData": json_event_market,
                    "wrUpdateType": 1,
                    "wrIsSendData": is_SendData
                })
            
                row["wrData"] = json_event_market
                socket_data.append(json_socket_data)
            all_partnership_event_markets.loc[index]=row  
            LD_PARTNERSHIPBOUNDARIES_MARKETS[f"{commentary_id}_{commentary_team}"]=all_partnership_event_markets
    
    
    if runner_data and market_datalog and event_data:
        runner_json=json.dumps(runner_data)
        market_datalog_json=json.dumps(market_datalog)
        eventmarket_json=json.dumps(event_data)
        common.send_data_to_socket_async(commentary_id,socket_data)
        common.update_database_async(runner_json,market_datalog_json,eventmarket_json)
    print("Execution time for partnership boundaries is", time.time() - stime)




def create_lostballs_event_market(commentary_id,commentary_team,match_type_id, market_template_result, status, predefined,event_id,wicket_count,current_score): 

    markets_to_create=None
    global LD_WICKETLOSTBALLS_MARKETS
    key = f"{commentary_id}_{commentary_team}"
    if key not in LD_WICKETLOSTBALLS_MARKETS:
        column_names=common.get_column_names("tblEventMarkets")

        partneship_markets=pd.DataFrame(columns=column_names)
    else:
        lostballs_markets=LD_WICKETLOSTBALLS_MARKETS[f"{commentary_id}_{commentary_team}"]
        
    if wicket_count==0:
        markets_to_create=market_template_result["wrHowManyOpenMarkets"]
    else:
        markets_to_create=1
    market_name=market_template_result["wrTemplateName"]
    new_wicket=1
    if wicket_count>0:
        new_wicket=int(lostballs_markets["wrWicketNo"].max())+1
    lostballs_market_name = market_name.format(wicket=f"{new_wicket}{common.ordinal_suffix(new_wicket)}")
    team_name=common.get_commentary_teams(commentary_id,1)
    lostballs_market_name= lostballs_market_name + " - "+team_name[1]
    lostballs_market=check_existing_fow_partnership_market(commentary_id,commentary_team, lostballs_market_name,WICKETLOSTBALLS)
    #if fow_market is not None:
    #    return fow_market
    #else:
    if lostballs_market is None:    
        with get_db_session() as session:
    
            
            line_odd=0
            time_now = datetime.now()
            market_template_result["wrPredefinedValue"] =predefined
            current_innings = common.get_current_innings(commentary_id)
            if status == OPEN:            
                new_lostballs_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrOpenTime", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrTemplateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue", "wrWicketNo") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :open_time, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:template_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff,:predefined_value, :wicket_no) RETURNING "wrID"')
                result = session.execute(new_lostballs_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team),'innings_id':int(current_innings), 'market_name': str(lostballs_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': 0, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'open_time': time_now, 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'template_type': int(market_template_result["wrTemplateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': line_odd, 'min_odd': line_odd, 'max_odd': line_odd, 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]), "wicket_no":int(new_wicket) })
            else:
                new_lostballs_event_query = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrMargin", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrPlayerID", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultType", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrIsAllow", "wrLastUpdate", "wrMarketTypeCategoryId", "wrRateSource","wrMarketTypeId","wrMarketTemplateId","wrCreateType","wrDelay", "wrCreate","wrCreateRefId","wrOpenRefId","wrActionType", "wrOpenOdds","wrMinOdds","wrMaxOdds","wrLineType", "wrDefaultBackSize", "wrDefaultLaySize","wrDefaultIsSendData", "wrRateDiff", "wrPredefinedValue", "wrWicketNo") VALUES (:commentary_id, :event_id, :team_id, :innings_id, :market_name, :margin, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :player_id, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_type, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :is_allow, :last_update, :market_type_category_id, 1,:market_type_id,:market_template_id,:create_type,:delay, :create,:create_ref_id,:open_ref_id,:action_type, :open_odd,:min_odd, :max_odd, :line_type, :default_back_size, :default_lay_size,:default_is_send_data, :rate_diff, :predefined_value, :wicket_no) RETURNING "wrID"')
                result = session.execute(new_lostballs_event_query, { 'commentary_id': int(commentary_id), 'event_id': str(event_id), 'team_id': int(commentary_team), 'innings_id':int(current_innings),'market_name': str(lostballs_market_name), 'margin': float(market_template_result["wrMargin"]), 'status': int(status), 'is_predefine_market': bool(market_template_result['wrIsPredefineMarket']), 'template_type': int(market_template_result['wrTemplateType']), 'is_over': bool(market_template_result['wrIsOver']), 'over': int(0), 'is_player': True, 'player_id': 0, 'is_auto_cancel': bool(market_template_result['wrIsAutoCancel']), 'auto_open_type': int(market_template_result['wrAutoOpenType']), 'auto_open': float(market_template_result['wrAutoOpen']), 'auto_close_type': int(market_template_result['wrAutoCloseType']), 'before_auto_close': float(market_template_result['wrBeforeAutoClose']), 'auto_suspend_type': int(market_template_result['wrAutoSuspendType']), 'before_auto_suspend': float(market_template_result['wrBeforeAutoSuspend']), 'is_ball_start': bool(market_template_result['wrIsBallStart']), 'is_auto_result_set': bool(market_template_result['wrIsAutoResultSet']), 'auto_result_type': int(market_template_result['wrAutoResultType']), 'auto_result_after_ball': float(market_template_result['wrAutoResultafterBall']), 'after_wicket_auto_suspend': int(market_template_result['wrAfterWicketAutoSuspend']), 'after_wicket_not_created': int(market_template_result['wrAfterWicketNotCreated']), 'is_active': bool(market_template_result['wrIsActive']), 'is_allow': bool(market_template_result['wrIsDefaultBetAllowed']), 'last_update': time_now, 'market_type_category_id': int(market_template_result["wrMarketTypeCategoryId"]), 'market_type_id': int(market_template_result["wrMarketTypeId"]), 'market_template_id': int(market_template_result["wrID"]), 'create_type': int(market_template_result["wrCreateType"]), 'delay': int(market_template_result["wrDelay"]), 'create': float(market_template_result["wrCreate"]), 'create_ref_id': int(market_template_result["wrCreateRefId"]) if market_template_result["wrCreateRefId"] is not None else None, 'open_ref_id': int(market_template_result["wrOpenRefId"]) if market_template_result["wrOpenRefId"] is not None else None, 'action_type': int(market_template_result["wrActionType"]), 'open_odd': line_odd, 'min_odd': line_odd, 'max_odd': line_odd, 'line_type': int(market_template_result["wrLineType"]), 'default_back_size': int(market_template_result["wrDefaultBackSize"]), 'default_lay_size': int(market_template_result["wrDefaultLaySize"]), 'default_is_send_data': bool(market_template_result["wrDefaultIsSendData"]), 'rate_diff': int(market_template_result["wrRateDiff"]), 'predefined_value': float(market_template_result["wrPredefinedValue"]), "wicket_no":int(new_wicket) })
            event_market_id = result.fetchone()[0]
    
            LOG.info("Insert Success in the player event market %s", event_market_id)
            session.commit()
            
            #row = { "wrID":event_market_id, "wrCommentaryId":  commentary_id, "wrEventRefID":  event_id, "wrTeamID":  commentary_team, "wrInningsID":current_innings,"wrMarketName": lostballs_market_name, "wrMargin": market_template_result["wrMargin"], "wrStatus": status, "wrIsPredefineMarket": market_template_result['wrIsPredefineMarket'], "wrIsOver": bool(market_template_result['wrIsOver']), "wrOver": market_template_result['wrOver'], "wrIsPlayer": True, "wrPlayerID": 0, "wrIsAutoCancel": market_template_result['wrIsAutoCancel'], "wrAutoOpenType": market_template_result['wrAutoOpenType'], "wrAutoOpen": market_template_result['wrAutoOpen'], "wrAutoCloseType": market_template_result['wrAutoCloseType'], "wrBeforeAutoClose": market_template_result['wrBeforeAutoClose'], "wrAutoSuspendType": market_template_result['wrAutoSuspendType'], "wrBeforeAutoSuspend": market_template_result['wrBeforeAutoSuspend'], "wrIsBallStart": market_template_result['wrIsBallStart'], "wrIsAutoResultSet": market_template_result['wrIsAutoResultSet'], "wrAutoResultType": market_template_result['wrAutoResultType'], "wrAutoResultafterBall": market_template_result['wrAutoResultafterBall'], "wrAfterWicketAutoSuspend": market_template_result['wrAfterWicketAutoSuspend'], "wrAfterWicketNotCreated": market_template_result['wrAfterWicketNotCreated'], "wrIsActive": market_template_result['wrIsActive'], "wrIsAllow": bool(market_template_result['wrIsDefaultBetAllowed']), "wrOpenTime": time_now, "wrLastUpdate": time_now, "wrMarketTypeCategoryId": market_template_result["wrMarketTypeCategoryId"], "wrMarketTypeId": market_template_result["wrMarketTypeId"], "wrMarketTemplateId": market_template_result["wrID"], "wrCreateType": market_template_result["wrCreateType"], "wrTemplateType": market_template_result["wrTemplateType"], "wrDelay": market_template_result["wrDelay"], "wrCreate": market_template_result["wrCreate"], "wrCreateRefId": market_template_result["wrCreateRefId"], "wrOpenRefId": market_template_result["wrOpenRefId"], "wrActionType": market_template_result["wrActionType"], "wrOpenOdds": line_odd, "wrMinOdds": line_odd, "wrMaxOdds": line_odd, "wrLineType": market_template_result["wrLineType"], "wrDefaultBackSize": market_template_result["wrDefaultBackSize"], "wrDefaultLaySize": market_template_result["wrDefaultLaySize"], "wrDefaultIsSendData": market_template_result["wrDefaultIsSendData"], "wrRateDiff": market_template_result["wrRateDiff"],"wrPredefinedValue":market_template_result["wrPredefinedValue"],"wrSettledTime":None,"wrCloseTime":None , "wrWicketNo":new_wicket ,"player_score":0, "suspend_over":0,"close_over":0 }
            #df=pd.DataFrame([row])
            df= pd.read_sql_query(text('select * from "tblEventMarkets" where "wrID"=:id'), session.bind, params={"id":event_market_id})
            if key in LD_WICKETLOSTBALLS_MARKETS:
                lostballs_market=LD_WICKETLOSTBALLS_MARKETS[f"{commentary_id}_{commentary_team}"]
            lostballs_market = pd.concat([lostballs_market, df], ignore_index=True)
            lostballs_market=lostballs_market.fillna(0)
            #LD_PLAYER_MARKETS[f"{commentary_id}_{commentary_team}"]=player_markets
            
            
            rateDiff=market_template_result["wrRateDiff"]
            
            wrRunner = lostballs_market_name
            selection_id = f"{event_market_id}01"
            update_time = datetime.now()
            #if(lineType==STATICLINE):
            #    wrBackPrice=predicted_player_score
            #    wrLayPrice=predicted_player_score
            #elif(lineType==DYNAMICLINE):
            wrBackPrice = round(predefined) + int(rateDiff)
            wrLayPrice=round(predefined)
            wrBackSize=int(market_template_result["wrDefaultBackSize"])
            wrLaySize=int(market_template_result["wrDefaultLaySize"])
            margin=1+(float(df["wrMargin"].iloc[0])/100)
            over_value=0
             
            under_value=0
            
            
        
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
                    'event_market_wrId': event_market_id,
                    'wrRunner': wrRunner,
                    'predicted_player_score': predefined,
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
                LOG.info("Insert Success in the player boundary event runner market %s", runner_id)
                session.commit()
            
            LD_WICKETLOSTBALLS_MARKETS[f"{commentary_id}_{commentary_team}"]= lostballs_market
    return LD_WICKETLOSTBALLS_MARKETS[f"{commentary_id}_{commentary_team}"]


def process_lostballs_markets(current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_details, market_template, ball_by_ball_id):
    print("starting time for lostballs is", time.time())

    global LD_WICKETLOSTBALLS_MARKETS, LD_DEFAULT_VALUES
    stime=time.time()
    key = f"{commentary_id}_{commentary_team}"
    all_lostballs_event_markets = pd.DataFrame()
    runner_data = []
    event_data = []
    market_datalog = []
    socket_data = []
    market_template_row = market_template.iloc[0]
    market_name = market_template_row["wrTemplateName"]
    new_wicket = 1
    is_create = False

    if key in LD_WICKETLOSTBALLS_MARKETS:
        all_lostballs_event_markets = LD_WICKETLOSTBALLS_MARKETS[key]

    player_wicket_df = get_wicket_player_details(commentary_id, commentary_team)
    total_wicket = len(player_wicket_df)
    new_wicket = total_wicket + 1 if total_wicket > 0 else 1

    lostballs_market_name = market_name.format(wicket=f"{new_wicket}{common.ordinal_suffix(new_wicket)}")
    team_name = common.get_commentary_teams(commentary_id,1)
    lostballs_market_name = lostballs_market_name + " - " + team_name[1]
    if (key not in LD_WICKETLOSTBALLS_MARKETS) or (not all_lostballs_event_markets.empty and lostballs_market_name not in all_lostballs_event_markets["wrMarketName"].values) or (all_lostballs_event_markets.empty):

        create_type_id = market_template_row["wrCreateType"]
        if create_type_id == 1:
            with get_db_session() as session:
                query = text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets" w '
                             'INNER JOIN "tblCommentaryBallByBalls" b '
                             'ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" '
                             'WHERE w."wrCommentaryId"=:commentary_id AND w."wrTeamId"=:team_id ORDER BY w."wrCommentaryBallByBallId" DESC')
                wickets = pd.read_sql_query(query, session.bind, params={"commentary_id": commentary_id, "team_id": commentary_team})
                if wickets.empty:
                    autoCreateBall = market_template_row["wrCreate"]
                    if current_ball < float(autoCreateBall):
                        return {"status": False, "msg": f"AutoStart ball is yet to start. Current ball {current_ball} is behind auto-create ball {autoCreateBall}", "data": None}
                    else:
                        is_create = True
                else:
                    last_wicket = wickets["wrOverCount"].iloc[0]
                    last_wicket_balls = common.overs_to_balls(last_wicket, match_type_id)
                    create_balls = last_wicket_balls + market_template_row["wrCreate"] * 10
                    autoCreateBall = int(create_balls / 6) + (create_balls % 6) / 10
                    if current_ball >= float(autoCreateBall):
                        is_create = True

        if new_wicket > int(market_template_row["wrAfterWicketNotCreated"]) or current_ball >= float(market_template_row["wrBeforeAutoClose"]):
            is_create = False

        autoOpenStatus = float(market_template_row["wrAutoOpen"])
        status = OPEN if autoOpenStatus <= current_ball else INACTIVE
        predefined_value = 18

        if is_create:
            lostballs_event_markets = create_lostballs_event_market(
                commentary_id,
                commentary_team,
                match_type_id,
                market_template_row,
                status,
                predefined_value,
                event_id,
                total_wicket,
                current_score
            )
            lostballs_event_markets = lostballs_event_markets.assign(player_score=0, suspend_over=0, close_over=0)
            all_lostballs_event_markets = lostballs_event_markets
        LD_WICKETLOSTBALLS_MARKETS[key] = all_lostballs_event_markets

    all_lostballs_event_markets = all_lostballs_event_markets.sort_values(by="wrWicketNo").reset_index(drop=True)
    is_wicket = False

    for index, row in all_lostballs_event_markets.iterrows():
        if row["wrStatus"] != SETTLED:
            suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
            close_balls = common.overs_to_balls(row["close_over"], match_type_id)
            current_over_balls = common.overs_to_balls(current_ball, match_type_id)

            market_runner = []
            is_update = False
            is_SendData = True
            predicted_value = fixed_line_value=over_value=under_value=back_price=lay_price=0
            lay_size = back_size=100
            
            selection_status = NOTCREATED

            if row["wrWicketNo"] <= total_wicket and total_wicket != 0 and ((row["wrStatus"] not in (SUSPEND, CLOSE)) or (row["wrStatus"] == SUSPEND and suspend_balls == 0)):
                row["wrStatus"] = SUSPEND
                row["suspend_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                is_wicket = True
                selection_status = SUSPEND

            elif (total_wicket < row["wrWicketNo"] or total_wicket == 0) and row["wrStatus"] == SUSPEND:
                row["wrStatus"] = OPEN
                row["suspend_over"] = 0
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                selection_status = OPEN

            elif current_over_balls >= suspend_balls + 2 and row["wrStatus"] == SUSPEND and suspend_balls > 0:
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                selection_status = CLOSE
                row["wrCloseTime"] = datetime.now().isoformat()

            elif current_ball >= row["wrAutoOpen"] and row["wrStatus"] == INACTIVE:
                row["wrStatus"] = OPEN
                selection_status = OPEN
                row["wrOpenTime"]=datetime.now().isoformat()

            if is_wicket:
                row["wrStatus"] = SUSPEND
                selection_status = SUSPEND

            if current_ball >= float(row["wrBeforeAutoClose"]):
                row["wrStatus"] = CLOSE
                row["close_over"] = current_ball
                suspend_balls = common.overs_to_balls(row["suspend_over"], match_type_id)
                close_balls = common.overs_to_balls(row["close_over"], match_type_id)
                row["wrCloseTime"] = datetime.now().isoformat()

            all_lostballs_event_markets.loc[index] = row

            if row["wrStatus"] != SETTLED:
                row["player_score"] = current_score
                margin = 1 + (float(row["wrMargin"]) / 100)

                predicted_value = row["wrPredefinedValue"] + common.overs_to_balls(current_ball, match_type_id)
                predicted_value=common.round_down(predicted_value)
                new_predicted_value = int(predicted_value) + 0.5

                over_value = margin / (1 + math.exp(-(predicted_value - new_predicted_value)))
                over_value = 1 / over_value

                under_value = margin / (1 + math.exp(predicted_value - new_predicted_value))
                under_value = 1 / under_value

                over_value = round(over_value, 2)
                under_value = round(under_value, 2)
                lay_price = common.custom_round(predicted_value)
                back_price = lay_price + row["wrRateDiff"]
                fixed_line_value = round(predicted_value, 1)
                lay_size = row["wrDefaultLaySize"]
                back_size = row["wrDefaultBackSize"]

                
                row["wrMinOdds"] = min(row["wrMinOdds"] or back_price, back_price)
                row["wrMaxOdds"] = max(row["wrMaxOdds"] or back_price, back_price)
                row["wrOpenOdds"] = row["wrOpenOdds"] or back_price

                if row["wrStatus"] == CLOSE:
                    selection_status = CLOSE
                elif row["wrStatus"] == OPEN:
                    selection_status = OPEN
                    is_SendData = row["wrDefaultIsSendData"]

            if (current_over_balls >= close_balls + row["wrAutoResultafterBall"] and row["wrStatus"] == CLOSE) or (row["wrWicketNo"] <= total_wicket and total_wicket != 0 and current_ball >= float(row["wrBeforeAutoClose"]) and row["wrStatus"] == CLOSE):
                row["wrResult"] = common.overs_to_balls(common.markets_result_settlement(row, commentary_id, commentary_team, row["wrWicketNo"]), match_type_id)
                row["wrStatus"] = SETTLED
                selection_status = SETTLED
                row["wrSettledTime"] = datetime.now().isoformat()

            market_runner_row = common.fetch_runner_data(selection_status, row)
            if len(market_runner_row) > 0:
                runner_data_dict = {
                    "wrRunner": market_runner_row["wrRunner"].iloc[0],
                    "wrLine": fixed_line_value,
                    "wrOverRate": over_value,
                    "wrUnderRate": under_value,
                    "wrBackPrice": back_price,
                    "wrLayPrice": lay_price,
                    "wrBackSize": back_size,
                    "wrLaySize": lay_size,
                    "wrSelectionStatus": int(row["wrStatus"]),
                    "wrEventMarketId": int(row["wrID"]),
                    "wrRunnerId": int(market_runner_row["wrRunnerId"].iloc[0])
                }

                row["wrIsSendData"] = is_SendData
                runner_data.append(runner_data_dict)
                market_runner_row = pd.DataFrame([runner_data_dict])
                market_runner = common.convert_runner_data(market_runner_row)
                json_event_market = common.convert_event_market(row, market_runner)
                json_socket_data = common.convert_socket_data(row, market_runner, ball_by_ball_id)

                event_data.append(common.parse_event_data(row, json_event_market, is_SendData))

                market_datalog.append({
                    "wrCommentaryId": commentary_id,
                    "wrEventMarketId": row["wrID"],
                    "wrData": json_event_market,
                    "wrUpdateType": 1,
                    "wrIsSendData": is_SendData
                })

                row["wrData"] = json_event_market
                socket_data.append(json_socket_data)

            all_lostballs_event_markets.loc[index] = row
            LD_WICKETLOSTBALLS_MARKETS[key] = all_lostballs_event_markets

    if runner_data and market_datalog and event_data:
        runner_json = json.dumps(runner_data)
        market_datalog_json = json.dumps(market_datalog)
        eventmarket_json = json.dumps(event_data)
        common.send_data_to_socket_async(commentary_id, socket_data)
        common.update_database_async(runner_json, market_datalog_json, eventmarket_json)
    print("Execution time for lostballs is", time.time() - stime)



async def update_player_market_status(commentary_id,commentary_team,status):
    if commentary_team is None or commentary_team == 0:
        try:
            with get_db_session() as session:
                query = text('select "wrTeamId", "wrTeamScore" from "tblCommentaryTeams" where "wrCommentaryId" = :commentary_id and "wrTeamStatus" = 1')
                commentary_team_df = pd.read_sql(query, con=session.bind, params={"commentary_id": commentary_id})
                if not commentary_team_df.empty:
                    commentary_team = commentary_team_df['wrTeamId'][0]
        except SQLAlchemyError as e:
            print(f"Database query failed: {e}")
            return
    category_ids = {
        # PLAYER: 'LD_PLAYER_MARKETS',
        # PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
        # PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
        # WICKET:'LD_FOW_MARKETS',
        # PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
        # WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS'
       
    }
    
    socket_data = []
    data = []
    if status in {OPEN, SUSPEND}:
        for category_id, global_var_name in category_ids.items():
            global_dict = globals()[global_var_name]
            key=f"{commentary_id}_{commentary_team}"
            if key in global_dict:
           
                event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
            
                for index, row in event_market.iterrows():
                    try:
                        if (row["wrStatus"] == OPEN and status != OPEN and bool(row["wrIsBallStart"]) is True):
                            row["wrStatus"] = status
                            wrdata = json.loads(row["wrData"])
                            wrdata["status"] = status
                            #wrdata["isSendData"]=True
                            runner = wrdata["runner"][0]
                            runner["status"] = status
                            wrdata["runner"][0] = runner
                            row["wrData"] = json.dumps(wrdata)
                            row['wrIsSendData']=True
                            json_socket_data = common.convert_socket_data(row, wrdata["runner"])
                            socket_data.append(json_socket_data)
                            event_market.loc[index] = row
                            data.append({
                                "eventid": row["wrID"],
                                "status": status,
                                "data": row["wrData"],
                                "issenddata": True,
                                "lastupdate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00")
                            })
        
                        elif (row["wrStatus"] == SUSPEND and status != SUSPEND and bool(row["wrIsBallStart"]) is True):

                            row["wrStatus"] = status
                            wrdata = json.loads(row["wrData"])
                            wrdata["status"] = status
                            #wrdata["isSendData"]= False if status == OPEN else True
                            runner = wrdata["runner"][0]
                            runner["status"] = status
                            wrdata["runner"][0] = runner
                            row["wrData"] = json.dumps(wrdata)
                            row['wrIsSendData']=False if status == OPEN else True
                            json_socket_data = common.convert_socket_data(row, wrdata["runner"])
                            socket_data.append(json_socket_data)
                            event_market.loc[index] = row
                            data.append({
                                "eventid": row["wrID"],
                                "status": status,
                                "data": row["wrData"],
                                "issenddata": False if status == OPEN else True,
                                "lastupdate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00")
                            })
                    except Exception as e:
                        print(f"Error processing row {index}: {e}")
                        continue
                globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=event_market
        if data:
            data_json = json.dumps(data)
            try:
                with get_db_session() as session:
                    query = text('CALL update_market_status(:data_json)')
                    session.execute(query, {"data_json": data_json})
                    session.commit()
            except SQLAlchemyError as e:
                print(f"Database update failed: {e}")
                session.rollback()
                return
    
            try:
                common.send_data_to_socket_async(commentary_id,socket_data)
            except Exception as e:
              print(f"Error sending data to socket: {e}")
      
def update_player_markets_line(commentary_id,commentary_team,players_data, fall_of_wicket_data, partnership_data, lostballs_data):
    try:
        category_ids = {
            PLAYER: 'LD_PLAYER_MARKETS',
            PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
            PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
            WICKET:'LD_FOW_MARKETS',
            PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
            WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS'

        }
        for category_id, global_var_name in category_ids.items():
            global_dict = globals()[global_var_name]
            key=f"{commentary_id}_{commentary_team}"
            if key in global_dict:
           
                event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
                if category_id not in (WICKET,PARTNERSHIPBOUNDARIES):
                    for item in players_data:
                        for index,row in event_market.iterrows():
                            if(item["market_type_category_id"]==row["wrMarketTypeCategoryId"] and item["commentary_player_id"]==row["wrPlayerID"] and row["wrStatus"] not in (CLOSE,SETTLED)):
                                row["wrIsAllow"]=item["is_allow"]
                                row["wrIsSendData"]=item["is_senddata"]
                                row["wrIsActive"]=item["is_active"]
                                row["wrData"]=item["data"]
                                row["wrRateDiff"]=item["rate_diff"]
                                row["wrDefaultLaySize"] = item["lay_size"]
                                row["wrDefaultBackSize"] = item["back_size"]
                                row["wrPredefinedValue"]=item["line_diff"]+row["wrPredefinedValue"]
                                event_market.loc[index] = row
                                
                if category_id==WICKET:
                    for item in fall_of_wicket_data:
                        for index,row in event_market.iterrows():
                            if(item["market_type_category_id"]==row["wrMarketTypeCategoryId"]  and item["market_id"]==row["wrID"] and row["wrStatus"] not in (CLOSE,SETTLED)):
                                row["wrIsAllow"]=item["is_allow"]
                                row["wrIsSendData"]=item["is_senddata"]
                                row["wrIsActive"]=item["is_active"]
                                row["wrData"]=item["data"]
                                row["wrRateDiff"]=item["rate_diff"]
                                row["wrDefaultLaySize"] = item["lay_size"]
                                row["wrDefaultBackSize"] = item["back_size"]
                                row["wrPredefinedValue"]=item["line_diff"]+row["wrPredefinedValue"]
                                print("the updated value is "+row["wrMarketName"], row["wrPredefinedValue"])
                                event_market.loc[index] = row
                            
                if category_id==PARTNERSHIPBOUNDARIES:
                    for item in partnership_data:
                        for index,row in event_market.iterrows():
                            if(item["market_type_category_id"]==row["wrMarketTypeCategoryId"]  and item["market_id"]==row["wrID"] and row["wrStatus"] not in (CLOSE,SETTLED)):
                                row["wrIsAllow"]=item["is_allow"]
                                row["wrIsSendData"]=item["is_senddata"]
                                row["wrIsActive"]=item["is_active"]
                                row["wrData"]=item["data"]
                                row["wrRateDiff"]=item["rate_diff"]
                                row["wrDefaultLaySize"] = item["lay_size"]
                                row["wrDefaultBackSize"] = item["back_size"]
                                row["wrPredefinedValue"]=item["line_diff"]+row["wrPredefinedValue"]
                                print("the updated value is "+row["wrMarketName"], row["wrPredefinedValue"])
                                event_market.loc[index] = row
                
                if category_id==WICKETLOSTBALLS:
                    for item in lostballs_data:
                        for index,row in event_market.iterrows():
                            if(item["market_type_category_id"]==row["wrMarketTypeCategoryId"]  and item["market_id"]==row["wrID"] and row["wrStatus"] not in (CLOSE,SETTLED)):
                                row["wrIsAllow"]=item["is_allow"]
                                row["wrIsSendData"]=item["is_senddata"]
                                row["wrIsActive"]=item["is_active"]
                                row["wrData"]=item["data"]
                                row["wrRateDiff"]=item["rate_diff"]
                                row["wrDefaultLaySize"] = item["lay_size"]
                                row["wrDefaultBackSize"] = item["back_size"]
                                row["wrPredefinedValue"]=item["line_diff"]+row["wrPredefinedValue"]
                                print("the updated value is "+row["wrMarketName"], row["wrPredefinedValue"])
                                event_market.loc[index] = row
                                
                                
                                
                                
                globals()[global_var_name][f"{commentary_id}_{commentary_team}"] =event_market
        return {"status":True, "msg":""}
    except Exception as err:
        LOG.error("Error updating player markets line: %s", str(err))
        return {"status": False, "msg": str(err)}      
     

def process_task(func, *args):
    """
    Wrapper to run a task function with arguments in a thread.
    """
    try:
        func(*args)
    except Exception as e:
        print(f"Error in thread {func.__name__}: {e}")
        traceback.print_exc()
        
# def run_in_executor(func, *args):
#     with ProcessPoolExecutor(max_workers=6) as executor:
#         loop = asyncio.get_event_loop()
#         return loop.run_in_executor(executor, func, *args)  


# async def run_all_tasks_in_executor(tasks):
#     """
#     Submit all tasks to the ThreadPoolExecutor at once and run them concurrently.
#     """
#     loop = asyncio.get_event_loop()
#     with ThreadPoolExecutor(max_workers=2) as executor:
#         # Submit all tasks to the thread pool
#         futures = [
#             loop.run_in_executor(executor, func, *args)
#             for func, *args in tasks
#         ]
#         # Wait for all futures to complete
#         await asyncio.gather(*futures)


def execute_function(func, args):
    return func(*args)
    
async def run_all_tasks_in_executor(tasks):
    """
    Submit all tasks to the ThreadPoolExecutor at once and run them concurrently.
    """
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit all tasks to the thread pool
        futures = [
            loop.run_in_executor(executor, func, *args)
            for func, *args in tasks
        ]
        # Wait for all futures to complete
        await asyncio.gather(*futures)

async def run(commentary_id,commentary_team,match_type_id, current_ball,current_score, event_id,player_runs_details,partnership_details,ball_by_ball_id):
    try:
        global LD_MARKET_TEMPLATE
        # print("Available keys in LD_MARKET_TEMPLATE:", LD_MARKET_TEMPLATE.keys())

        market_template=LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]

        # async def safe_task(func, *args):
        #     try:
        #         # Measure the start time
        #         start_time = time.perf_counter()
        #         await func(*args)
        #         # Measure the end time
        #         end_time = time.perf_counter()
        #         # Log the execution time for the task
        #         duration = end_time - start_time
        #         print(f"Execution time for {func.__name__}: start time: {start_time}  , end time: {end_time}  ,  duration: {duration:.2f} seconds")
        #     except Exception as e:
        #         # Log the error with the function name and traceback
        #         print(f"Error in {func.__name__}: {str(e)}")
        #         traceback.print_exc()

        # #await update_player_market_status(commentary_id, commentary_team,OPEN)
        # player_market=market_template[market_template['wrMarketTypeCategoryId'] == PLAYER]
        # boundary_market=market_template[market_template['wrMarketTypeCategoryId'] == PLAYERBOUNDARIES]
        # balls_face_market=market_template[market_template['wrMarketTypeCategoryId'] == PLAYERBALLSFACED]
        
        # tasks = [
        #     safe_task(process_player_markets, current_ball,commentary_id,current_score,commentary_team,match_type_id, event_id,player_runs_details, player_market,ball_by_ball_id),
        #     safe_task(process_player_boundary_markets, current_ball,commentary_id,current_score,commentary_team,match_type_id, event_id,player_runs_details, boundary_market,ball_by_ball_id),
        #     safe_task(process_player_balls_faced_markets, current_ball,commentary_id,current_score,commentary_team,match_type_id, event_id,player_runs_details,balls_face_market ,ball_by_ball_id)
  
        #     ]

        #await update_player_market_status(commentary_id, commentary_team,OPEN)
        player_market=market_template[market_template['wrMarketTypeCategoryId'] == PLAYER]
        boundary_market=market_template[market_template['wrMarketTypeCategoryId'] == PLAYERBOUNDARIES]
        balls_face_market=market_template[market_template['wrMarketTypeCategoryId'] == PLAYERBALLSFACED]
        fall_of_wicket_market=market_template[market_template['wrMarketTypeCategoryId'] == WICKET]
        partnership_boundaries_market=market_template[market_template['wrMarketTypeCategoryId'] == PARTNERSHIPBOUNDARIES]
        wicket_lost_balls_market=market_template[market_template['wrMarketTypeCategoryId'] == WICKETLOSTBALLS]
        # tasks = [
        #     run_in_executor(process_player_markets, current_ball,commentary_id,current_score,commentary_team,match_type_id, event_id,player_runs_details, player_market,ball_by_ball_id),
        #     run_in_executor(process_player_boundary_markets, current_ball,commentary_id,current_score,commentary_team,match_type_id, event_id,player_runs_details, boundary_market,ball_by_ball_id),
        #     run_in_executor(process_player_balls_faced_markets, current_ball,commentary_id,current_score,commentary_team,match_type_id, event_id,player_runs_details,balls_face_market ,ball_by_ball_id)
  
        #     ]

        # await asyncio.gather(*tasks)
        
        executor = ProcessPoolExecutor()
        loop = asyncio.get_event_loop()
        tasks=[]
        
        if not player_market.empty:
            tasks.append(loop.run_in_executor(executor, process_player_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, player_market, ball_by_ball_id))
        
        # Check if boundary_market is not empty before adding the task
        if not boundary_market.empty:
            tasks.append(loop.run_in_executor(executor, process_player_boundary_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, boundary_market, ball_by_ball_id))
        
        # Check if balls_face_market is not empty before adding the task
        if not balls_face_market.empty:
            tasks.append(loop.run_in_executor(executor, process_player_balls_faced_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, balls_face_market, ball_by_ball_id))
        
        if not fall_of_wicket_market.empty:
            tasks.append(loop.run_in_executor(executor, process_fow_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, fall_of_wicket_market, ball_by_ball_id))
       
        if not partnership_boundaries_market.empty:
            tasks.append(loop.run_in_executor(executor, process_partnership_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details,partnership_details, partnership_boundaries_market, ball_by_ball_id))

        if not wicket_lost_balls_market.empty:
            tasks.append(loop.run_in_executor(executor, process_lostballs_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, wicket_lost_balls_market,ball_by_ball_id))
        
        if tasks:
            await asyncio.gather(*tasks)
        
        
        
        
        
        # tasks = []
        # if not fall_of_wicket_market.empty:
        #     tasks.append((process_fow_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, fall_of_wicket_market, ball_by_ball_id))

        # # Check if player_market is not empty before adding the task
        # if not player_market.empty:
        #     tasks.append((process_player_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, player_market, ball_by_ball_id))
        
        # # Check if boundary_market is not empty before adding the task
        # if not boundary_market.empty:
        #     tasks.append((process_player_boundary_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, boundary_market, ball_by_ball_id))
        
        # # Check if balls_face_market is not empty before adding the task
        # if not balls_face_market.empty:
        #     tasks.append((process_player_balls_faced_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, balls_face_market, ball_by_ball_id))
        
        
        # if not partnership_boundaries_market.empty:
        #     tasks.append((process_partnership_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details,partnership_details, partnership_boundaries_market, ball_by_ball_id))

        # if not wicket_lost_balls_market.empty:
        #     tasks.append((process_lostballs_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, wicket_lost_balls_market,ball_by_ball_id))
        
        # await run_in_executor(tasks)

        # formatted_tasks = [(task[0], task[1:]) for task in tasks]

        # with Pool(processes=1) as pool:  # Adjust the number of processes as needed
        #     pool.starmap(execute_function, formatted_tasks)
        #     pool.close()  # Close the pool to new tasks
        #     pool.join()
            
        
        # with ThreadPoolExecutor(max_workers=6) as executor:
        #     # Submit all tasks concurrently
        #     futures = [executor.submit(process_task, task[0], *task[1:]) for task in tasks]
# Only gather tasks if there are any
        
        # tasks=[]
        # if not player_market.empty:
        #     tasks.append(run_in_executor( process_player_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, player_market, ball_by_ball_id))
        
        # # Check if boundary_market is not empty before adding the task
        # if not boundary_market.empty:
        #     tasks.append(run_in_executor( process_player_boundary_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, boundary_market, ball_by_ball_id))
        
        # # Check if balls_face_market is not empty before adding the task
        # if not balls_face_market.empty:
        #     tasks.append(run_in_executor( process_player_balls_faced_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, balls_face_market, ball_by_ball_id))
        
        # if not fall_of_wicket_market.empty:
        #     tasks.append(run_in_executor( process_fow_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, fall_of_wicket_market, ball_by_ball_id))
       
        # if not partnership_boundaries_market.empty:
        #     tasks.append(run_in_executor(process_partnership_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details,partnership_details, partnership_boundaries_market, ball_by_ball_id))

        # if not wicket_lost_balls_market.empty:
        #     tasks.append(run_in_executor(process_lostballs_markets, current_ball, commentary_id, current_score, commentary_team, match_type_id, event_id, player_runs_details, wicket_lost_balls_market,ball_by_ball_id))
        
        # if tasks:
        #     await asyncio.gather(*tasks)

    except Exception as err:
        print("Error ", err)
        traceback.print_exc()
        return {"status": False, "msg": str(err)}