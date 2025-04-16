# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 15:29:22 2025

@author: hurai
"""

import pandas as pd
import math
import json
from score.services.database.base import get_db_session 
from score.settings.base import DATA_TEMP_BASE_PATH
from score.services.score.config import CLOSE,OPEN,SUSPEND,NOTCREATED,SETTLED,CANCEL,WIN,LOSE,BALL,OVER,INACTIVE,INPLAY,PREMATCHINPLAY,AUTO,ONLYOVER,PLAYER,WICKET,FANCYLDO, OVERSESSION, SESSION, ONLYOVERLDO,LASTDIGITNUMBER,PLAYERBOUNDARIES, PLAYERBALLSFACED, PARTNERSHIPBOUNDARIES, WICKETLOSTBALLS, DYNAMICLINE ,STATICLINE ,ODDEVEN , TOTALEVENTRUN,sio
from score.services.score.config import  LD_OVERSESSION_MARKETS, LD_SESSION_MARKETS, LD_SESSIONLDO_MARKETS, LD_LASTDIGIT_MARKETS, LD_ONLYOVER_MARKETS, LD_ONLYOVERLDO_MARKETS, LD_MARKET_TEMPLATE, LD_IPL_DATA, LD_LDO_RUNNERS, LD_PLAYER_MARKETS, LD_PLAYERBOUNDARY_MARKETS,LD_PLAYERBALLSFACED_MARKETS, LD_FOW_MARKETS, LD_DEFAULT_VALUES, LD_PARTNERSHIPBOUNDARIES_MARKETS, LD_WICKETLOSTBALLS_MARKETS,LD_OVER_BALLS, LD_ODDEVEN_MARKETS, LD_TOTALEVENTRUN_MARKETS
import os
import datetime
import time
import re
from contextlib import closing
from sqlalchemy.sql import text
import threading
from sqlalchemy.exc import SQLAlchemyError
import csv
import os
from score.services.score import common
import asyncio
import traceback
from score.services.score.player import update_player_market_status
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from multiprocessing import Pool


def load_commentary_data(commentary_id,match_type_id,event_id,is_template,ratio_data,default_ball_faced,default_player_boundaries,default_player_runs):
    set_balls_per_over(match_type_id)
    commentary_team,team_shortname=get_set_ipl_data(commentary_id,match_type_id,event_id,is_template,ratio_data)
    get_set_market_template(commentary_id, match_type_id, commentary_team)
    get_event_markets(commentary_id, commentary_team, commentary_id)
    #get_set_non_striker_markets(commentary_id,commentary_id)
    create_only_overs(commentary_id,match_type_id,commentary_team,event_id,team_shortname)
    create_ldo_lottery_markets(commentary_id,match_type_id,commentary_team,event_id,team_shortname)
    create_dynamic_markets(commentary_id,match_type_id,commentary_team,event_id)
    create_over_session_markets(commentary_id,match_type_id,commentary_team,event_id,team_shortname)
    
    get_totalrun_markets(commentary_id,commentary_team,match_type_id,event_id)
    
    get_fancy_ldo_values(commentary_id, match_type_id, commentary_team)
    calculations(commentary_id, match_type_id, commentary_team, team_shortname)
    #get_fancy_ldo_values(commentary_id, match_type_id, commentary_team)
    update_player_markets_data(commentary_id,match_type_id,commentary_team)
    set_default_values(commentary_id, commentary_team, match_type_id,default_ball_faced,default_player_boundaries,default_player_runs)
    

def set_balls_per_over(match_type_id):
    global LD_OVER_BALLS
    balls=common.get_balls_per_over(match_type_id)
    if balls>0:
        LD_OVER_BALLS[f"{match_type_id}"]=balls 
    else:
        LD_OVER_BALLS[f"{match_type_id}"]=6
    
def get_set_ipl_data(commentary_id,match_type_id,event_id,is_template,ratio_data):
    global LD_IPL_DATA
    with get_db_session() as session:
            query = text('select * from "tblMatchTypePredictors" where "wrMatchTypeId" = :match_type_id AND "wrIsDeleted"=False')
            ipl = pd.read_sql(query, con=session.bind, params={"match_type_id": match_type_id})
         
    
    commentary_team=common.get_commentary_teams(commentary_id,1)
    
    rpb_list = ipl['wrRunPerBall'].tolist() 
    cumulative_sum = 0
    cumulative_list = []
    for index,item in ipl.iterrows():
        cumulative_sum += item['wrRunPerBall']
        cumulative_list.append(cumulative_sum)
    
    
    ipl_data = pd.DataFrame({
        'Over': ipl['wrOver'],
        'Ball': ipl["wrBall"],
        'RPB': rpb_list,
        'Cumulative': cumulative_list

    })
    LD_IPL_DATA[f"{commentary_id}_{commentary_team[0]}_ipldata"]=ipl_data
    return commentary_team[0],commentary_team[1]
    
def get_set_market_template(commentary_id, match_type_id, commentary_team):
    try:
        with get_db_session() as session:
            global LD_MARKET_TEMPLATE
            # query_templates = text('SELECT * FROM "tblMarketTemplates" '
            #                    'WHERE "wrMatchTypeID" = :match_type_id '
            #                    'AND "wrMarketTypeCategoryId" IN (:onlyover,:session,:fancyldo,:onlyoverldo,:lastdigit,:player, :wicket, :player_boundaries, :balls_faced) And "wrIsActive"=True')
            # result_templates = session.execute(query_templates, {
            #     "match_type_id": match_type_id,
            #     "onlyover":ONLYOVER,
            #     "session":SESSION,
            #     "fancyldo":FANCYLDO,
            #     "onlyoverldo":ONLYOVERLDO,
            #     "lastdigit":LASTDIGITNUMBER,
            #     "player": PLAYER,
            #     "wicket": WICKET,
            #     "player_boundaries":PLAYERBOUNDARIES,
            #     "balls_faced":PLAYERBALLSFACED
            # })
            query_templates=text('SELECT tmt.* FROM "tblCommMatchTypeTemplate" tcm inner JOIN "tblMarketTemplates" tmt ON tcm."wrMarketTemplateId" = tmt."wrID" inner JOIN "tblMarketTypes" tmt1 ON tmt."wrMarketTypeId" = tmt1."wrId" inner JOIN "tblMarketTypeCategories" tmc ON tmt."wrMarketTypeCategoryId" = tmc."wrId" WHERE tcm."wrCommentaryId" = :commentary_id AND tmt."wrIsDeleted" = false and tmt."wrIsActive"=True')
            result_templates=session.execute(query_templates, {"commentary_id":commentary_id})
            market_template = pd.DataFrame(result_templates.fetchall(), columns=result_templates.keys())
        if not market_template.empty:
            LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]=market_template

    except SQLAlchemyError as e:
        print(f"An error occurred during the database operation: {e}")
    
  
def get_event_markets(commentary_id, commentary_team, match_type_id):
    """Create or update market data based on provided parameters."""    
    try:
        with get_db_session() as session:
            query_event_market = text('SELECT * FROM "tblEventMarkets" '
                                  'WHERE "wrCommentaryId" = :commentary_id '
                                  'AND "wrTeamID" = :commentary_team '
                                  'AND "wrStatus" NOT IN (:settled, :cancel) '
                                  #'AND "wrMarketTypeCategoryId" NOT IN (:player, :wicket, :player_boundaries) '
                                  'ORDER BY "wrOver" ASC')
            result_event_market = session.execute(query_event_market, {
                "commentary_id": commentary_id,
                "commentary_team": commentary_team,
                "settled": SETTLED,
                "cancel": CANCEL,
                #"player": PLAYER,
                #"wicket": WICKET,
                #"player_boundaries":PLAYERBOUNDARIES
            })
            event_market = pd.DataFrame(result_event_market.fetchall(), columns=result_event_market.keys())
            if not event_market.empty:
                category_ids = {
                    SESSION: 'LD_SESSION_MARKETS',
                    FANCYLDO: 'LD_SESSIONLDO_MARKETS',
                    LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
                    ONLYOVER: 'LD_ONLYOVER_MARKETS',
                    ONLYOVERLDO: 'LD_ONLYOVERLDO_MARKETS',
                    PLAYER: 'LD_PLAYER_MARKETS',
                    PLAYERBOUNDARIES: 'LD_PLAYERBOUNDARY_MARKETS',
                    PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
                    WICKET: 'LD_FOW_MARKETS',
                    PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
                    WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS',
                    ODDEVEN:'LD_ODDEVEN_MARKETS'

                }
    
                for category_id, global_var_name in category_ids.items():
                    filtered_data = event_market[event_market["wrMarketTypeCategoryId"] == category_id]
                    print(filtered_data["wrMarketName"])
                    if not filtered_data.empty:
                        if(category_id in (PLAYER,PLAYERBOUNDARIES, PLAYERBALLSFACED, WICKET, PARTNERSHIPBOUNDARIES, WICKETLOSTBALLS)):
                            filtered_data = filtered_data.assign(
                                player_score=0,
                                suspend_over=0,
                                close_over=0
                                
                            )
                        globals()[global_var_name][f"{commentary_id}_{commentary_team}"] = filtered_data
    except SQLAlchemyError as e:
        print(f"An error occurred during the database operation: {e}")


def get_set_non_striker_markets(commentary_id, match_type_id):
    global LD_SESSION_MARKETS
    non_striker_team=common.get_commentary_teams(commentary_id,2)
    non_striker_team_id=non_striker_team[0]
    try:
        with get_db_session() as session:
            query_event_market = text('SELECT * FROM "tblEventMarkets" '
                                  'WHERE "wrCommentaryId" = :commentary_id '
                                  'AND "wrTeamID" = :commentary_team '
                                  'AND "wrStatus" NOT IN (:cancel) '
                                  'And "wrIsInningRun"=True '
                                  #'AND "wrMarketTypeCategoryId" NOT IN (:player, :wicket, :player_boundaries) '
                                  'ORDER BY "wrOver" ASC')
            result_event_market = session.execute(query_event_market, {
                "commentary_id": commentary_id,
                "commentary_team": non_striker_team_id,
                #"settled": SETTLED,
                "cancel": CANCEL
            })
            event_market = pd.DataFrame(result_event_market.fetchall(), columns=result_event_market.keys())
            print("fetched event market is ", event_market)
            if not event_market.empty:
                LD_SESSION_MARKETS[f"{commentary_id}_{non_striker_team_id}"] =event_market
                print("the non striker session markets stored are ", LD_SESSION_MARKETS[f"{commentary_id}_{non_striker_team_id}"])
    except SQLAlchemyError as e:
        print(f"An error occurred during the database operation: {e}")

def get_totalrun_markets(commentary_id,commentary_team,match_type_id,event_id):
    global LD_MARKET_TEMPLATE, LD_TOTALEVENTRUN_MARKETS
    market_template=LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]
    if TOTALEVENTRUN in market_template["wrMarketTypeCategoryId"].values:
        try:
            with get_db_session() as session:
                query_event_market = text('SELECT * FROM "tblEventMarkets" '
                                      'WHERE "wrCommentaryId" = :commentary_id '
                                      'AND "wrMarketTypeCategoryId" = :TOTALEVENTRUN '
                                      'ORDER BY "wrOver" ASC')
                result_event_market = session.execute(query_event_market, {
                    "commentary_id": commentary_id,
                    "TOTALEVENTRUN": TOTALEVENTRUN,
                   
                })
                event_market = pd.DataFrame(result_event_market.fetchall(), columns=result_event_market.keys())
                if not event_market.empty:
                    event_market = event_market.assign(
                        predefined_value=0,
                        onlyovers=0,
                        maxovers=common.get_max_overs(commentary_id),
                        minodds=0,
                        maxodds=0,
                        openvalue=0,
                        overBalls=0,
                        line=0,
                        overrate=0,
                        underrate=0,
                        backprice=0,
                        layprice=0,
                        laysize=0, 
                        backsize=0,
                        ballbyballid=0
                    )
                    LD_TOTALEVENTRUN_MARKETS[f"{commentary_id}"]=event_market
                # else:
                #     filtered_data = market_template[market_template["wrMarketTypeCategoryId"] == TOTALEVENTRUN]
                #     current_innings=common.get_current_innings(commentary_id)
                #     rows_to_append=[]
                #     for index, row in filtered_data.iterrows():
                #             new_row = {}
                #             for column in filtered_data.columns:
                #                 if column in event_market.columns:
                #                     new_row[column] = row[column]
                #                 new_row["wrCommentaryId"]=commentary_id
                #                 new_row["wrEventRefID"]=event_id
                #                 new_row["wrTeamID"]=None
                #                 new_row["wrMarketName"]=row["wrTemplateName"]
                #                 new_row["wrStatus"]=INACTIVE
                #                 new_row["wrPredefinedValue"]=0
                #                 new_row["wrMarketTemplateId"]=row["wrID"]
                #                 new_row["wrCreate"]=row["wrCreate"]
                #                 new_row["wrMargin"]=row["wrMargin"]
                #                 new_row["wrMarketTemplateId"]=row["wrID"]
                #                 new_row["wrInningsID"] = current_innings
                #                 new_row["wrCreateType"]=AUTO
                #                 new_row["wrMarketTypeCategoryId"]=row["wrMarketTypeCategoryId"]
                #                 new_row["wrMarketTypeId"]=row["wrMarketTypeId"]
                #                 #new_row["maxovers"]=max_overs
                #                 new_row["wrDelay"]=row["wrDelay"]
                #                 new_row["wrCreate"]=row["wrCreate"]
                #                 new_row["wrCreateRefId"]=row["wrCreateRefId"]
                #                 new_row["wrOpenRefId"]=row["wrOpenRefId"]
                #                 new_row["wrActionType"]=row["wrActionType"]
                #                 new_row["wrAutoResultType"]=row["wrAutoResultType"]
                #                 new_row['wrIsActive']=row['wrIsDefaultMarketActive']
                #                 new_row['wrIsAllow']=row['wrIsDefaultBetAllowed']
                #                 new_row["wrIsSendData"]=True if bool(row["wrIsPredefineRunnerValue"]) else False
                                
                              
                #             rows_to_append.append(new_row)
                     
                #     if rows_to_append:
                #         event_market = pd.concat([event_market, pd.DataFrame(rows_to_append)], ignore_index=True)
                #     if not event_market.empty:
                #         event_market=event_market.fillna(0)
                #         event_market["wrIsAllow"]=1
                #         event_market["wrRateSource"]=1
                #         event_market = event_market.assign(
                #             predefined_value=0,
                #             onlyovers=0,
                #             maxovers=common.get_max_overs(commentary_id),
                #             minodds=0,
                #             maxodds=0,
                #             openvalue=0,
                #             overBalls=0,
                #             line=0,
                #             overrate=0,
                #             underrate=0,
                #             backprice=0,
                #             layprice=0,
                #             laysize=0, 
                #             backsize=0,
                #             ballbyballid=0
                #         )
                #         event_market["wrOver"] = pd.to_numeric(event_market["wrOver"], errors='coerce')
    
                #         event_market = event_market.reset_index(drop=True)
                #     LD_TOTALEVENTRUN_MARKETS[f"{commentary_id}"]=event_market
                
                    get_set_non_striker_markets(commentary_id, match_type_id)
        
        except SQLAlchemyError as e:
            print(f"An error occurred during the database operation: {e}")
        

def create_only_overs(commentary_id,match_type_id,commentary_team,event_id,team_shortname):
    global LD_MARKET_TEMPLATE
    
    category_ids = {
        ONLYOVERLDO: 'LD_ONLYOVERLDO_MARKETS',
        ONLYOVER: 'LD_ONLYOVER_MARKETS'
    }
    
    
    market_template=LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]
    max_overs=common.get_max_overs(commentary_id)

    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key = f"{commentary_id}_{commentary_team}"
        column_names=common.get_column_names("tblEventMarkets")
        if key not in global_dict:
            event_market=pd.DataFrame(columns=column_names)
        else:
            event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
     
        filtered_template=market_template[market_template['wrMarketTypeCategoryId'] == category_id]
        print("filtere data is ", filtered_template)
           
        if not filtered_template.empty:
            only_over_template = filtered_template.iloc[-1]
        else:
            print(f"No data found for category_id {category_id}")
            continue
        only_over_template["wrBeforeAutoClose"] = pd.to_numeric(only_over_template["wrBeforeAutoClose"], errors='coerce')
        only_over_template["wrBeforeAutoSuspend"] = pd.to_numeric(only_over_template["wrBeforeAutoSuspend"], errors='coerce')
        only_over_template["wrCreate"] = pd.to_numeric(only_over_template["wrCreate"], errors='coerce')
        only_over_template["wrAutoOpen"] = pd.to_numeric(only_over_template["wrAutoOpen"], errors='coerce')
        only_over_template["wrOver"] = pd.to_numeric(only_over_template["wrOver"], errors='coerce')

        start_over = only_over_template["wrOver"]
        result_df = pd.DataFrame()
        template_name=only_over_template["wrTemplateName"] + " - "+team_shortname
        diff=only_over_template["wrOver"]
        autoclose=only_over_template["wrBeforeAutoClose"]
        autosuspend=only_over_template["wrBeforeAutoSuspend"]
        autocreate=only_over_template["wrCreate"]
        autoopen=only_over_template["wrAutoOpen"]
        for over in range(start_over, max_overs + 1):
            only_over_template["wrBeforeAutoClose"] =  (over-diff) * 6 +autoclose+6
            only_over_template["wrBeforeAutoSuspend"] =  (over-diff) * 6+autosuspend+6
            only_over_template["wrCreate"] =  (over-diff) * 6 +autocreate
            only_over_template["wrAutoOpen"] =  (over-diff) * 6+autoopen
            only_over_template["wrOver"]=over
            only_over_template["wrTemplateName"]= template_name.replace("{x}", str(over))
            print(only_over_template["wrCreate"])
            result_df = pd.concat([result_df, pd.DataFrame([only_over_template])], ignore_index=True)
        rows_to_append = []
        current_innings = common.get_current_innings(commentary_id)
        
        for index, row in result_df.iterrows():
            # Check if the row should be appended
            new_row = {column: row[column] for column in column_names if column in result_df.columns}
            
            # Add additional fields
            new_row.update({
                "wrCommentaryId": commentary_id,
                "wrEventRefID": event_id,
                "wrTeamID": commentary_team,
                "wrMarketName": row["wrTemplateName"],
                "wrStatus": INACTIVE,
                "wrPredefinedValue": 0,
                "wrMarketTemplateId": row["wrID"],
                "wrInningsID": current_innings,
                "wrCreateType": AUTO,
                "wrMarketTypeCategoryId": row["wrMarketTypeCategoryId"],
                "wrMarketTypeId": row["wrMarketTypeId"],
                "maxovers": max_overs,
                "wrDelay": row["wrDelay"],
                "wrCreate": row["wrCreate"],
                "wrCreateRefId": row["wrCreateRefId"],
                "wrOpenRefId": row["wrOpenRefId"],
                "wrActionType": row["wrActionType"],
                "wrAutoResultType": row["wrAutoResultType"],
                "wrIsActive": row["wrIsDefaultMarketActive"],
                "wrIsAllow": row["wrIsDefaultBetAllowed"],
                "wrIsSendData": True if bool(row["wrIsPredefineRunnerValue"]) else False,
                "wrLineRatio": 1
            })
            rows_to_append.append(new_row)
    
        # Convert to DataFrame
        if rows_to_append:
            all_only_over_markets = pd.DataFrame(rows_to_append).fillna(0)
            #all_only_over_markets["wrIsAllow"]=1
            all_only_over_markets["wrRateSource"]=1
            for col in event_market.columns:
                if col not in all_only_over_markets.columns:
                    all_only_over_markets[col] = 0
            # Add additional fields
            all_only_over_markets = all_only_over_markets.assign(
                predefined_value=0,
                onlyovers=0,
                maxovers=common.get_max_overs(commentary_id),
                minodds=0,
                maxodds=0,
                openvalue=0,
                overBalls=0,
                line=0,
                overrate=0,
                underrate=0,
                backprice=0,
                layprice=0,
                laysize=0, 
                backsize=0,
                ballbyballid=0
            )
        
            # Sort and print final length
            if not event_market.empty:
                for index, row in event_market.iterrows():
                    # Find the corresponding rows in the new DataFrame
                    matching_indices = all_only_over_markets[all_only_over_markets["wrOver"] == row["wrOver"]].index
                    
                    # Update all matching rows with the existing row's values
                    for idx in matching_indices:
                        for column in all_only_over_markets.columns:
                            if column in row:
                                value = row[column]
                                
                                # Convert Timestamp to string if it's a datetime object
                                if isinstance(value, pd.Timestamp):
                                    print(value)
                                    if pd.isna(value):  # Check for NaT
                                        value = None  # You can also choose to set it to a default string if needed
                                    else:
                                        value = value.strftime("%Y-%m-%d %H:%M:%S.%f+00")  # Convert Timestamp to string

                                
                                all_only_over_markets.at[idx, column] = value
                        print(all_only_over_markets["wrStatus"])
            all_only_over_markets = all_only_over_markets.sort_values(by="wrOver")
            print("The length of only over is:", len(all_only_over_markets))


            globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=all_only_over_markets


def create_ldo_lottery_markets(commentary_id,match_type_id,commentary_team,event_id,team_shortname):
    global LD_MARKET_TEMPLATE
    
    category_ids = {
        FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
        ODDEVEN:'LD_ODDEVEN_MARKETS'
    }
    
    
    market_template=LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]
    max_overs=common.get_max_overs(commentary_id)

    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key = f"{commentary_id}_{commentary_team}"
        column_names=common.get_column_names("tblEventMarkets")
        if key not in global_dict:
            event_market=pd.DataFrame(columns=column_names)
        else:
            event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
     
        filtered_template=market_template[market_template['wrMarketTypeCategoryId'] == category_id]
        print("filtere data is ", filtered_template)
           
        if not filtered_template.empty:
            template = filtered_template.iloc[-1]
        else:
            print(f"No data found for category_id {category_id}")
            continue
        template["wrBeforeAutoClose"] = pd.to_numeric(template["wrBeforeAutoClose"], errors='coerce')
        template["wrBeforeAutoSuspend"] = pd.to_numeric(template["wrBeforeAutoSuspend"], errors='coerce')
        template["wrCreate"] = pd.to_numeric(template["wrCreate"], errors='coerce')
        template["wrAutoOpen"] = pd.to_numeric(template["wrAutoOpen"], errors='coerce')
        template["wrOver"] = pd.to_numeric(template["wrOver"], errors='coerce')

        start_over = template["wrOver"]
        result_df = pd.DataFrame()
        template_name=template["wrTemplateName"] + " - "+team_shortname
        diff=template["wrOver"]
        autoclose=template["wrBeforeAutoClose"]
        autosuspend=template["wrBeforeAutoSuspend"]
        autocreate=template["wrCreate"]
        autoopen=template["wrAutoOpen"]
        notincludedover =[]
        how_many_open_markets=int(template["wrHowManyOpenMarkets"])
        nextopen=0.0
        nextcreate=0.0
        no_of_markets_created=0
        nextaddmarket=0
        
        if template["wrNotIncludedOver"] is not None:
            
            notincludedover = [int(x) for x in template["wrNotIncludedOver"].split(',')]
        for over in range(start_over, max_overs + 1):
            if over not in notincludedover:
                #template["wrBeforeAutoClose"] =  common.balls_to_overs(((over-diff) * 6 +autoclose),match_type_id)
                #template["wrBeforeAutoSuspend"] =  common.balls_to_overs(((over-diff) * 6+autosuspend),match_type_id)
                template["wrBeforeAutoClose"]= common.balls_to_overs((over*6-autoclose),match_type_id)
                template["wrBeforeAutoSuspend"]= common.balls_to_overs((over*6-autosuspend),match_type_id)
                
                #template["wrCreate"] =  common.balls_to_overs(((over-diff) * 6 +autocreate-6),match_type_id)
                #template["wrAutoOpen"] =  common.balls_to_overs(((over-diff) * 6+autoopen-6),match_type_id)
                template["wrOver"]=over
                template["wrTemplateName"]= template_name.replace("{x}", str(over))
                
                if how_many_open_markets==1:
                    template["wrCreate"]=common.balls_to_overs(((over-diff) * 6 +autocreate-6),match_type_id)
                    template["wrAutoOpen"]=common.balls_to_overs(((over-diff) * 6+autoopen-6),match_type_id)
                    
                else:
                    
                    template["wrCreate"] =  nextcreate
                    template["wrAutoOpen"] =  nextopen
                    no_of_markets_created+=1
       
                    if no_of_markets_created==how_many_open_markets:
                        no_of_markets_created-=1

                        nextcreate=int(nextaddmarket)+(autocreate/10)
                        nextopen=int(nextaddmarket)+(autoopen/10)
                        nextaddmarket+=1
                    
                
            result_df = pd.concat([result_df, pd.DataFrame([template])], ignore_index=True)
        rows_to_append = []
        current_innings = common.get_current_innings(commentary_id)
        
        for index, row in result_df.iterrows():
            # Check if the row should be appended
            new_row = {column: row[column] for column in column_names if column in result_df.columns}
            
            # Add additional fields
            new_row.update({
                "wrCommentaryId": commentary_id,
                "wrEventRefID": event_id,
                "wrTeamID": commentary_team,
                "wrMarketName": row["wrTemplateName"],
                "wrStatus": INACTIVE,
                "wrPredefinedValue": 0,
                "wrMarketTemplateId": row["wrID"],
                "wrInningsID": current_innings,
                "wrCreateType": AUTO,
                "wrMarketTypeCategoryId": row["wrMarketTypeCategoryId"],
                "wrMarketTypeId": row["wrMarketTypeId"],
                "maxovers": max_overs,
                "wrDelay": row["wrDelay"],
                "wrCreate": row["wrCreate"],
                "wrCreateRefId": row["wrCreateRefId"],
                "wrOpenRefId": row["wrOpenRefId"],
                "wrActionType": row["wrActionType"],
                "wrAutoResultType": row["wrAutoResultType"],
                "wrIsActive": row["wrIsDefaultMarketActive"],
                "wrIsAllow": row["wrIsDefaultBetAllowed"],
                "wrIsSendData": True if bool(row["wrIsPredefineRunnerValue"]) else False,
                "wrLineRatio": 1
            })
            rows_to_append.append(new_row)
    
        # Convert to DataFrame
        if rows_to_append:
            all_event_markets = pd.DataFrame(rows_to_append).fillna(0)
            #all_only_over_markets["wrIsAllow"]=1
            all_event_markets["wrRateSource"]=1
            for col in event_market.columns:
                if col not in all_event_markets.columns:
                    all_event_markets[col] = 0
            # Add additional fields
            all_event_markets = all_event_markets.assign(
                predefined_value=0,
                onlyovers=0,
                maxovers=common.get_max_overs(commentary_id),
                minodds=0,
                maxodds=0,
                openvalue=0,
                overBalls=0,
                line=0,
                overrate=0,
                underrate=0,
                backprice=0,
                layprice=0,
                laysize=0, 
                backsize=0,
                ballbyballid=0
            )
        
            # Sort and print final length
            if not event_market.empty:
                for index, row in event_market.iterrows():
                    # Find the corresponding rows in the new DataFrame
                    matching_indices = all_event_markets[all_event_markets["wrOver"] == row["wrOver"]].index
                    
                    # Update all matching rows with the existing row's values
                    for idx in matching_indices:
                        for column in all_event_markets.columns:
                            if column in row:
                                value = row[column]
                                
                                # Convert Timestamp to string if it's a datetime object
                                if isinstance(value, pd.Timestamp):
                                    print(value)
                                    if pd.isna(value):  # Check for NaT
                                        value = None  # You can also choose to set it to a default string if needed
                                    else:
                                        value = value.strftime("%Y-%m-%d %H:%M:%S.%f+00")  # Convert Timestamp to string

                                
                                all_event_markets.at[idx, column] = value
            all_event_markets = all_event_markets.sort_values(by="wrOver")


            globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=all_event_markets


def create_dynamic_markets(commentary_id,match_type_id,commentary_team,event_id):
    global LD_MARKET_TEMPLATE
    fancy_template=LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]
    max_overs=common.get_max_overs(commentary_id)
    current_innings=common.get_current_innings(commentary_id)
    
    category_ids = {
        SESSION: 'LD_SESSION_MARKETS'
        #FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        #LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS'
    }
    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key = f"{commentary_id}_{commentary_team}"
        column_names=common.get_column_names("tblEventMarkets")
        rows_to_append = []
        if key not in global_dict:
            event_market=pd.DataFrame(columns=column_names)
        else:
            event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
        for index, row in fancy_template.iterrows():
            #if not row["wrIsPredefineMarket"] and row["wrTemplateType"] in (INPLAY,PREMATCHINPLAY) and row["wrMarketTypeCategoryId"] not in (PLAYER,WICKET, PLAYERBOUNDARIES) and ((row["wrMarketTypeCategoryId"]==SESSION and row["wrID"] not in event_market["wrMarketTemplateId"].values) or (row["wrMarketTypeCategoryId"] in (FANCYLDO, LASTDIGITNUMBER)and row["wrID"] not in event_market["wrMarketTemplateId"].values) ):
            if not row["wrIsPredefineMarket"] and row["wrTemplateType"] in (INPLAY,PREMATCHINPLAY) and category_id==row["wrMarketTypeCategoryId"] and row["wrID"] not in event_market["wrMarketTemplateId"].values:
                new_row = {}
                for column in fancy_template.columns:
                    if column in event_market.columns:
                        new_row[column] = row[column]
                    new_row["wrCommentaryId"]=commentary_id
                    new_row["wrEventRefID"]=event_id
                    new_row["wrTeamID"]=commentary_team
                    new_row["wrMarketName"]=row["wrTemplateName"]
                    new_row["wrStatus"]=INACTIVE
                    new_row["wrPredefinedValue"]=0
                    new_row["wrMarketTemplateId"]=row["wrID"]
                    new_row["wrCreate"]=row["wrCreate"]
                    new_row["wrMargin"]=row["wrMargin"]
                    new_row["wrMarketTemplateId"]=row["wrID"]
                    new_row["wrInningsID"] = current_innings
                    new_row["wrCreateType"]=AUTO
                    new_row["wrMarketTypeCategoryId"]=row["wrMarketTypeCategoryId"]
                    new_row["wrMarketTypeId"]=row["wrMarketTypeId"]
                    new_row["maxovers"]=max_overs
                    new_row["wrDelay"]=row["wrDelay"]
                    new_row["wrCreate"]=row["wrCreate"]
                    new_row["wrCreateRefId"]=row["wrCreateRefId"]
                    new_row["wrOpenRefId"]=row["wrOpenRefId"]
                    new_row["wrActionType"]=row["wrActionType"]
                    new_row["wrAutoResultType"]=row["wrAutoResultType"]
                    new_row['wrIsActive']=row['wrIsDefaultMarketActive']
                    new_row['wrIsAllow']=row['wrIsDefaultBetAllowed']
                    new_row["wrIsSendData"]=True if bool(row["wrIsPredefineRunnerValue"]) else False
                    
                  
                rows_to_append.append(new_row)
         
        if rows_to_append:
            event_market = pd.concat([event_market, pd.DataFrame(rows_to_append)], ignore_index=True)
        if not event_market.empty:
            event_market=event_market.fillna(0)
            event_market["wrIsAllow"]=1
            event_market["wrRateSource"]=1
            event_market = event_market.assign(
                predefined_value=0,
                onlyovers=0,
                maxovers=common.get_max_overs(commentary_id),
                minodds=0,
                maxodds=0,
                openvalue=0,
                overBalls=0,
                line=0,
                overrate=0,
                underrate=0,
                backprice=0,
                layprice=0,
                laysize=0, 
                backsize=0,
                ballbyballid=0
            )
            event_market["wrOver"] = pd.to_numeric(event_market["wrOver"], errors='coerce')

            event_market=event_market.sort_values(by="wrOver")
            event_market = event_market.reset_index(drop=True)

            print("the markets are ", event_market)
            print(category_id)
        globals()[global_var_name][f"{commentary_id}_{commentary_team}"] =event_market
     
    
def create_over_session_markets(commentary_id,match_type_id,commentary_team,event_id,team_shortname):
    global LD_MARKET_TEMPLATE, LD_SESSION_MARKETS, LD_OVERSESSION_MARKETS
    
    
    
    
    market_template=LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]
    max_overs=common.get_max_overs(commentary_id)
    
    
    
    key = f"{commentary_id}_{commentary_team}"
    column_names=common.get_column_names("tblEventMarkets")
    if key not in LD_OVERSESSION_MARKETS:
        event_market=pd.DataFrame(columns=column_names)
    else:
        event_market=LD_OVERSESSION_MARKETS[f"{commentary_id}_{commentary_team}"] 
 
    filtered_template=market_template[market_template['wrMarketTypeCategoryId'] == OVERSESSION]
    print("filtere data is ", filtered_template)
       
    if not filtered_template.empty:
        over_session_template = filtered_template.iloc[-1]
        
        balls_per_over=common.get_balls_per_over(match_type_id)
        over_session_template["wrBeforeAutoClose"] = pd.to_numeric(over_session_template["wrBeforeAutoClose"], errors='coerce')
        over_session_template["wrBeforeAutoSuspend"] = pd.to_numeric(over_session_template["wrBeforeAutoSuspend"], errors='coerce')
        over_session_template["wrCreate"] = pd.to_numeric(over_session_template["wrCreate"], errors='coerce')
        over_session_template["wrAutoOpen"] = pd.to_numeric(over_session_template["wrAutoOpen"], errors='coerce')
        over_session_template["wrOver"] = pd.to_numeric(over_session_template["wrOver"], errors='coerce')
        
        result_df = pd.DataFrame()
        template_name=over_session_template["wrTemplateName"]
        autoclose=over_session_template["wrBeforeAutoClose"]
        autosuspend=over_session_template["wrBeforeAutoSuspend"]
        autocreate=over_session_template["wrCreate"]
        autoopen=over_session_template["wrAutoOpen"]
        how_many_open_markets=int(over_session_template["wrHowManyOpenMarkets"])
        nextopen=autoopen/10
        nextcreate=autocreate/10
        no_of_markets_created=0
        nextaddmarket=1
        notincludedover =[]
        if over_session_template["wrNotIncludedOver"] is not None:
            
            notincludedover = [int(x) for x in over_session_template["wrNotIncludedOver"].split(',')]
        for over in range(1, max_overs + 1):
            if over not in notincludedover:
                over_session_template["wrOver"]=over
                #if (key in LD_SESSION_MARKETS and not LD_SESSION_MARKETS[key].empty and over in LD_SESSION_MARKETS[key]["wrOver"].values):
                    #name=template_name+' (2)' + " - "+team_shortname
                    #over_session_template["wrTemplateName"]= name.replace("{x}", str(over))
                #else:
                name=template_name +" - "+team_shortname
                over_session_template["wrTemplateName"]= name.replace("{x}", str(over))
    
                
                close_balls=((over-1)*balls_per_over)+autoclose
                over_session_template["wrBeforeAutoClose"] =  int(close_balls/6)+(close_balls%6)/10
                suspend_balls=((over-1)*balls_per_over)+autosuspend
                over_session_template["wrBeforeAutoSuspend"] =  int(suspend_balls/6)+(suspend_balls%6)/10
                
                over_session_template["wrCreate"] =  nextcreate
                over_session_template["wrAutoOpen"] =  nextopen
                no_of_markets_created+=1
                if no_of_markets_created==how_many_open_markets:
                    no_of_markets_created=1
                    #create_balls=((over-how_many_open_markets)*balls_per_over)+autocreate
                    #nextcreate=int(create_balls/6)+(create_balls%6)/10
                    #open_balls=((over-how_many_open_markets)*balls_per_over)+autoopen
                    #nextopen=int(open_balls/6)+(open_balls%6)/10
                    
                    nextcreate=int(nextaddmarket)+(autocreate/10)
                    nextopen=int(nextaddmarket)+(autoopen/10)
                    nextaddmarket+=1
                result_df = pd.concat([result_df, pd.DataFrame([over_session_template])], ignore_index=True)

            else:
                nextaddmarket+=1
                
        print("the markets over sesion are ",result_df["wrCreate"])
        rows_to_append = []
        current_innings = common.get_current_innings(commentary_id)
        
        for index, row in result_df.iterrows():
            # Check if the row should be appended
            new_row = {column: row[column] for column in column_names if column in result_df.columns}
            
            # Add additional fields
            new_row.update({
                "wrCommentaryId": commentary_id,
                "wrEventRefID": event_id,
                "wrTeamID": commentary_team,
                "wrMarketName": row["wrTemplateName"],
                "wrStatus": INACTIVE,
                "wrPredefinedValue": 0,
                "wrMarketTemplateId": row["wrID"],
                "wrInningsID": current_innings,
                "wrCreateType": AUTO,
                "wrMarketTypeCategoryId": row["wrMarketTypeCategoryId"],
                "wrMarketTypeId": row["wrMarketTypeId"],
                "maxovers": max_overs,
                "wrDelay": row["wrDelay"],
                "wrCreate": row["wrCreate"],
                "wrCreateRefId": row["wrCreateRefId"],
                "wrOpenRefId": row["wrOpenRefId"],
                "wrActionType": row["wrActionType"],
                "wrAutoResultType": row["wrAutoResultType"],
                "wrIsActive": row["wrIsDefaultMarketActive"],
                "wrIsAllow": row["wrIsDefaultBetAllowed"],
                "wrIsSendData": True if bool(row["wrIsPredefineRunnerValue"]) else False,
                "wrLineRatio": 1
            })
            rows_to_append.append(new_row)
    
        # Convert to DataFrame
        if rows_to_append:
            all_over_session_markets = pd.DataFrame(rows_to_append).fillna(0)
            #all_only_over_markets["wrIsAllow"]=1
            all_over_session_markets["wrRateSource"]=1
            for col in event_market.columns:
                if col not in all_over_session_markets.columns:
                    all_over_session_markets[col] = 0
            # Add additional fields
            all_over_session_markets = all_over_session_markets.assign(
                predefined_value=0,
                onlyovers=0,
                maxovers=common.get_max_overs(commentary_id),
                minodds=0,
                maxodds=0,
                openvalue=0,
                overBalls=0,
                line=0,
                overrate=0,
                underrate=0,
                backprice=0,
                layprice=0,
                laysize=0, 
                backsize=0,
                ballbyballid=0
            )
        
            # Sort and print final length
            if not event_market.empty:
                for index, row in event_market.iterrows():
                    # Find the corresponding rows in the new DataFrame
                    matching_indices = all_over_session_markets[all_over_session_markets["wrOver"] == row["wrOver"]].index
                    
                    # Update all matching rows with the existing row's values
                    for idx in matching_indices:
                        for column in all_over_session_markets.columns:
                            if column in row:
                                value = row[column]
                                
                                # Convert Timestamp to string if it's a datetime object
                                if isinstance(value, pd.Timestamp):
                                    print(value)
                                    if pd.isna(value):  # Check for NaT
                                        value = None  # You can also choose to set it to a default string if needed
                                    else:
                                        value = value.strftime("%Y-%m-%d %H:%M:%S.%f+00")  # Convert Timestamp to string
    
                                
                                all_over_session_markets.at[idx, column] = value
                        print(all_over_session_markets["wrStatus"])
            all_over_session_markets = all_over_session_markets.sort_values(by="wrOver")
            print("The length of only over is:", len(all_over_session_markets))
    
            LD_OVERSESSION_MARKETS[key]=all_over_session_markets
    
    
def update_player_markets_data(commentary_id,match_type_id,commentary_team):
    category_ids = {
        PLAYER: 'LD_PLAYER_MARKETS',
        PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
        PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
        WICKET:'LD_FOW_MARKETS',
        PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
        WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS'

    }
    with get_db_session() as session:

        query=text('SELECT b."wrOverCount", w."wrBatterId" FROM "tblCommentaryWickets"  w inner JOIN "tblCommentaryBallByBalls" b ON w."wrCommentaryBallByBallId"=b."wrCommentaryBallByBallId" WHERE w."wrCommentaryId"=:commentary_id')
        wickets = pd.read_sql_query(query, session.bind, params={"commentary_id":commentary_id})
        
    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key = f"{commentary_id}_{commentary_team}"
        
        if key in global_dict:
            event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
            for index,row in event_market.iterrows():
                if(row["wrStatus"] in (CLOSE,SETTLED)):
                    score=common.markets_result_settlement(row,commentary_id,commentary_team,row["wrWicketNo"])
                    if score is None:
                        score=0
                    row["player_score"]=score
                if(row["wrPlayerID"] in wickets["wrBatterId"].values and row["wrMarketTypeCategoryId"] not in (WICKET,PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS)):
                    print("the player id is ",row["wrPlayerID"])
                    filtered_wickets = wickets[wickets["wrBatterId"] == row["wrPlayerID"]]
                    if not filtered_wickets.empty:
                        row["suspend_over"]=wickets[wickets["wrBatterId"]==row["wrPlayerID"]]["wrOverCount"].iloc[0]
                    else:
                        row["suspend_over"] = 0
                    if(row["wrStatus"]==CLOSE):
                        suspendballs=common.overs_to_balls(row["suspend_over"],match_type_id)+2
                        row["close_over"]=int(suspendballs/6)+(suspendballs%6)/10
                        
                elif (row["wrMarketTypeCategoryId"] in (WICKET,PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS)):    
                    if(row["wrStatus"]==SUSPEND):
                        if not wickets.empty and "wrOverCount" in wickets.columns and len(wickets) > 0:
                            row["suspend_over"]=wickets["wrOverCount"].iloc[-1] 
                    elif(row["wrStatus"]==CLOSE):
                        suspendballs=(common.overs_to_balls(row["suspend_over"],match_type_id)+2) if row["suspend_over"]>0 else 0
                        row["close_over"]=int(suspendballs/6)+(suspendballs%6)/10
                        
                event_market.loc[index] = row
            
            globals()[global_var_name][f"{commentary_id}_{commentary_team}"] =event_market
            print("load commentary event markets are ",event_market)
    
    
def calculations(commentary_id, match_type_id, commentary_team, team_shortname):
    global LD_IPL_DATA, LD_LDO_RUNNERS
    category_ids = {
        SESSION: 'LD_SESSION_MARKETS',
        FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
        ONLYOVER:'LD_ONLYOVER_MARKETS',
        ONLYOVERLDO:'LD_ONLYOVERLDO_MARKETS',
        OVERSESSION:'LD_OVERSESSION_MARKETS',
        ODDEVEN:'LD_ODDEVEN_MARKETS',
        TOTALEVENTRUN:'LD_TOTALEVENTRUN_MARKETS'
    }
    ipl_data=LD_IPL_DATA[f"{commentary_id}_{commentary_team}_ipldata"]
    with get_db_session() as session:
        query = text('SELECT "wrID", "wrData" FROM "tblEventMarkets" WHERE "wrCommentaryId"=:commentary_id AND "wrTeamID"=:commentary_team AND "wrMarketTypeCategoryId" NOT IN (:PLAYER, :WICKET, :PLAYERBOUNDARIES, :PARTNERSHIPBOUNDARIES, :WICKETLOSTBALLS)')
        wrdata = pd.read_sql_query(query, session.bind, 
                                                 params={"commentary_id":commentary_id, "commentary_team":commentary_team,"PLAYER":PLAYER, "WICKET":WICKET, "PLAYERBOUNDARIES": PLAYERBOUNDARIES,"PARTNERSHIPBOUNDARIES":PARTNERSHIPBOUNDARIES, "WICKETLOSTBALLS":WICKETLOSTBALLS})
    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key = f"{commentary_id}_{commentary_team}"
        total_event_run_key=f"{commentary_id}"
        event_market=pd.DataFrame()
        column_names=common.get_column_names("tblEventMarkets")
        if ((category_id!=TOTALEVENTRUN and key in global_dict) or (category_id==TOTALEVENTRUN and total_event_run_key in global_dict)):
            if category_id!=TOTALEVENTRUN:
                event_market=globals()[global_var_name][key] 
            else:
                event_market=globals()[global_var_name][total_event_run_key] 
            for index,row in event_market.iterrows():
                #print(row["wrMarketName"]+ str(row["wrLineRatio"]))
                filtered_df = wrdata[wrdata['wrID'] == row["wrID"]]
                if(len(filtered_df)>0):
                    row["wrData"]=filtered_df["wrData"].iloc[-1]
                if row["wrLineRatio"] == 0:
                    row["wrLineRatio"] = 1
                    event_market.loc[index] = row
                    
                #if row["wrMarketTypeCategoryId"] in (ONLYOVER, ONLYOVERLDO):
                #    name_split = row["wrMarketName"].split(' ')
                #    if row["wrMarketTypeCategoryId"] != FANCYLDO:
                #        if name_split[1] != '{x}':
                #            row["wrOver"] = int(name_split[1])
                        #else:
                        #    row["wrMarketName"] += f" - {team_shortname}"
                
                if row["wrMarketTypeCategoryId"] == SESSION and not row["wrIsPredefineMarket"]:
                    words = row["wrMarketName"].split()
                    if team_shortname not in words:
                        row["wrMarketName"] += f" - {team_shortname}"
                
                event_market.loc[index] = row
                if row["wrMarketTypeCategoryId"] not in (PLAYER, WICKET, PLAYERBOUNDARIES, ONLYOVER,ONLYOVERLDO, PARTNERSHIPBOUNDARIES,WICKETLOSTBALLS):

                    event_market.at[index, "beforeAutoCloseBalls"] = common.overs_to_balls(float(row["wrBeforeAutoClose"]), match_type_id)
                    event_market.at[index, "beforeAutoSuspendBalls"] = common.overs_to_balls(float(row["wrBeforeAutoSuspend"]), match_type_id)
                    event_market.at[index, "autoOpenBalls"] = common.overs_to_balls(float(row["wrAutoOpen"]), match_type_id)
                    event_market.at[index, 'overBalls'] = common.overs_to_balls(float(row['wrOver']), match_type_id)
                    event_market.at[index, 'createBalls'] = common.overs_to_balls(float(row['wrCreate']), match_type_id)
                    row = event_market.loc[index]
                    if row["wrMarketTypeCategoryId"] not in (ONLYOVER,ONLYOVERLDO) and row["wrOpenTime"] not in (None, '0', 0):
                        row["wrOpenTime"] = str(row["wrOpenTime"])
                        row["wrOpenTime"] = re.sub(r':\d{2}$', '', row["wrOpenTime"])
                        event_market.loc[index] = row
                    total_balls = row["overBalls"]
                    if row["wrIsPredefineMarket"] and row["wrMarketTypeCategoryId"]!= TOTALEVENTRUN:
                        if row["wrPredefinedValue"] == 0:
                            row["wrPredefinedValue"] = ipl_data.at[total_balls - 1, 'Cumulative']
                            event_market.loc[index] = row
                    else:
                        if row["wrMarketTypeCategoryId"] in (SESSION,OVERSESSION):
                            if row["wrPredefinedValue"] == 0:
                                row["wrPredefinedValue"] = ipl_data.at[total_balls - 1, 'Cumulative']
                                event_market.loc[index] = row
                    # Additional calculations and updates for ipl_data if needed
                if row["wrMarketTypeCategoryId"] in (ONLYOVER,ONLYOVERLDO):
                    event_market.at[index, "beforeAutoCloseBalls"] = row["wrBeforeAutoClose"]
                    event_market.at[index, "beforeAutoSuspendBalls"] = row["wrBeforeAutoSuspend"]
                    event_market.at[index, "autoOpenBalls"] = row["wrAutoOpen"]
                    event_market.at[index, 'overBalls'] = common.overs_to_balls(row['wrOver'], match_type_id)
                    event_market.at[index, 'createBalls'] = row['wrCreate']
                
                if row["wrMarketTypeCategoryId"] in (FANCYLDO,LASTDIGITNUMBER,ODDEVEN) and row["wrStatus"] in (INACTIVE, NOTCREATED):
                
                    #row["wrMarketName"] += f" - {team_shortname}"
                    if row["createBalls"]==0:
                        open_close_market(row, 0, 0)
                        
                        row["wrID"] = insert_markets_to_db(row, 0, 0, False, commentary_id, match_type_id, commentary_team)
                        event_market.loc[index] = row
            
                if row["wrMarketTypeCategoryId"] in (FANCYLDO, ONLYOVERLDO):
                    ldo_market_runners=LD_LDO_RUNNERS[f"{commentary_id}_{commentary_team}"]
    
                    filtered_ldo_market = ldo_market_runners[ldo_market_runners['wrMarketTemplateId'] == row["wrMarketTemplateId"]]
                    row_ldo_market=filtered_ldo_market.iloc[0]
                    row["line"]=row_ldo_market["wrLine"]
                    row["overrate"]=row_ldo_market["wrOverRate"] 
                    row["underrate"]= row_ldo_market["wrUnderRate"] 
                    row["backprice"]=row_ldo_market["wrBackPrice"]
                    row["layprice"]=row_ldo_market["wrLayPrice"]
                    row["laysize"]=row_ldo_market["wrLaySize"] 
                    row["backsize"]=row_ldo_market["wrBackSize"]
                    event_market.loc[index]=row
                
                if row["wrMarketTypeCategoryId"]== TOTALEVENTRUN:
                    if row["createBalls"]==0:
                        #open_close_market(row, 0, 0)
                        
                        row["wrID"] = insert_markets_to_db(row, 0, 0, False, commentary_id, match_type_id, commentary_team)
                        event_market.loc[index] = row
                        
            #globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=event_market
            if category_id!=TOTALEVENTRUN:
                globals()[global_var_name][key]=event_market 
            else:
                globals()[global_var_name][total_event_run_key] =event_market
                
def set_default_values(commentary_id, commentary_team, match_type_id,default_ball_faced,default_player_boundaries,default_player_runs):
    global LD_DEFAULT_VALUES
    data={
            "default_ball_faced":default_ball_faced,
            "default_player_boundaries":default_player_boundaries,
            "default_player_runs":default_player_runs
        }
    LD_DEFAULT_VALUES[f"{commentary_id}_{commentary_team}"]=pd.DataFrame([data])
    


def open_close_market(market,balls,total_wicket):
    before_auto_close=int(market["wrBeforeAutoClose"])  if market["wrMarketTypeCategoryId"] in (ONLYOVER,ONLYOVERLDO) else int(market["beforeAutoCloseBalls"])
    before_auto_suspend=int(market["wrBeforeAutoSuspend"])  if market["wrMarketTypeCategoryId"] in (ONLYOVER,ONLYOVERLDO) else  int(market["beforeAutoSuspendBalls"])
    auto_open=int(market["wrAutoOpen"])  if market["wrMarketTypeCategoryId"] in (ONLYOVER,ONLYOVERLDO) else  int(market["autoOpenBalls"])
    is_update=False
    if total_wicket is not None:

        if(total_wicket >= market["wrAfterWicketAutoSuspend"] and market["wrAfterWicketAutoSuspend"]>0):
            market["wrStatus"]=CLOSE
            market["wrCloseTime"]=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00")
            is_update=True
        else:
            if(balls>=before_auto_close and before_auto_close!=before_auto_suspend and market["wrStatus"]!=SETTLED):
                market["wrStatus"]=CLOSE
                market["wrCloseTime"]=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00")
                is_update=True
            elif(balls>=auto_open and market["wrStatus"] not in (CLOSE,OPEN) and balls<before_auto_close-2):
                market["wrStatus"]=OPEN
                market["wrOpenTime"]=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00")
                is_update=True
            elif(balls>=before_auto_close and before_auto_close==before_auto_suspend and market["wrStatus"]!=SETTLED):
                market["wrStatus"]=CLOSE
                market["wrCloseTime"]=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00")
                is_update=True
            elif(before_auto_suspend==balls and before_auto_close!=before_auto_suspend):
                market["wrStatus"]=SUSPEND
                is_update=True
            elif(market["wrMarketTypeCategoryId"] == FANCYLDO and market["wrStatus"]==SUSPEND and market["wrIsBallStart"]==False):
                market["wrStatus"]=OPEN
                is_update=True
    return market,is_update  


#def insert_markets_to_db(market, balls, total_wicket, is_only, commentary_id, match_type_id, commentary_team):
#    return asyncio.run(insert_markets(market, balls, total_wicket, is_only, commentary_id, match_type_id, commentary_team))

def insert_markets_to_db(market, balls, total_wicket, is_only, commentary_id, match_type_id, commentary_team, session_markets=pd.DataFrame(),iplData=pd.DataFrame()):
    with get_db_session() as session:
        # Start a transaction explicitly
        transaction = session.begin()
        try:
            if market["wrTemplateType"] in (INPLAY, PREMATCHINPLAY):
                overs = 0

                if total_wicket is not None and total_wicket < market["wrAfterWicketNotCreated"]:                
                    # Check for existing record
                    
                    if market["wrMarketTypeCategoryId"]==TOTALEVENTRUN:
                        query = text('SELECT "wrID" FROM "tblEventMarkets" WHERE "wrCommentaryId"=:commentary_id AND "wrMarketTypeCategoryId"=:market_type_category_id')
                    else:
                        query = text('SELECT "wrID" FROM "tblEventMarkets" WHERE "wrCommentaryId"=:commentary_id AND "wrMarketTypeCategoryId"=:market_type_category_id AND "wrTeamID"=:team_id AND "wrOver"= :overs')
                    
                    params = {
                            'commentary_id': int(market["wrCommentaryId"]),
                            'market_type_category_id': int(market["wrMarketTypeCategoryId"]),
                        }
                        
                        # Add additional parameters if they are needed
                    if market["wrMarketTypeCategoryId"] != TOTALEVENTRUN:
                        params['team_id'] = int(market["wrTeamID"])
                        params['overs'] = int(market["wrOver"])
                    result = session.execute(query, params)
                    
                    if_record_exist = result.fetchall()
                    print("The existing market is:", if_record_exist)

                    if len(if_record_exist) == 0:
                        # Insert into database
                        #query1 = text('INSERT INTO "tblEventMarkets" ("wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrMarketTemplateId", "wrMargin", "wrCreateType", "wrMarketTypeCategoryId", "wrMarketTypeId", "wrRateSource", "wrPredefinedValue", "wrDelay", "wrCreate", "wrCreateRefId", "wrOpenRefId", "wrActionType", "wrAutoResultType", "wrIsSendData", "wrIsAllow", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize", "wrDefaultIsSendData", "wrRateDiff") VALUES (:commentary_id, :event_ref_id, :team_id, :innings_id, :market_name, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :market_template_id, :margin, :create_type, :market_type_category_id, :market_type_id, :rate_source, :predefined_value, :delay, :create, :create_ref_id, :open_ref_id, :action_type, :auto_result_type, :is_send_data, :is_bet_allowed, :line_type, :default_back_size, :default_lay_size, :default_is_send_data, :rate_diff) RETURNING "wrID"')
                        
                        if market["wrMarketTypeCategoryId"] in (SESSION,OVERSESSION):
                            if not market["wrIsPredefineMarket"] and not session_markets.empty:
                                idx=session_markets["wrOver"].idxmax()
                                last_row=session_markets.loc[idx]
                                wrdata = json.loads(last_row["wrData"])
                                runner = wrdata["runner"][0]
                                maxover_line=float(runner["line"])
                                market["wrPredefinedValue"]=float(market["wrPredefinedValue"])*(maxover_line/float(iplData['Cumulative'].iloc[-1]))

                        
                        
                        query1 = text('INSERT INTO "tblEventMarkets" ( "wrCommentaryId", "wrEventRefID", "wrTeamID", "wrInningsID", "wrMarketName", "wrStatus", "wrIsPredefineMarket", "wrTemplateType", "wrIsOver", "wrOver", "wrIsPlayer", "wrIsAutoCancel", "wrAutoOpenType", "wrAutoOpen", "wrAutoCloseType", "wrBeforeAutoClose", "wrAutoSuspendType", "wrBeforeAutoSuspend", "wrIsBallStart", "wrIsAutoResultSet", "wrAutoResultafterBall", "wrAfterWicketAutoSuspend", "wrAfterWicketNotCreated", "wrIsActive", "wrMarketTemplateId", "wrMargin", "wrCreateType", "wrMarketTypeCategoryId", "wrMarketTypeId", "wrRateSource", "wrPredefinedValue", "wrDelay", "wrCreate", "wrCreateRefId", "wrOpenRefId", "wrActionType", "wrAutoResultType", "wrIsSendData", "wrIsAllow", "wrLineType", "wrDefaultBackSize", "wrDefaultLaySize", "wrDefaultIsSendData", "wrRateDiff" ) SELECT :commentary_id, :event_ref_id, :team_id, :innings_id, :market_name, :status, :is_predefine_market, :template_type, :is_over, :over, :is_player, :is_auto_cancel, :auto_open_type, :auto_open, :auto_close_type, :before_auto_close, :auto_suspend_type, :before_auto_suspend, :is_ball_start, :is_auto_result_set, :auto_result_after_ball, :after_wicket_auto_suspend, :after_wicket_not_created, :is_active, :market_template_id, :margin, :create_type, :market_type_category_id, :market_type_id, :rate_source, :predefined_value, :delay, :create, :create_ref_id, :open_ref_id, :action_type, :auto_result_type, :is_send_data, :is_bet_allowed, :line_type, :default_back_size, :default_lay_size, :default_is_send_data, :rate_diff WHERE NOT EXISTS ( SELECT 1 FROM "tblEventMarkets" WHERE "wrCommentaryId" = :commentary_id AND "wrMarketTypeCategoryId" = :market_type_category_id AND "wrTeamID" = :team_id AND "wrOver" = :over ) RETURNING "wrID"')
                        query2 = text('INSERT INTO "tblMarketRunners" ("wrEventMarketId", "wrRunner", "wrLine", "wrOverRate", "wrUnderRate", "wrBackPrice", "wrBackSize", "wrLayPrice", "wrLaySize", "wrLastUpdate", "wrSelectionId", "wrSelectionStatus") VALUES (:market_id, :runner, :line, :over_rate, :under_rate, :back_price, :back_size, :lay_price, :lay_size, :last_update, :selection_id, :selection_status) RETURNING "wrRunnerId"')

                        if not is_only:
                            if balls >= market["autoOpenBalls"]:
                                market["wrStatus"] = OPEN
                            else:
                                market["wrStatus"] = INACTIVE

                        if market["wrMarketTypeCategoryId"] in (FANCYLDO, ONLYOVERLDO, LASTDIGITNUMBER,ODDEVEN):
                            result = session.execute(query1, {
                                'commentary_id': int(market["wrCommentaryId"]),
                                'event_ref_id': str(market["wrEventRefID"]),
                                'team_id': int(market["wrTeamID"]),
                                'innings_id': int(market["wrInningsID"]),
                                'market_name': str(market['wrMarketName']),
                                'status': int(market["wrStatus"]),
                                'is_predefine_market': bool(market['wrIsPredefineMarket']),
                                'template_type': int(market['wrTemplateType']),
                                'is_over': bool(market['wrIsOver']),
                                'over': int(market['wrOver']),
                                'is_player': bool(market['wrIsPlayer']),
                                'is_auto_cancel': bool(market['wrIsAutoCancel']),
                                'auto_open_type': int(market['wrAutoOpenType']),
                                'auto_open': float(market['wrAutoOpen']),
                                'auto_close_type': int(market['wrAutoCloseType']),
                                'before_auto_close': float(market['wrBeforeAutoClose']),
                                'auto_suspend_type': int(market['wrAutoSuspendType']),
                                'before_auto_suspend': float(market['wrBeforeAutoSuspend']),
                                'is_ball_start': bool(market['wrIsBallStart']),
                                'is_auto_result_set': bool(market['wrIsAutoResultSet']),
                                'auto_result_after_ball': int(market['wrAutoResultafterBall']),
                                'after_wicket_auto_suspend': int(market['wrAfterWicketAutoSuspend']),
                                'after_wicket_not_created': int(market['wrAfterWicketNotCreated']),
                                'is_active': bool(market['wrIsActive']),
                                'market_template_id': int(market['wrMarketTemplateId']),
                                'margin': float(market['wrMargin']),
                                'create_type': int(market["wrCreateType"]),
                                'market_type_category_id': int(market["wrMarketTypeCategoryId"]),
                                'market_type_id': int(market["wrMarketTypeId"]),
                                'rate_source': int(market["wrRateSource"]),
                                'predefined_value': float(market["wrPredefinedValue"]),
                                'delay': int(market["wrDelay"]),
                                'create': float(market["wrCreate"]),
                                'create_ref_id': int(market["wrCreateRefId"]),
                                'open_ref_id': int(market["wrOpenRefId"]),
                                'action_type': int(market["wrActionType"]),
                                'auto_result_type': int(market["wrAutoResultType"]),
                                'is_send_data': bool(market['wrIsSendData']),
                                'is_bet_allowed': bool(market['wrIsAllow']),
                                'line_type': int(market['wrLineType']),
                                'default_back_size': int(market['wrDefaultBackSize']),
                                'default_lay_size': int(market['wrDefaultLaySize']),
                                'default_is_send_data': bool(market['wrDefaultIsSendData']),
                                'rate_diff': int(market["wrRateDiff"])
                            })
                            if result.rowcount > 0:
                                market_id = result.fetchone()[0]
                                market["wrID"] = market_id
    
                                # Handle market runners
                                ldo_market_runners = LD_LDO_RUNNERS[f"{commentary_id}_{commentary_team}"]
                                filtered_df = ldo_market_runners[ldo_market_runners['wrMarketTemplateId'] == market["wrMarketTemplateId"]]
                                filtered_df = filtered_df.reset_index(drop=True)
                                market_runner_data = []
                                for index, row in filtered_df.iterrows():
                                    ldo_market_runners.at[index, "eventmarketid"] = market_id
    
                                    result = session.execute(query2, {
                                        'market_id': int(market_id),
                                        'runner': row["wrRunner"] if market["wrMarketTypeCategoryId"] in (LASTDIGITNUMBER,ODDEVEN) else market["wrMarketName"],
                                        'line': row["wrLine"],
                                        'over_rate': row["wrOverRate"],
                                        'under_rate': row["wrUnderRate"],
                                        'back_price': row["wrBackPrice"],
                                        'back_size': row["wrBackSize"],
                                        'lay_price': row["wrLayPrice"],
                                        'lay_size': row["wrLaySize"],
                                        'last_update': datetime.datetime.now(),
                                        'selection_id': int(f"{market_id}01"),
                                        'selection_status': int(market["wrStatus"])
                                    })
                                    runner_id = result.fetchone()[0]
    
                                    row["wrRunnerId"] = runner_id
                                    row["wrSelectionStatus"] = market["wrStatus"]
                                    market_runner_data.append(common.convert_runner_data(row))
                                market_runner_data = [runner for sublist in market_runner_data for runner in sublist]
                                wrdata = common.convert_event_market(market, market_runner_data)
                                market['wrData'] = wrdata
    
                                query3 = text('UPDATE "tblEventMarkets" SET "wrData"=:wr_data WHERE "wrID"=:market_id AND "wrCommentaryId"=:commentary_id')
                                session.execute(query3, {'wr_data': wrdata, 'market_id': int(market["wrID"]), 'commentary_id': int(market["wrCommentaryId"])})
    
                                LD_LDO_RUNNERS[f"{commentary_id}_{commentary_team}"] = ldo_market_runners
                                transaction.commit()  # Commit the transaction
                                return market_id
                        else:
                            print("Inserted:", market["wrMarketName"])
                            max_overs = common.get_max_overs(commentary_id)
                            if market['wrOver'] <= max_overs:
                                result = session.execute(query1, {
                                    'commentary_id': int(market["wrCommentaryId"]),
                                    'event_ref_id': str(market["wrEventRefID"]),
                                    'team_id': int(market["wrTeamID"]),
                                    'innings_id': int(market["wrInningsID"]),
                                    'market_name': str(market['wrMarketName']),
                                    'status': int(market["wrStatus"]),
                                    'is_predefine_market': bool(market['wrIsPredefineMarket']),
                                    'template_type': int(market['wrTemplateType']),
                                    'is_over': bool(market['wrIsOver']),
                                    'over': int(market['wrOver']),
                                    'is_player': bool(market['wrIsPlayer']),
                                    'is_auto_cancel': bool(market['wrIsAutoCancel']),
                                    'auto_open_type': int(market['wrAutoOpenType']),
                                    'auto_open': market['wrAutoOpen'],
                                    'auto_close_type': int(market['wrAutoCloseType']),
                                    'before_auto_close': market['wrBeforeAutoClose'],
                                    'auto_suspend_type': int(market['wrAutoSuspendType']),
                                    'before_auto_suspend': market['wrBeforeAutoSuspend'],
                                    'is_ball_start': bool(market['wrIsBallStart']),
                                    'is_auto_result_set': bool(market['wrIsAutoResultSet']),
                                    'auto_result_after_ball': int(market['wrAutoResultafterBall']),
                                    'after_wicket_auto_suspend': int(market['wrAfterWicketAutoSuspend']),
                                    'after_wicket_not_created': int(market['wrAfterWicketNotCreated']),
                                    'is_active': bool(market['wrIsActive']),
                                    'market_template_id': int(market['wrMarketTemplateId']),
                                    'margin': market['wrMargin'],
                                    'create_type': int(market["wrCreateType"]),
                                    'market_type_category_id': int(market["wrMarketTypeCategoryId"]),
                                    'market_type_id': int(market["wrMarketTypeId"]),
                                    'rate_source': int(market["wrRateSource"]),
                                    'predefined_value': float(market["wrPredefinedValue"]),
                                    'delay': int(market["wrDelay"]),
                                    'create': float(market["wrCreate"]),
                                    'create_ref_id': int(market["wrCreateRefId"]),
                                    'open_ref_id': int(market["wrOpenRefId"]),
                                    'action_type': int(market["wrActionType"]),
                                    'auto_result_type': int(market["wrAutoResultType"]),
                                    'is_send_data': bool(market['wrIsSendData']),
                                    'is_bet_allowed': bool(market['wrIsAllow']),
                                    'line_type': int(market['wrLineType']),
                                    'default_back_size': int(market['wrDefaultBackSize']),
                                    'default_lay_size': int(market['wrDefaultLaySize']),
                                    'default_is_send_data': bool(market['wrDefaultIsSendData']),
                                    'rate_diff': int(market["wrRateDiff"])
                                })
                                if result.rowcount > 0:
                                    market_id = result.fetchone()[0]
                                    market["wrID"] = market_id
                                    session.execute(query2, {
                                        'market_id': market_id,
                                        'runner': market["wrMarketName"],
                                        'line': 0,
                                        'over_rate': 0,
                                        'under_rate': 0,
                                        'back_price': 0,
                                        'back_size': 100,
                                        'lay_price': 0,
                                        'lay_size': 100,
                                        'last_update': datetime.datetime.now(),
                                        'selection_id': f"{market_id}01",
                                        'selection_status': INACTIVE
                                    })
                                print("Inserted after market ID is", market["wrID"])
                                transaction.commit()  # Commit the transaction
                                return market_id
                    else:
                        return if_record_exist[0][0]
        except Exception as e:
            print("An error occurred:", e)
            transaction.rollback()  
            # Rollback the transaction in case of error
            raise  # Re-raise the exception for further handling
    return market["wrID"]

def get_fancy_ldo_values(commentary_id, match_type_id, commentary_team):
    market_template = LD_MARKET_TEMPLATE[f"{commentary_id}_{commentary_team}"]
    ldo_market_runners = []

    query1 = text('SELECT * FROM "tblMarketTemplateRunners" WHERE "wrMarketTemplateId"=:template_id AND "wrIsDeleted"=False')

    with get_db_session() as session:
        for index, row in market_template.iterrows():
            if row["wrMarketTypeCategoryId"] in (FANCYLDO, ONLYOVERLDO, LASTDIGITNUMBER, ODDEVEN):
                try:
                    ldo_market_template = pd.read_sql_query(
                        query1, 
                        session.bind, 
                        params={"template_id": row["wrID"]}
                    )
                    ldo_market_runners.append(ldo_market_template)
                except SQLAlchemyError as e:
                    print(f"Database query failed: {e}")

    if len(ldo_market_runners) > 0:
        concatenated_df = pd.concat(ldo_market_runners, ignore_index=True)
        concatenated_df["eventmarketid"] = 0
        LD_LDO_RUNNERS[f"{commentary_id}_{commentary_team}"]=concatenated_df



def process_session_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of session is ",time.time())
    stime = time.time()
    global LD_SESSION_MARKETS, LD_IPL_DATA
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_SESSION_MARKETS:
        event_markets = LD_SESSION_MARKETS[key]
        iplData = LD_IPL_DATA[f"{commentary_id}_{commentary_team}_ipldata"]
        runner_data = []
        event_data = []
        market_datalog = []
        socket_data = []

        for index, row in event_markets.iterrows():
            if row["wrStatus"] != SETTLED and total_balls >= row["createBalls"]:

                selection_status = NOTCREATED
                fixed_line_value=over_value=under_value=back_price=lay_price = 0
                lay_size=back_size = 100
                market_runner = []
                is_update = False
                is_SendData = True
                predicted_value = 0

                if row["wrStatus"] == SUSPEND:
                    row["wrStatus"] = OPEN

                if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (OPEN, SUSPEND, CLOSE):
                    
                    market_id = insert_markets_to_db(row, total_balls, total_wicket, False, commentary_id, match_type_id, commentary_team,event_markets,iplData)
                    row["wrID"] = market_id
                    event_markets.loc[index] = row

                updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                if is_wicket == 1:
                    updated_market["wrPredefinedValue"] -= wicket_deduction
                event_markets.loc[index] = updated_market

                if (row["wrIsOver"] and row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                    (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                    (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"])):
                    if total_balls == 0:
                        predicted_value = row["wrPredefinedValue"] + current_score
                    else:
                        predicted_value = (current_score - iplData['Cumulative'][total_balls - 1] * (1 / row["wrLineRatio"])) + row["wrPredefinedValue"]

                    if predicted_value > 0:
                        predicted_value=round(predicted_value,1)
                        if predicted_value<=current_score:
                            predicted_value+=1
                        margin = 1 + (float(row["wrMargin"]) / 100)
                        new_predicted_value = int(predicted_value) + 0.5
                        over_value = 1 / (margin / (1 + math.exp(-(predicted_value - new_predicted_value))))
                        under_value = 1 / (margin / (1 + math.exp(predicted_value - new_predicted_value)))
                        over_value, under_value = round(over_value, 2), round(under_value, 2)
                        
                        lay_price = round(predicted_value)
                        back_price = lay_price + row["wrRateDiff"]
                        lay_size, back_size = row["wrDefaultLaySize"], row["wrDefaultBackSize"]
                        fixed_line_value = round(predicted_value, 1)

                        row["minodds"] = min(row["minodds"] or back_price, back_price)
                        row["maxodds"] = max(row["maxodds"] or back_price, back_price)
                        row["openvalue"] = row["openvalue"] or back_price

                        event_markets.loc[index] = row
                        wicket_deduction += 2

                        selection_status = CLOSE if row["wrStatus"] == CLOSE else OPEN
                        is_SendData = row["wrDefaultIsSendData"] if selection_status == OPEN else is_SendData

                else:
                    if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                        selection_status = CLOSE
                    elif row["wrStatus"] == SUSPEND:
                        selection_status = SUSPEND

                if (total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED):
                    row["wrStatus"] = SETTLED
                    event_markets.loc[index] = row
                    selection_status = SETTLED
                    result = common.markets_result_settlement(row, commentary_id, commentary_team)
                    if result is None:
                        continue
                    row["wrResult"] = result
                    row["wrSettledTime"] = datetime.datetime.now().isoformat()
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

                event_markets.loc[index] = row
                LD_SESSION_MARKETS[key] = event_markets
        print("Execution time for session without db is", time.time() - stime)

        if runner_data and market_datalog and event_data:
            runner_json = json.dumps(runner_data)
            market_datalog_json = json.dumps(market_datalog)
            eventmarket_json = json.dumps(event_data)
            common.send_data_to_socket_async(commentary_id, socket_data)
            common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for session is", time.time() - stime)
    print("closing time for session is", time.time())


def process_over_session_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of over session is ",time.time())

    stime = time.time()
    global LD_OVERSESSION_MARKETS, LD_IPL_DATA
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_OVERSESSION_MARKETS:
        event_markets = LD_OVERSESSION_MARKETS[key]
        iplData = LD_IPL_DATA[f"{commentary_id}_{commentary_team}_ipldata"]
        runner_data = []
        event_data = []
        market_datalog = []
        socket_data = []

        for index, row in event_markets.iterrows():
            if row["wrStatus"] != SETTLED and  total_balls >= row["createBalls"]:

                selection_status = NOTCREATED
                fixed_line_value=over_value=under_value=back_price=lay_price = 0
                lay_size=back_size = 100
                market_runner = []
                is_update = False
                is_SendData = True
                predicted_value = 0
                marketname=""
                before_auto_close=0

                if row["wrStatus"] == SUSPEND:
                    row["wrStatus"] = OPEN

                if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (OPEN, SUSPEND, CLOSE):
                    if key in LD_SESSION_MARKETS:
                        session_markets = LD_SESSION_MARKETS[key]
                        market_id = insert_markets_to_db(row, total_balls, total_wicket, False, commentary_id, match_type_id, commentary_team,session_markets,iplData)
                        row["wrID"] = market_id
                    else:
                        market_id = insert_markets_to_db(row, total_balls, total_wicket, False, commentary_id, match_type_id, commentary_team)
                        row["wrID"] = market_id
                    event_markets.loc[index] = row

                updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                if is_wicket == 1:
                    updated_market["wrPredefinedValue"] -= wicket_deduction
                event_markets.loc[index] = updated_market

                if (row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                    (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                    (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"])):
                    if total_balls == 0:
                        predicted_value = row["wrPredefinedValue"] + current_score
                    else:
                        predicted_value = (current_score - iplData['Cumulative'][total_balls - 1] * (1 / row["wrLineRatio"])) + row["wrPredefinedValue"]

                    if predicted_value > 0:
                        predicted_value=round(predicted_value,1)
                        if predicted_value<=current_score:
                            predicted_value+=1
                        margin = 1 + (float(row["wrMargin"]) / 100)
                        new_predicted_value = int(predicted_value) + 0.5
                        over_value = 1 / (margin / (1 + math.exp(-(predicted_value - new_predicted_value))))
                        under_value = 1 / (margin / (1 + math.exp(predicted_value - new_predicted_value)))
                        over_value, under_value = round(over_value, 2), round(under_value, 2)
                        lay_price = round(predicted_value)
                        back_price = lay_price + row["wrRateDiff"]
                        lay_size, back_size = row["wrDefaultLaySize"], row["wrDefaultBackSize"]
                        fixed_line_value = round(predicted_value, 1)

                        row["minodds"] = min(row["minodds"] or back_price, back_price)
                        row["maxodds"] = max(row["maxodds"] or back_price, back_price)
                        row["openvalue"] = row["openvalue"] or back_price

                        event_markets.loc[index] = row
                        wicket_deduction += 2

                        selection_status = CLOSE if row["wrStatus"] == CLOSE else OPEN
                        is_SendData = row["wrDefaultIsSendData"] if selection_status == OPEN else is_SendData

                else:
                    if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                        selection_status = CLOSE
                    elif row["wrStatus"] == SUSPEND:
                        selection_status = SUSPEND

                if (total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED):
                    row["wrStatus"] = SETTLED
                    event_markets.loc[index] = row
                    selection_status = SETTLED
                    result = common.markets_result_settlement(row, commentary_id, commentary_team)
                    if result is None:
                        continue
                    row["wrResult"] = result
                    row["wrSettledTime"] = datetime.datetime.now().isoformat()


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

                event_markets.loc[index] = row
                LD_OVERSESSION_MARKETS[key] = event_markets
        print("Execution time for over session without db is", time.time() - stime)

        if runner_data and market_datalog and event_data:
            runner_json = json.dumps(runner_data)
            market_datalog_json = json.dumps(market_datalog)
            eventmarket_json = json.dumps(event_data)
            common.send_data_to_socket_async(commentary_id, socket_data)
            common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for over session is", time.time() - stime)
    print("closing time for overssion is", time.time())


def process_onlyover_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of only over is ",time.time())

    global LD_ONLYOVER_MARKETS, LD_IPL_DATA
    stime=time.time()
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_ONLYOVER_MARKETS:
        event_markets = LD_ONLYOVER_MARKETS[key]
        iplData = LD_IPL_DATA[f"{commentary_id}_{commentary_team}_ipldata"]
        runner_data = []
        event_data = []
        market_datalog = []
        socket_data = []

        for index, row in event_markets.iterrows():
            if row["wrStatus"] != SETTLED and total_balls >= row["createBalls"]:

                selection_status = NOTCREATED
                fixed_line_value=over_value=under_value=back_price=lay_price = 0
                lay_size=back_size = 100
                market_runner = []
                is_update = False
                is_SendData = True
                predicted_value = 0

                if row["wrStatus"] == SUSPEND:
                    row["wrStatus"] = OPEN

                if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (OPEN, SUSPEND, CLOSE):
                    filtered_df = iplData[iplData["Over"] == row["wrOver"]]
                    if len(filtered_df) > 0:
                        only_over_runs = filtered_df["RPB"].sum()
                        if is_wicket and row["wrPredefinedValue"] == 0:
                            only_over_runs -= 1
                            predicted_value = only_over_runs * row["wrLineRatio"]
                        elif is_wicket and row["wrPredefinedValue"] != 0:
                            predicted_value = row["wrPredefinedValue"] - 1
                        elif not is_wicket and row["wrPredefinedValue"] == 0:
                            predicted_value = only_over_runs * row["wrLineRatio"]
                        elif not is_wicket and row["wrPredefinedValue"] != 0:
                            predicted_value = row["wrPredefinedValue"]

                        row["wrPredefinedValue"] = predicted_value
                    
                    market_id = insert_markets_to_db(row, total_balls, total_wicket, True, commentary_id, match_type_id, commentary_team)
                    row["wrID"] = market_id
                    event_markets.loc[index] = row

                updated_market, is_update = open_close_market(row, total_balls, total_wicket)

                if row["wrIsOver"] and (
                    row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                    (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                    (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"])):

                    filtered_df = iplData[iplData["Over"] == row["wrOver"]]
                    if len(filtered_df) > 0:
                        only_over_runs = filtered_df["RPB"].sum()
                        if is_wicket and row["wrPredefinedValue"] == 0:
                            only_over_runs -= 1
                            predicted_value = only_over_runs * row["wrLineRatio"]
                        elif is_wicket and row["wrPredefinedValue"] != 0:
                            predicted_value = row["wrPredefinedValue"] - 1
                        elif not is_wicket and row["wrPredefinedValue"] == 0:
                            predicted_value = only_over_runs * row["wrLineRatio"]
                        elif not is_wicket and row["wrPredefinedValue"] != 0:
                            predicted_value = row["wrPredefinedValue"]

                        row["wrPredefinedValue"] = predicted_value

                    selection_status = row["wrStatus"]

                    if predicted_value > 0:
                        predicted_value=round(predicted_value,1)

                        margin = 1 + (float(row["wrMargin"]) / 100)
                        new_predicted_value = int(predicted_value) + 0.5

                        over_value = 1 / (margin / (1 + math.exp(-(predicted_value - new_predicted_value))))
                        under_value = 1 / (margin / (1 + math.exp(predicted_value - new_predicted_value)))
                        over_value, under_value = round(over_value, 2), round(under_value, 2)
                        lay_price = round(predicted_value)
                        back_price = lay_price + row["wrRateDiff"]
                        lay_size, back_size = row["wrDefaultLaySize"], row["wrDefaultBackSize"]
                        fixed_line_value = round(predicted_value, 1)

                        row["minodds"] = min(row["minodds"] or back_price, back_price)
                        row["maxodds"] = max(row["maxodds"] or back_price, back_price)
                        row["openvalue"] = row["openvalue"] or back_price

                        event_markets.loc[index] = row
                        wicket_deduction += 2

                        selection_status = CLOSE if row["wrStatus"] == CLOSE else OPEN
                        is_SendData = row["wrDefaultIsSendData"] if selection_status == OPEN else is_SendData

                else:
                    if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                        selection_status = CLOSE
                    elif row["wrStatus"] == SUSPEND:
                        selection_status = SUSPEND

                if (total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED):
                    row["wrStatus"] = SETTLED
                    event_markets.loc[index] = row
                    selection_status = SETTLED
                    result = common.markets_result_settlement(row, commentary_id, commentary_team)
                    if result is None:
                        continue
                    row["wrResult"] = result
                    row["wrSettledTime"] = datetime.datetime.now().isoformat()


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

                event_markets.loc[index] = row
                LD_ONLYOVER_MARKETS[key] = event_markets
        print("Execution time for onlyover without db is", time.time() - stime)

        if runner_data and market_datalog and event_data:
            runner_json = json.dumps(runner_data)
            market_datalog_json = json.dumps(market_datalog)
            eventmarket_json = json.dumps(event_data)
            common.send_data_to_socket_async(commentary_id, socket_data)
            common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for onlyover is", time.time() - stime)
    print("closing time for onlyover is", time.time())

def process_sessionldo_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of sessionldo is ",time.time())

    global LD_SESSIONLDO_MARKETS, LD_IPL_DATA
    stime=time.time()
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_SESSIONLDO_MARKETS:
        event_markets = LD_SESSIONLDO_MARKETS[key]
        if not event_markets.empty:
            runner_data = []
            event_data = []
            market_datalog = []
            socket_data = []

            for index, row in event_markets.iterrows():
                if row["wrStatus"] != SETTLED:

                    selection_status = NOTCREATED
                    fixed_line_value=over_value=under_value=back_price=lay_price = 0
                    lay_size=back_size = 100
                    market_runner = []
                    is_update = False
                    is_SendData = True
                    predicted_value = 0
                    market_runner_row = pd.DataFrame()

                    if row["wrStatus"] == SUSPEND:
                        row["wrStatus"] = OPEN
                        
                    

                    updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                    if is_wicket == 1:
                        updated_market["wrPredefinedValue"] -= wicket_deduction
                    event_markets.loc[index] = updated_market
                    
                    if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (SUSPEND, CLOSE):
                        market_id = insert_markets_to_db(row, total_balls, total_wicket, False, commentary_id, match_type_id, commentary_team)
                        if row["wrID"] != market_id:
                            is_update = True
                        row["wrID"] = market_id
                    
                    if (row["wrIsOver"] and
                        (row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                         (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                         (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"]))):
                        

                        if is_update:
                            fixed_line_value = round(float(row["line"]), 1)
                            over_value = row["overrate"]
                            under_value = row["underrate"]
                            back_price = row["backprice"]
                            lay_price = row["layprice"]
                            lay_size = row["laysize"]
                            back_size = row["backsize"]

                            selection_status = CLOSE if row["wrStatus"] == CLOSE else OPEN
                            is_SendData = row["wrDefaultIsSendData"] if selection_status == OPEN else is_SendData

                    else:
                        if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                            selection_status = CLOSE

                        if row["wrStatus"] == SUSPEND:
                            selection_status = SUSPEND

                    if (total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED):
                        row["wrStatus"] = SETTLED
                        event_markets.loc[index] = row["wrStatus"]
                        selection_status = SETTLED
                        result = common.markets_result_settlement(row, commentary_id, commentary_team)
                        if result is None:
                            continue
                        row["wrResult"] = result
                        row["wrSettledTime"] = datetime.datetime.now().isoformat()


                    if is_update:
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

                    event_markets.loc[index] = row
                    LD_SESSIONLDO_MARKETS[key] = event_markets

            if runner_data and market_datalog and event_data:
                runner_json = json.dumps(runner_data)
                market_datalog_json = json.dumps(market_datalog)
                eventmarket_json = json.dumps(event_data)
                common.send_data_to_socket_async(commentary_id, socket_data)
                common.update_database_async(runner_json, market_datalog_json, eventmarket_json)
    print("Execution time for sessionldo is", time.time() - stime)
    print("closing time for sessionldo is", time.time())
            
def process_onlyoverldo_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of onlyover ldo is ",time.time())

    global LD_ONLYOVERLDO_MARKETS, LD_IPL_DATA
    stime=time.time()
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_ONLYOVERLDO_MARKETS:
        event_markets = LD_ONLYOVERLDO_MARKETS[key]
        runner_data = []
        event_data = []
        market_datalog = []
        socket_data = []

        for index, row in event_markets.iterrows():
            if row["wrStatus"] != SETTLED and total_balls >= row["createBalls"]:

                selection_status = NOTCREATED
                fixed_line_value=over_value=under_value=back_price=lay_price = 0
                lay_size=back_size = 100
                market_runner = []
                is_update = False
                is_SendData = True
                predicted_value = 0
                market_runner_row = pd.DataFrame()

                if row["wrStatus"] == SUSPEND:
                    row["wrStatus"] = OPEN
                
                updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                if is_wicket == 1:
                    updated_market["wrPredefinedValue"] -= wicket_deduction
                event_markets.loc[index] = updated_market
                
                if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (SUSPEND, CLOSE):
                    market_id = insert_markets_to_db(row, total_balls, total_wicket, True, commentary_id, match_type_id, commentary_team)
                    if row["wrID"] != market_id:
                        is_update = True
                    row["wrID"] = market_id
                    
               

                if (row["wrIsOver"] and
                    (row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                     (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                     (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"]))):

                    

                    if is_update:
                        fixed_line_value = round(float(row["line"]), 1)
                        over_value = row["overrate"]
                        under_value = row["underrate"]
                        back_price = row["backprice"]
                        lay_price = row["layprice"]
                        lay_size = row["laysize"]
                        back_size = row["backsize"]

                        selection_status = CLOSE if row["wrStatus"] == CLOSE else OPEN
                        is_SendData = row["wrDefaultIsSendData"] if selection_status == OPEN else is_SendData

                else:
                    if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                        selection_status = CLOSE

                    if row["wrStatus"] == SUSPEND:
                        selection_status = SUSPEND

                if (total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED):
                    row["wrStatus"] = SETTLED
                    event_markets.loc[index] = row["wrStatus"]
                    selection_status = SETTLED
                    result = common.markets_result_settlement(row, commentary_id, commentary_team)
                    if result is None:
                        continue
                    row["wrResult"] = result
                    row["wrSettledTime"] = datetime.datetime.now().isoformat()
                    

                if is_update:
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

                event_markets.loc[index] = row
                LD_ONLYOVERLDO_MARKETS[key] = event_markets

        if runner_data and market_datalog and event_data:
            runner_json = json.dumps(runner_data)
            market_datalog_json = json.dumps(market_datalog)
            eventmarket_json = json.dumps(event_data)
            common.send_data_to_socket_async(commentary_id, socket_data)
            common.update_database_async(runner_json, market_datalog_json, eventmarket_json)
    
    print("Execution time for onlyoverldo is", time.time() - stime)
    print("closing time for onlyoverldo is", time.time())

def process_lastdigit_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of last digit is ",time.time())

    global LD_LASTDIGIT_MARKETS, LD_LDO_RUNNERS
    stime=time.time()
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_LASTDIGIT_MARKETS:
        event_markets = LD_LASTDIGIT_MARKETS[key]
        if not event_markets.empty:

            ldo_market_runners = LD_LDO_RUNNERS[key]
            runner_data = []
            event_data = []
            market_datalog = []
            socket_data = []

            for index, row in event_markets.iterrows():
                if row["wrStatus"] != SETTLED:
                    selection_status = NOTCREATED
                    market_runner = []
                    is_update = False
                    is_SendData = True
                    market_runner_row = pd.DataFrame()

                    if row["wrStatus"] == SUSPEND:
                        row["wrStatus"] = OPEN
                    
                    
                    updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                    if is_wicket == 1:
                        updated_market["wrPredefinedValue"] -= wicket_deduction
                    event_markets.loc[index] = updated_market
                    
                    if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (SUSPEND, CLOSE):

                        market_id = insert_markets_to_db(row, total_balls, total_wicket, False, commentary_id, match_type_id, commentary_team)
                        if row["wrID"] != market_id:
                            is_update = True
                        row["wrID"] = market_id
                        
                    

                    if (row["wrIsOver"] and
                        (row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                         (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                         (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"]))):

                        if is_update:
                            if row["wrStatus"] == CLOSE:
                                selection_status = CLOSE
                            elif row["wrStatus"] == OPEN:
                                selection_status = OPEN
                                is_SendData = row["wrDefaultIsSendData"]

                            if total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED:
                                market_runner_row = common.fetch_runner_data(SETTLED, row)
                            else:
                                market_runner_row = common.fetch_runner_data(selection_status, row)

                            if len(market_runner_row) > 0:
                                filtered_ldo_market = ldo_market_runners[ldo_market_runners["wrMarketTemplateId"] == row["wrMarketTemplateId"]].reset_index(drop=True)

                                if total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"]:
                                    runs = common.get_market_score(commentary_id, commentary_team, int(row["wrOver"]), True)
                                    if runs is None:
                                        continue
                                    market_runner_row["wrSelectionStatus"] = market_runner_row["wrRunner"].apply(lambda x: WIN if x == str(runs % 10) else LOSE)
                                    id_of_winner = market_runner_row.loc[market_runner_row["wrSelectionStatus"] == WIN, "wrRunnerId"].tolist()
                                    row["wrResult"] = id_of_winner[0]
                                    selection_status = SETTLED
                                    row["wrStatus"] = SETTLED
                                    row["wrSettledTime"] = datetime.datetime.now().isoformat()

                                for _, row_ldo_market in filtered_ldo_market.iterrows():
                                    matching_row = market_runner_row[market_runner_row["wrRunner"] == row_ldo_market["wrRunner"]]
                                    runner_status = int(row["wrStatus"]) if row["wrStatus"] != SETTLED else int(matching_row["wrSelectionStatus"].iloc[0])

                                    runner_data_dict = {
                                        "wrRunner": row_ldo_market["wrRunner"],
                                        "wrLine": round(float(row_ldo_market["wrLine"]), 1),
                                        "wrOverRate": float(row_ldo_market["wrOverRate"]),
                                        "wrUnderRate": float(row_ldo_market["wrUnderRate"]),
                                        "wrBackPrice": float(row_ldo_market["wrBackPrice"]),
                                        "wrLayPrice": float(row_ldo_market["wrLayPrice"]),
                                        "wrBackSize": float(row_ldo_market["wrBackSize"]),
                                        "wrLaySize": float(row_ldo_market["wrLaySize"]),
                                        "wrSelectionStatus": runner_status,
                                        "wrEventMarketId": int(row["wrID"]),
                                        "wrRunnerId": int(matching_row["wrRunnerId"].iloc[0])
                                    }
                                    runner_data.append(runner_data_dict)
                                    row["wrIsSendData"] = is_SendData
                                    updated_runner_row = pd.DataFrame([runner_data_dict])
                                    market_runner.append(common.convert_runner_data(updated_runner_row))

                                market_runner = [runner for sublist in market_runner for runner in sublist]

                    else:
                        if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                            selection_status = CLOSE

                        if row["wrStatus"] == SUSPEND:
                            selection_status = SUSPEND

                    if is_update:
                        market_runner_row = common.fetch_runner_data(selection_status, row)

                    if len(market_runner_row) > 0:
                        row["wrIsSendData"] = is_SendData
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

                    event_markets.loc[index] = row
                    LD_LASTDIGIT_MARKETS[key] = event_markets

            if runner_data and market_datalog and event_data:
                runner_json = json.dumps(runner_data)
                market_datalog_json = json.dumps(market_datalog)
                eventmarket_json = json.dumps(event_data)
                common.send_data_to_socket_async(commentary_id, socket_data)
                common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for lastdigit is", time.time() - stime)
    print("closing time for lastdigit is", time.time())

    
def process_oddeven_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of last digit is ",time.time())

    global LD_ODDEVEN_MARKETS, LD_LDO_RUNNERS
    stime=time.time()
    wicket_deduction = 1
    key = f"{commentary_id}_{commentary_team}"

    if key in LD_ODDEVEN_MARKETS:
        event_markets = LD_ODDEVEN_MARKETS[key]
        if not event_markets.empty:

            ldo_market_runners = LD_LDO_RUNNERS[key]
            runner_data = []
            event_data = []
            market_datalog = []
            socket_data = []

            for index, row in event_markets.iterrows():
                if row["wrStatus"] != SETTLED:
                    selection_status = NOTCREATED
                    market_runner = []
                    is_update = False
                    is_SendData = True
                    market_runner_row = pd.DataFrame()

                    if row["wrStatus"] == SUSPEND:
                        row["wrStatus"] = OPEN
                    
                    
                    updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                    if is_wicket == 1:
                        updated_market["wrPredefinedValue"] -= wicket_deduction
                    event_markets.loc[index] = updated_market
                    
                    if total_balls >= int(row["createBalls"]) and total_balls < int(row["beforeAutoCloseBalls"]) - 1 and row["wrStatus"] not in (SUSPEND, CLOSE):

                        market_id = insert_markets_to_db(row, total_balls, total_wicket, False, commentary_id, match_type_id, commentary_team)
                        if row["wrID"] != market_id:
                            is_update = True
                        row["wrID"] = market_id
                        
                    

                    if (row["wrIsOver"] and
                        (row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                         (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                         (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"]))):

                        if is_update:
                            if row["wrStatus"] == CLOSE:
                                selection_status = CLOSE
                            elif row["wrStatus"] == OPEN:
                                selection_status = OPEN
                                is_SendData = row["wrDefaultIsSendData"]
                            print("going to settle", row["wrMarketName"])

                            if total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED:
                                market_runner_row = common.fetch_runner_data(SETTLED, row)
                            else:
                                market_runner_row = common.fetch_runner_data(selection_status, row)

                            if len(market_runner_row) > 0:
                                filtered_ldo_market = ldo_market_runners[ldo_market_runners["wrMarketTemplateId"] == row["wrMarketTemplateId"]].reset_index(drop=True)
                                if total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"]:
                                    runs = common.get_market_score(commentary_id, commentary_team, int(row["wrOver"]), True)
                                    if runs is None:
                                        continue
                                    #market_runner_row["wrSelectionStatus"] = market_runner_row["wrRunner"].apply(lambda x: WIN if runs % 2==0 else LOSE)
                                    market_runner_row["wrSelectionStatus"] = market_runner_row["wrRunner"].apply( lambda x: WIN if ((runs % 2 == 0 and "even" in str(x.lower())) or (runs % 2 != 0 and "odd" in str(x.lower()))) else LOSE )
                                    id_of_winner = market_runner_row.loc[market_runner_row["wrSelectionStatus"] == WIN, "wrRunnerId"].tolist()
                                    row["wrResult"] = id_of_winner[0]
                                    selection_status = SETTLED
                                    row["wrStatus"] = SETTLED
                                    row["wrSettledTime"] = datetime.datetime.now().isoformat()

                                for _, row_ldo_market in filtered_ldo_market.iterrows():
                                    matching_row = market_runner_row[market_runner_row["wrRunner"] == row_ldo_market["wrRunner"]]
                                    runner_status = int(row["wrStatus"]) if row["wrStatus"] != SETTLED else int(matching_row["wrSelectionStatus"].iloc[0])

                                    runner_data_dict = {
                                        "wrRunner": row_ldo_market["wrRunner"],
                                        "wrLine": round(float(row_ldo_market["wrLine"]), 1),
                                        "wrOverRate": float(row_ldo_market["wrOverRate"]),
                                        "wrUnderRate": float(row_ldo_market["wrUnderRate"]),
                                        "wrBackPrice": float(row_ldo_market["wrBackPrice"]),
                                        "wrLayPrice": float(row_ldo_market["wrLayPrice"]),
                                        "wrBackSize": float(row_ldo_market["wrBackSize"]),
                                        "wrLaySize": float(row_ldo_market["wrLaySize"]),
                                        "wrSelectionStatus": runner_status,
                                        "wrEventMarketId": int(row["wrID"]),
                                        "wrRunnerId": int(matching_row["wrRunnerId"].iloc[0])
                                    }
                                    runner_data.append(runner_data_dict)
                                    row["wrIsSendData"] = is_SendData
                                    updated_runner_row = pd.DataFrame([runner_data_dict])
                                    market_runner.append(common.convert_runner_data(updated_runner_row))

                                market_runner = [runner for sublist in market_runner for runner in sublist]

                    else:
                        if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                            selection_status = CLOSE

                        if row["wrStatus"] == SUSPEND:
                            selection_status = SUSPEND

                    if is_update:
                        market_runner_row = common.fetch_runner_data(selection_status, row)

                    if len(market_runner_row) > 0:
                        row["wrIsSendData"] = is_SendData
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

                    event_markets.loc[index] = row
                    LD_ODDEVEN_MARKETS[key] = event_markets

            if runner_data and market_datalog and event_data:
                runner_json = json.dumps(runner_data)
                market_datalog_json = json.dumps(market_datalog)
                eventmarket_json = json.dumps(event_data)
                common.send_data_to_socket_async(commentary_id, socket_data)
                common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for oddeven is", time.time() - stime)
    print("closing time for oddeven is", time.time())


def process_totalruns_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls):
    print("starting time of session is ",time.time())
    stime = time.time()
    global LD_SESSION_MARKETS, LD_IPL_DATA, LD_TOTALEVENTRUN_MARKETS
    wicket_deduction = 1
    key = f"{commentary_id}"
    non_striker_team=common.get_commentary_teams(commentary_id,2)
    non_striker_team_id=non_striker_team[0]
    if key in LD_TOTALEVENTRUN_MARKETS:
        event_markets = LD_TOTALEVENTRUN_MARKETS[key]
        if not event_markets.empty:
            striker_session_markets=LD_SESSION_MARKETS[f"{commentary_id}_{commentary_team}"]
            
            nonstriker_session_markets=LD_SESSION_MARKETS[f"{commentary_id}_{non_striker_team_id}"]
            striker_session_markets = striker_session_markets[striker_session_markets["wrIsInningRun"] == True]
            nonstriker_session_markets = nonstriker_session_markets[nonstriker_session_markets["wrIsInningRun"] == True]
            striker_row=striker_session_markets.iloc[0]
            nonstriker_row=nonstriker_session_markets.iloc[0]
            striker_predicted_value=0
            iplData = LD_IPL_DATA[f"{commentary_id}_{commentary_team}_ipldata"]
            runner_data = []
            event_data = []
            market_datalog = []
            socket_data = []
            
            if total_balls == 0:
                striker_predicted_value = striker_row["wrPredefinedValue"] + current_score
            else:
                striker_predicted_value = (current_score - iplData['Cumulative'][total_balls - 1] * (1 / striker_row["wrLineRatio"])) + striker_row["wrPredefinedValue"]
    
                striker_predicted_value=round(striker_predicted_value,1)
                
            for index, row in event_markets.iterrows():
                if row["wrStatus"] != SETTLED and total_balls >= row["createBalls"]:
    
                    selection_status = NOTCREATED
                    fixed_line_value=over_value=under_value=back_price=lay_price = 0
                    lay_size=back_size = 100
                    market_runner = []
                    is_update = False
                    is_SendData = True
                    predicted_value = 0
    
                    #if row["wrStatus"] == SUSPEND:
                    #    row["wrStatus"] = OPEN
    
                    
                    updated_market, is_update = open_close_market(row, total_balls, total_wicket)
                    if is_wicket == 1:
                        updated_market["wrPredefinedValue"] -= wicket_deduction
                    event_markets.loc[index] = updated_market
    
                    if (row["wrIsOver"] and row["wrStatus"] in (OPEN, INACTIVE, SUSPEND) or
                        (row["wrStatus"] == CLOSE and total_balls >= row["beforeAutoCloseBalls"] and total_balls <= row["overBalls"]) or
                        (row["wrStatus"] == CLOSE and total_balls == row["overBalls"] + row["wrAutoResultafterBall"])):
                        
                        if nonstriker_row["wrStatus"]==SETTLED:
                            if nonstriker_row["wrResult"] is None: 
                                nonstriker_row["wrResult"]=0
                            if striker_predicted_value>nonstriker_row["wrResult"]:
                                striker_predicted_value=nonstriker_row["wrResult"]
                            predicted_value=striker_predicted_value+nonstriker_row["wrResult"]+1+row["wrPredefinedValue"]
                        else:
                            predicted_value=striker_predicted_value+nonstriker_row["wrPredefinedValue"]+row["wrPredefinedValue"]
                        if predicted_value > 0:
                            predicted_value=round(predicted_value,1)
                            
                            margin = 1 + (float(row["wrMargin"]) / 100)
                            new_predicted_value = int(predicted_value) + 0.5
                            over_value = 1 / (margin / (1 + math.exp(-(predicted_value - new_predicted_value))))
                            under_value = 1 / (margin / (1 + math.exp(predicted_value - new_predicted_value)))
                            over_value, under_value = round(over_value, 2), round(under_value, 2)
                            
                            lay_price = round(predicted_value)
                            back_price = lay_price + row["wrRateDiff"]
                            lay_size, back_size = row["wrDefaultLaySize"], row["wrDefaultBackSize"]
                            fixed_line_value = round(predicted_value, 1)
    
                            row["minodds"] = min(row["minodds"] or back_price, back_price)
                            row["maxodds"] = max(row["maxodds"] or back_price, back_price)
                            row["openvalue"] = row["openvalue"] or back_price
    
                            event_markets.loc[index] = row
                            wicket_deduction += 2
    
                            selection_status = CLOSE if row["wrStatus"] == CLOSE else OPEN
                            is_SendData = row["wrDefaultIsSendData"] if selection_status == OPEN else is_SendData
    
                    else:
                        if row["wrStatus"] == CLOSE and float(current_ball) != float(row["wrOver"]) + (float(row["wrAutoResultafterBall"]) / 10):
                            selection_status = CLOSE
                        elif row["wrStatus"] == SUSPEND:
                            selection_status = SUSPEND
    
                    if (total_balls >= row["overBalls"] + row["wrAutoResultafterBall"] and row["wrIsAutoResultSet"] and row["wrStatus"] == CLOSE and row["wrStatus"] != SETTLED):
                        row["wrStatus"] = SETTLED
                        event_markets.loc[index] = row
                        selection_status = SETTLED
                        result = common.markets_result_settlement(row, commentary_id, commentary_team)
                        if result is None:
                            continue
                        row["wrResult"] = result
                        row["wrSettledTime"] = datetime.datetime.now().isoformat()
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
    
                    event_markets.loc[index] = row
                    LD_TOTALEVENTRUN_MARKETS[key] = event_markets
            print("Execution time for session without db is", time.time() - stime)
    
            if runner_data and market_datalog and event_data:
                runner_json = json.dumps(runner_data)
                market_datalog_json = json.dumps(market_datalog)
                eventmarket_json = json.dumps(event_data)
                common.send_data_to_socket_async(commentary_id, socket_data)
                common.update_database_async(runner_json, market_datalog_json, eventmarket_json)

    print("Execution time for total event run is", time.time() - stime)
    print("closing time for total event run is", time.time())
    




# async def run_in_executor(func, *args):
#     loop = asyncio.get_running_loop()
#     return await loop.run_in_executor(None, func, *args)

# def run_in_executor(func, *args):
#     """
#     Wrapper for running tasks in ThreadPoolExecutor.
#     """
#     loop = asyncio.get_event_loop()
#     with ProcessPoolExecutor(max_workers=6) as executor:
#         return loop.run_in_executor(executor, func, *args)

# def process_task(func, *args):
#     """
#     Wrapper to run a task function with arguments in a thread.
#     """
#     try:
#         func(*args)
#     except Exception as e:
#         print(f"Error in thread {func.__name__}: {e}")
#         traceback.print_exc()
    

# def run_in_process(func, *args):
#     """
#     Wrapper to run a task function with arguments in a process.
#     """
#     return func(*args)

# async def run_all_tasks_in_executor(tasks):
#     """
#     Submit all tasks to the ThreadPoolExecutor at once and run them concurrently.
#     """
#     loop = asyncio.get_event_loop()
#     with ThreadPoolExecutor(max_workers=6) as executor:
#         # Submit all tasks to the thread pool
#         futures = [
#             loop.run_in_executor(executor, func, *args)
#             for func, *args in tasks
#         ]
#         # Wait for all futures to complete
#         await asyncio.gather(*futures)

def execute_function(func, args):
    return func(*args)



async def process_all_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id):
    total_balls = common.overs_to_balls(float(current_ball), match_type_id)
    executor = ProcessPoolExecutor()
    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(executor, process_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_over_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_sessionldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_lastdigit_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_oddeven_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_onlyover_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_onlyoverldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
        loop.run_in_executor(executor, process_totalruns_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    ]

    await asyncio.gather(*tasks)
    #Create tasks using the run_in_executor function
    # tasks = [
    #     run_in_executor(process_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     run_in_executor(process_over_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     run_in_executor(process_sessionldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     run_in_executor(process_lastdigit_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     run_in_executor(process_onlyover_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     run_in_executor(process_onlyoverldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),

    # ]

    # await asyncio.gather(*tasks)
    
    # tasks = [
    #     (process_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     (process_over_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     (process_onlyover_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     (process_sessionldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     (process_lastdigit_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    #     (process_onlyoverldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # ]
    # await run_in_executor(tasks)

    # formatted_tasks = [(task[0], task[1:]) for task in tasks]

    # with Pool(processes=1) as pool:  # Adjust the number of processes as needed
    #     pool.starmap(execute_function, formatted_tasks)
    #     pool.close()  # Close the pool to new tasks
    #     pool.join()
        
    # print("Starting threads...")

    # # Use ThreadPoolExecutor with a defined number of workers
    # with ThreadPoolExecutor(max_workers=6) as executor:
    #     futures = [executor.submit(process_task, task[0], *task[1:]) for task in tasks]

    #     # Wait for all threads to complete
    #     for future in futures:
    #         try:
    #             future.result()  # Retrieve the result to catch any exceptions raised during execution
    #         except Exception as e:
    #             print(f"Error in thread execution: {e}")

    # print("All threads have completed.")
    
    # tasks = [
    # (process_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # (process_over_session_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # (process_sessionldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # (process_lastdigit_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # (process_onlyover_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # (process_onlyoverldo_markets, current_ball, run, commentary_id, current_score, commentary_team, match_type_id, is_wicket, total_wicket, ball_by_ball_id, total_balls),
    # ]
    
    # with ThreadPoolExecutor(max_workers=6) as executor:
    #     # Submit all tasks concurrently
    #     futures = [executor.submit(process_task, task[0], *task[1:]) for task in tasks]
    
        
    


async def update_market_status(commentary_id, match_type_id, status, commentary_team, is_open_market, player_id):
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
        SESSION: 'LD_SESSION_MARKETS',
        FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
        ONLYOVER:'LD_ONLYOVER_MARKETS',
        ONLYOVERLDO:'LD_ONLYOVERLDO_MARKETS',
        OVERSESSION: 'LD_OVERSESSION_MARKETS',
        PLAYER: 'LD_PLAYER_MARKETS',
        PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
        PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
        WICKET:'LD_FOW_MARKETS',
        PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
        WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS',
        ODDEVEN:'LD_ODDEVEN_MARKETS',
        TOTALEVENTRUN:'LD_TOTALEVENTRUN_MARKETS'
    }
    
    socket_data = []
    data = []
    market_datalog = []
    if status in {OPEN, SUSPEND}:
        for category_id, global_var_name in category_ids.items():
            global_dict = globals()[global_var_name]
            key=f"{commentary_id}_{commentary_team}"
            key_total_event_run=f"{commentary_id}"
            event_market=pd.DataFrame()
            if ((category_id==TOTALEVENTRUN and key_total_event_run in global_dict) or (category_id!=TOTALEVENTRUN and key in global_dict)):
                if category_id==TOTALEVENTRUN :
                    event_market=globals()[global_var_name][key_total_event_run] 
                else:
                    event_market=globals()[global_var_name][key] 

                for index, row in event_market.iterrows():
                    try:
                        print("the wr data  of "+row["wrMarketName"]+ " "+ str(row["wrData"]))
                        if (row["wrStatus"] == OPEN and status != OPEN and
                            ((not is_open_market and bool(row["wrIsBallStart"]) is True) or is_open_market is True)):
                            if ((player_id is None) or (player_id is not None and row["wrMarketTypeCategoryId"] in (PLAYER,PLAYERBALLSFACED,PLAYERBOUNDARIES) and player_id==int(row["wrPlayerID"]) ) or (player_id is not None and row["wrMarketTypeCategoryId"] not in (PLAYER,PLAYERBALLSFACED,PLAYERBOUNDARIES) )):
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
                                    "lastupdate": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00")
                                })
                                
                                event_market_data=common.convert_event_market(row,wrdata["runner"])
                                market_datalog.append({
                                    "wrCommentaryId": commentary_id,
                                    "wrEventMarketId": row["wrID"],
                                    "wrData": event_market_data,
                                    "wrUpdateType": 1,
                                    "wrIsSendData": row['wrIsSendData']
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
                                "lastupdate": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00")
                            })
                            
                            event_market_data=common.convert_event_market(row,wrdata["runner"])
                            market_datalog.append({
                                "wrCommentaryId": commentary_id,
                                "wrEventMarketId": row["wrID"],
                                "wrData": event_market_data,
                                "wrUpdateType": 1,
                                "wrIsSendData": row['wrIsSendData']
                            })
                            
                    except Exception as e:
                        print(f"Error processing row {index}: {e}")
                        continue
                #globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=event_market
                if category_id==TOTALEVENTRUN :
                    globals()[global_var_name][key_total_event_run] =event_market
                else:
                    globals()[global_var_name][key] =event_market
        print("the data is ", data)
        if data:
            data_json = json.dumps(data)
            market_datalog_json=json.dumps(market_datalog)
            try:
                with get_db_session() as session:
                    query_update_status = text('CALL update_market_status(:data_json)')
                    session.execute(query_update_status, {"data_json": data_json})
                    query_market_datalogs=text('CALL insert_market_datalogs(:market_datalog_json)')
                    session.execute(query_market_datalogs, {"market_datalog_json": market_datalog_json})
                    session.commit()
            except SQLAlchemyError as e:
                print(f"Database update failed: {e}")
                session.rollback()
                return
    
            try:
                 common.send_data_to_socket_async(commentary_id,socket_data)
            except Exception as e:
              print(f"Error sending data to socket: {e}")
async def suspend_all_markets(data):
    for item in data:
        commentary_id = item["commentary_id"]
        match_type_id = item["match_type_id"]
        await update_market_status(commentary_id, match_type_id, SUSPEND,None, True)
        

async def end_current_innings(commentary_id, match_type_id, commentary_team):
    category_ids = {
        SESSION: 'LD_SESSION_MARKETS',
        FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
        ONLYOVER:'LD_ONLYOVER_MARKETS',
        ONLYOVERLDO:'LD_ONLYOVERLDO_MARKETS',
        PLAYER: 'LD_PLAYER_MARKETS',
        PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
        PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
        WICKET:'LD_FOW_MARKETS',
        PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
        WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS',
        OVERSESSION: 'LD_OVERSESSION_MARKETS',
        ODDEVEN:'LD_ODDEVEN_MARKETS'
    }
    
    socket_data = []
    data = []
    
    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key=f"{commentary_id}_{commentary_team}"
        if key in global_dict:
       
            event_market=globals()[global_var_name][f"{commentary_id}_{commentary_team}"] 
    
            for index, row in event_market.iterrows():
                status=CLOSE
                result=None
                if row["wrStatus"]!=SETTLED:
                    if row["wrStatus"]==CLOSE:
                        status=SETTLED
                        row["wrSettledTime"]=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00")

                        if category_id in (LASTDIGITNUMBER,ODDEVEN) and row["wrMarketTypeCategoryId"] in (LASTDIGITNUMBER,ODDEVEN):
                            runs=common.get_market_score(commentary_id,commentary_team,int(row["wrOver"]),True)
                            if runs is None:
                                continue
                            market_runner_row=common.fetch_runner_data(SETTLED,row)
                            if category_id==LASTDIGITNUMBER:
                                market_runner_row['wrSelectionStatus'] = market_runner_row['wrRunner'].apply(lambda x: WIN if x == str(runs%10) else LOSE)
                                
                            else:
                                market_runner_row["wrSelectionStatus"] = market_runner_row["wrRunner"].apply( lambda x: WIN if (runs % 2 == 0 and "even" in str(x.lower())) or (runs % 2 != 0 and "odd" in str(x.lower())) else LOSE )
                                #market_runner_row["wrSelectionStatus"] = market_runner_row["wrRunner"].apply(lambda x: WIN if runs % 2==0 else LOSE)
                            id_of_winner= market_runner_row.loc[market_runner_row['wrSelectionStatus'] == WIN, 'wrRunnerId'].tolist()
                            result=id_of_winner[0]
                                
                        elif category_id==WICKET and row["wrMarketTypeCategoryId"]==WICKET:
                            result=common.team_total_score(commentary_id,commentary_team)
                            if result is None:
                                continue
                        else:
                            result=common.markets_result_settlement(row,commentary_id,commentary_team, 0,True)
                            if result is None:
                                continue
                        row["wrResult"]=result

                    row["wrStatus"] = status
                    if(row["wrData"] is not None and row["wrData"] not in  ("0",0)):                         
                        wrdata = json.loads(row["wrData"])
                        wrdata["status"] = status
                        if (category_id in (LASTDIGITNUMBER,ODDEVEN) and status==SETTLED):
                            for item in wrdata["runner"]:
                                if item["runnerId"]==row["wrResult"]:
                                    item["status"]=WIN
                                else:
                                    item["status"]=LOSE
                        else:
                            for item in wrdata["runner"]:
                                item["status"]=status
                        row["wrData"] = json.dumps(wrdata)
            
                        json_socket_data = common.convert_socket_data(row, wrdata["runner"])
                        socket_data.append(json_socket_data)
                        event_market.loc[index] = row
                        data.append({
                            "eventid": row["wrID"],
                            "status": status,
                            "data": row["wrData"],
                            "issenddata": True,
                            "lastupdate": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00"),
                            "result":int(result) if result is not None else result,
                            "markettypecategoryid":row['wrMarketTypeCategoryId']
                        })
            globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=event_market

    # query=text('select * from "tbleventmarkets" where "wrcommentaryid"=:commentary_id and "wrteamid"=:strike_team and "wrstatus" not in (:close, :settled) and "wrmarkettypecategoryid" in (:player, :wicket, :player_boundaries)')
    # with get_db_session() as session:
    #     try:
    #         player_markets=pd.read_sql(query, con=session.bind, params={"commentary_id": commentary_id, "strike_team":commentary_team,"close":close,"settled":settled, "player":player, "wicket":wicket,"player_boundaries":playerboundaries})
    #         if(len(data)>0):
    #             for index, row in player_markets.iterrows():
    #                 if(row["wrdata"] is not none):
    #                     row["wrstatus"] = close
    #                     wrdata = json.loads(row["wrdata"])
    #                     wrdata["status"] = close
    #                     for item in wrdata["runner"]:
    #                         item["status"]=close
    #                     row["wrdata"] = json.dumps(wrdata)
                        
    #                     json_socket_data = common.convert_socket_data(row, wrdata["runner"],true)
    #                     socket_data.append(json_socket_data)
    #                     data.append({
    #                         "eventid": row["wrid"],
    #                         "status": close,
    #                         "data": row["wrdata"],
    #                         "issenddata": true,
    #                         "lastupdate": datetime.datetime.now().strftime("%y-%m-%dt%h:%m:%s.%f+00")
    #                     })
            
        
        
    #     except sqlalchemyerror as e:
    #         print(f"error fetching data for end innings: {e}")
            
    if data:
        data_json = json.dumps(data)
        with get_db_session() as session:
            try:
                session.execute(text('CALL update_end_innings_status(:data_json)'), {'data_json': data_json})
            except SQLAlchemyError as e:
                print(f"Database update failed: {e}")

    common.send_data_to_socket_async(commentary_id,socket_data)


async def undo_score(current_ball,run,commentary_id,current_score,commentary_team,match_type_id,is_wicket,total_wicket,ball_by_ball_id):
    await update_player_market_status(commentary_id,commentary_team,SUSPEND)   
    await update_market_status(commentary_id, match_type_id, SUSPEND, commentary_team, False, None)
    #await process_all_markets(current_ball, run, commentary_id, current_score, commentary_team, match_type_id, False,total_wicket,ball_by_ball_id)          


async def set_market_manual_close(commentary_id,match_type_id,commentary_team,status,event_market_id):
    category_ids = {
        SESSION: 'LD_SESSION_MARKETS',
        FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
        ONLYOVER:'LD_ONLYOVER_MARKETS',
        ONLYOVERLDO:'LD_ONLYOVERLDO_MARKETS',
        PLAYER:'LD_PLAYER_MARKETS',
        PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
        PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
        WICKET:'LD_FOW_MARKETS',
        PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
        WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS',
        OVERSESSION: 'LD_OVERSESSION_MARKETS',
        ODDEVEN:'LD_ODDEVEN_MARKETS',
        TOTALEVENTRUN:'LD_TOTALEVENTRUN_MARKETS'
    }
    
    socket_data = []
    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key=f"{commentary_id}_{commentary_team}"
        key_total_event_run=f"{commentary_id}"
        event_market=pd.DataFrame()
        if ((category_id==TOTALEVENTRUN and key_total_event_run in global_dict) or (category_id!=TOTALEVENTRUN and key in global_dict)):
            if category_id==TOTALEVENTRUN :
                event_market=globals()[global_var_name][key_total_event_run] 
            else:
                event_market=globals()[global_var_name][key] 
            if(status in (CLOSE,CANCEL)):
                for index,row in event_market.iterrows():
        
                    if(row["wrID"]==event_market_id):
                        row["wrStatus"]=status
                        wrdata=json.loads(row["wrData"])
                        wrdata["status"]=status
                        runner=wrdata["runner"][0]
                        runner["status"]=status
                        wrdata["runner"][0]=runner
                        row["wrData"]=json.dumps(wrdata)
                        json_socket_data = common.convert_socket_data(row, wrdata["runner"])
                        socket_data.append(json_socket_data)
                        event_market.loc[index] = row
                    
                    
            #globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=event_market
            if category_id==TOTALEVENTRUN :
                globals()[global_var_name][key_total_event_run] =event_market
            else:
                globals()[global_var_name][key] =event_market
    common.send_data_to_socket_async(commentary_id,socket_data)
    

async def set_market_manual_result(commentary_id,commentary_team,status,event_market_id, result):
    category_ids = {
        SESSION: 'LD_SESSION_MARKETS',
        FANCYLDO: 'LD_SESSIONLDO_MARKETS',
        LASTDIGITNUMBER: 'LD_LASTDIGIT_MARKETS',
        ONLYOVER:'LD_ONLYOVER_MARKETS',
        ONLYOVERLDO:'LD_ONLYOVERLDO_MARKETS',
        PLAYER:'LD_PLAYER_MARKETS',
        PLAYERBOUNDARIES:'LD_PLAYERBOUNDARY_MARKETS',
        PLAYERBALLSFACED:'LD_PLAYERBALLSFACED_MARKETS',
        WICKET:'LD_FOW_MARKETS',
        PARTNERSHIPBOUNDARIES:'LD_PARTNERSHIPBOUNDARIES_MARKETS',
        WICKETLOSTBALLS:'LD_WICKETLOSTBALLS_MARKETS',
        OVERSESSION: 'LD_OVERSESSION_MARKETS',
        ODDEVEN:'LD_ODDEVEN_MARKETS',
        TOTALEVENTRUN:'LD_TOTALEVENTRUN_MARKETS'
    }
    
    socket_data = []
    for category_id, global_var_name in category_ids.items():
        global_dict = globals()[global_var_name]
        key=f"{commentary_id}_{commentary_team}"
        key_total_event_run=f"{commentary_id}"
        event_market=pd.DataFrame()
        if ((category_id==TOTALEVENTRUN and key_total_event_run in global_dict) or (category_id!=TOTALEVENTRUN and key in global_dict)):
            if category_id==TOTALEVENTRUN :
                event_market=globals()[global_var_name][key_total_event_run] 
            else:
                event_market=globals()[global_var_name][key] 
                
            if(status in (CLOSE,CANCEL)):
                for index,row in event_market.iterrows():
        
                    if(row["wrID"]==event_market_id):
                        row["wrStatus"]=status
                        row["wrResult"]=result
                        wrdata=json.loads(row["wrData"])
                        wrdata["status"]=status
                        runner=wrdata["runner"][0]
                        runner["status"]=status
                        wrdata["runner"][0]=runner
                        row["wrData"]=json.dumps(wrdata)
                        json_socket_data = common.convert_socket_data(row, wrdata["runner"])
                        socket_data.append(json_socket_data)
                        event_market.loc[index] = row
                    
                    
            #globals()[global_var_name][f"{commentary_id}_{commentary_team}"]=event_market
            if category_id==TOTALEVENTRUN :
                globals()[global_var_name][key_total_event_run] =event_market
            else:
                globals()[global_var_name][key] =event_market
    #await common.send_data_to_socket_async(commentary_id,socket_data)
    
def end_current_commentary(commentary_id):
   
    global LD_SESSION_MARKETS, LD_SESSIONLDO_MARKETS, LD_LASTDIGIT_MARKETS, LD_ONLYOVER_MARKETS, LD_ONLYOVERLDO_MARKETS, LD_OVERSESSION_MARKETS, LD_ODDEVEN_MARKETS

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
        TOTALEVENTRUN
    ]:
        keys_to_delete = [key for key in global_dict.keys() if key.startswith(f"{commentary_id}")]
        
        # Remove the keys
        for key in keys_to_delete:
            del global_dict[key]
def update_market_line(commentary_id, match_type_id, strike_team, data, current_score, ball):
    global LD_IPL_DATA
    
    iplData=LD_IPL_DATA[f"{commentary_id}_{strike_team}_ipldata"]

    current_balls = 0
    socket_data = []
    balls_per_over = common.get_balls_per_over(match_type_id)
    if int(ball) == 0:
        current_balls = int(ball * 10)
    else:
        current_balls = int((round((ball - int(ball)), 1) * 10) + (int(ball) * balls_per_over))
    
    #query_tblrunner = text('UPDATE "tblMarketRunners" SET "wrLine"=:line, "wrLastUpdate"=:last_update WHERE "wrEventMarketId"=:event_market_id')
    #query_tbleventmarket = text('UPDATE "tblEventMarkets" SET "wrLastUpdate"=:last_update, "wrOpenOdds"=:open_odds, "wrMinOdds"=:min_odds, "wrMaxOdds"=:max_odds, "wrPredefinedValue"=:predefined_value WHERE "wrID"=:id')
    #query_market_datalog = text('INSERT INTO "tblMarketDataLogs" ("wrCommentaryId", "wrEventMarketId", "wrData", "wrUpdateType", "wrCreatedDate") VALUES (:commentary_id, :event_market_id, :data, :update_type, :created_date)')
    query_tbleventmarket = text('UPDATE "tblEventMarkets" SET "wrLastUpdate"=:last_update, "wrPredefinedValue"=:predefined_value WHERE "wrID"=:id')
    category_ids = {
        SESSION: 'LD_SESSION_MARKETS',
        ONLYOVER:'LD_ONLYOVER_MARKETS',
        OVERSESSION: 'LD_OVERSESSION_MARKETS',
        TOTALEVENTRUN:'LD_TOTALEVENTRUN_MARKETS'
    }
    sorted_overs = sorted(data, key=lambda x: x['over'])
    print("the sorted overs are ",sorted_overs)
    for category_id, global_var_name in category_ids.items():
        for row in sorted_overs:

            global_dict = globals()[global_var_name]
            updated_team_id=row["team_id"]
            key=f"{commentary_id}_{strike_team}"
            key_total_event_run=f"{commentary_id}"
            event_market=pd.DataFrame()
            if ((category_id==TOTALEVENTRUN and key_total_event_run in global_dict) or (category_id!=TOTALEVENTRUN and key in global_dict)):
                if category_id==TOTALEVENTRUN :
                    event_market=globals()[global_var_name][key_total_event_run] 
                else:
                    event_market=globals()[global_var_name][key] 
                 
                ##with get_db_session() as session:
                if row["market_type_category_id"] in (ONLYOVER,SESSION,OVERSESSION,TOTALEVENTRUN):
                    for index, market_row in event_market.iterrows():
                        try:
                            predicted_value = 0
                            if row["is_onlyover"] == 1 and row["market_id"] == market_row["wrID"]  and market_row["wrMarketTypeCategoryId"]==ONLYOVER:
                                predicted_value = market_row["wrPredefinedValue"] + row["line_diff"]
                                market_row["wrPredefinedValue"] = predicted_value
                                market_row["wrIsActive"] = row["is_active"]
                                market_row["wrIsSendData"] = row["is_senddata"]
                                market_row["wrIsAllow"] = row["is_allow"]
                                market_row["wrRateDiff"] = row["rate_diff"]
                                market_row["wrDefaultLaySize"] = row["lay_size"]
                                market_row["wrDefaultBackSize"] = row["back_size"]
                                market_row["wrData"] = row["data"]
                                if row["line_ratio"] != 0:
                                    market_row["wrLineRatio"] = row["line_ratio"]

                                event_market.loc[index] = market_row
                            elif row["is_onlyover"] == 0 and row["market_id"] == market_row["wrID"] and market_row["wrMarketTypeCategoryId"] in (SESSION, OVERSESSION, TOTALEVENTRUN):
                                #if row["value"] > 0:
                                market_row["wrPredefinedValue"] = market_row["wrPredefinedValue"] + row["line_diff"]
                                if row["line_ratio"] != 0:
                                    market_row["wrLineRatio"] = row["line_ratio"]
                                market_row["wrIsActive"] = row["is_active"]
                                market_row["wrIsSendData"] = row["is_senddata"]
                                market_row["wrIsAllow"] = row["is_allow"]
                                market_row["wrRateDiff"] = row["rate_diff"]
                                market_row["wrDefaultLaySize"] = row["lay_size"]
                                market_row["wrDefaultBackSize"] = row["back_size"]
                                market_row["wrData"] = row["data"]
                                
                                event_market.loc[index] = market_row
                                
                                if category_id!=TOTALEVENTRUN:
                                    if current_balls > 0:
                                        predicted_value = (current_score - iplData['Cumulative'][current_balls - 1] * (1 / market_row["wrLineRatio"])) + market_row["wrPredefinedValue"]
                                    elif current_balls==0:
                                        predicted_value = market_row["wrPredefinedValue"] #+ row["value"]
                                        market_row["wrPredefinedValue"] = predicted_value
            
                            if predicted_value > 0:
                                fancy_line_value = round(predicted_value)
                                fixed_line_value = round(predicted_value, 2)
                                
                                if market_row["wrMinOdds"] == 0 or market_row["wrMinOdds"] > (fancy_line_value + 1):
                                    market_row["wrMinOdds"] = fancy_line_value + 1
                                if market_row["wrMaxOdds"] == 0 or market_row["wrMaxOdds"] < (fancy_line_value + 1):
                                    market_row["wrMaxOdds"] = fancy_line_value + 1 
                                
                                # session.execute(query_tblrunner, {
                                #     'line': fixed_line_value,
                                #     'last_update': datetime.datetime.now(),
                                #     'event_market_id': market_row['wrID']
                                # })
                                
                                # try:
                                #     market_runner_row = pd.read_sql_query(text('SELECT * FROM "tblMarketRunners" WHERE "wrEventMarketId"=:id'), con=session.bind, params={"id":market_row['wrID']})
                                # except Exception as e:
                                #     print(f"Failed to load market runner data for wrEventMarketId {market_row['wrID']}: {e}")
                                #     continue
        
                                #market_runner = convert_runner_data(market_runner_row)
                                #json_event_market = convert_event_market(market_row, market_runner)
                                #json_socket_data = convert_socket_data(market_row, market_runner)
                                
                                
                                ##event_market.loc[index] = market_row
                                
                                ##session.execute(query_tbleventmarket, {
                                    #'data': json_event_market,
                                ##    'last_update': datetime.datetime.now(),
                                    #'open_odds': market_row["openvalue"],
                                    #'min_odds': market_row["minodds"],
                                    #'max_odds': market_row["maxodds"],
                                ##    'predefined_value': market_row["wrPredefinedValue"],
                                 ##   'id': market_row['wrID']
                               ## })
                               
                               
                                # session.execute(query_market_datalog, {
                                #     'commentary_id': commentary_id,
                                #     'event_market_id': market_row["wrID"],
                                #     'data': json_event_market,
                                #     'update_type': 1,
                                #     'created_date': datetime.datetime.now()
                                # })
                                
                                # socket_data.append(json_socket_data)
                    
                        except Exception as e:
                            print(f"Failed to update market row {market_row['wrID']}: {e}")
                            continue
    
                # try:
                #     send_data_to_socket({
                #         "commentaryId": commentary_id,
                #         "marketData": socket_data
                #     })
                # except Exception as e:
                #     print(f"Failed to send data to socket: {e}")
                
            try:
                LD_IPL_DATA[f"{commentary_id}_{strike_team}_ipldata"]=iplData
                #globals()[global_var_name][f"{commentary_id}_{updated_team_id}"]=event_market 
                if category_id==TOTALEVENTRUN :
                    globals()[global_var_name][key_total_event_run] =event_market
                else:
                    globals()[global_var_name][key] =event_market
            except Exception as e:
                print(f"Failed to save IPL data or market template: {e}")

def update_tables(match_type_id,is_template):
   
    files=os.listdir('data')
    for file_name in files:
        #parts=file_name.split('_')
        parts=[part for subpart in file_name.split('_') for part in subpart.split('.')]
        if len(parts)>=4 and parts[1].isdigit():
            if parts[1]==str(match_type_id) and parts[3]=='fancymarkettemplate' and is_template==True:
                #commentary_team=get_commentary_teams(commentary_id)
                #query='select "wrTeamId","wrTeamScore" from "tblCommentaryTeams" where "wrCommentaryId"=%s and "wrTeamStatus"=1'
                #commentary_team_df=pd.read_sql_query(query,connection,params=(commentary_id,))
                
                #cursor=connection.cursor()
                #cursor.execute(query,(commentary_id,))
                #strike_team=commentary_team_df['wrTeamId'][0]
                
                previous_template=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH,str(parts[0])+"_"+str(match_type_id)+"_"+str(parts[2])+"_eventmarkets.csv"))
                match_status=dict(zip(previous_template['wrID'],previous_template['status']))
                event_id=previous_template['wrEventRefID']
               # get_market_template(parts[0],match_type_id,parts[2],event_id[0])
                
                new_template=pd.read_csv(os.path.join(DATA_TEMP_BASE_PATH,str(parts[0])+"_"+str(match_type_id)+"_"+str(parts[2])+"_eventmarkets.csv"))
                new_template['status']=new_template['wrID'].map(match_status)
                
                new_template.to_csv(os.path.join(DATA_TEMP_BASE_PATH,str(parts[0])+"_"+str(match_type_id)+"_"+str(parts[2])+"_eventmarkets.csv"), index=False)
        
                new_template=calculations(parts[0],match_type_id,parts[2],new_template)
                
                new_template.to_csv(os.path.join(DATA_TEMP_BASE_PATH,str(parts[0])+"_"+str(match_type_id)+"_"+str(parts[2])+"_eventmarkets.csv"), index=False)
            # elif parts[1]==str(match_type_id) and is_template==False:
            #     process_ipl_data(parts[0], match_type_id,0, False)