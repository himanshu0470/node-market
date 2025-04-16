
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
from score.services.score.config import CLOSE, OPEN, SUSPEND, NOTCREATED, SETTLED, CANCEL, WIN, LOSE, BALL, OVER, INACTIVE, INPLAY, PREMATCHINPLAY, AUTO, ONLYOVER, PLAYER, WICKET, FANCYLDO, OVERSESSION, SESSION, ONLYOVERLDO, LASTDIGITNUMBER, PLAYERBOUNDARIES, PLAYERBALLSFACED, PARTNERSHIPBOUNDARIES, WICKETLOSTBALLS, DYNAMICLINE ,STATICLINE ,ODDEVEN , TOTALEVENTRUN, sio
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
    create_only_overs(commentary_id,match_type_id,commentary_team,event_id,team_shortname)
    create_ldo_lottery_markets(commentary_id,match_type_id,commentary_team,event_id,team_shortname)
    create_dynamic_markets(commentary_id,match_type_id,commentary_team,event_id)
    create_over_session_markets(commentary_id,match_type_id,commentary_team,event_id,team_shortname)
    get_totalrun_markets(commentary_id,commentary_team,match_type_id,event_id)
    get_fancy_ldo_values(commentary_id, match_type_id, commentary_team)
    calculations(commentary_id, match_type_id, commentary_team, team_shortname)
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
