import logging
from typing import Dict
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
from score.services.score.calculations import end_current_commentary, process_all_markets, load_commentary_data, suspend_all_markets, undo_score, update_market_line, update_market_status, update_tables, end_current_innings, set_market_manual_close, set_market_manual_result
from score.services.score.common import insert_logs
from score.api.schema.models import EndCommentaryModel, FailureResponse, LoadCommentary, PredictPlayerScore, PredictScore, SuccessResponse, SuspendAllMarket, UndoScoreModel, UpdateLine, UpdateMarketPredictor, UpdateMarketStatus, UpdatePlayerStatus,UpdatePlayerLine, EndInningsModel, MarketManualClose, MarketManualSettle 
from score.services.score.player import run,update_player_market_status, update_player_markets_line

import os
import csv
import time
from score.settings.base import DATA_TEMP_BASE_PATH
import asyncio
from threading import Lock

LOG = logging.getLogger('app')
router = APIRouter(tags=["Score Predictor"])

@router.get("/check/")
def check():
    return SuccessResponse(status_code=200)

processing_commentaries = {}
processing_lock = Lock()

@router.post("/loadcommentary/")
def load_commentary(data: LoadCommentary = Body(default=None)):
    try:
        data = data.dict()
        commentary_id = data.get("commentary_id")
        match_type_id = data.get("match_type_id")
        event_id = data.get("event_id")
        ratio_data = data.get("ratio_data")
        default_balls_faced = data.get("default_ball_faced")
        default_player_boundaries = data.get("default_player_boundaries")
        default_player_runs = data.get("default_player_runs")

        insert_logs(commentary_id, "loadcommentary", str(data))

        with processing_lock:
            if processing_commentaries.get(commentary_id):
                return FailureResponse(status_code=409, error_msg=f"Processing already in progress for commentary_id: {commentary_id}")
            processing_commentaries[commentary_id] = True

        load_commentary_data(commentary_id, match_type_id, event_id, True,
                             ratio_data, default_balls_faced,
                             default_player_boundaries, default_player_runs)

        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id, "loadcommentary", str(e))

    finally:
        with processing_lock:
            processing_commentaries.pop(commentary_id, None)

    return response

@router.post("/predictscore/")
async def predict_score(data: PredictScore = Body(default=None)):    
    try:
        data=data.dict()
        predictscore = data.get("predictscore")

        insert_logs(predictscore.get("commentary_id"),"before execution predictscore",str(data))

        print("data ", data)
        ball=predictscore.get("ball")
        score_per_ball=predictscore.get("run")
        commentary_id=predictscore.get("commentary_id")
        total_score=predictscore.get("total_score")
        strike_team_id=predictscore.get("strike_team_id")
        match_type_id=predictscore.get("match_type_id")
        is_wicket=predictscore.get("wicket")
        total_wicket=predictscore.get("total_wicket")
        ball_by_ball_id=predictscore.get("ball_by_ball_id")
        #insert_logs(commentary_id,"predictscore",str(data))

        playerpredictscore = data.get("playerpredictscore")

        #commentary_id = data["commentary_id"]
        match_type_id = playerpredictscore["match_type_id"]
        event_id = playerpredictscore["event_id"]
        current_team_id = playerpredictscore["current_team_id"]
        current_score = playerpredictscore["total_score"]
        current_ball = playerpredictscore["current_ball"]
        #ball_by_ball_id=data["ball_by_ball_id"]
        
        
        await asyncio.gather(
            run(commentary_id,current_team_id,match_type_id,current_ball,current_score,event_id, playerpredictscore["player_details"],playerpredictscore["partnership_details"],ball_by_ball_id),
            process_all_markets(ball, score_per_ball, commentary_id, total_score, strike_team_id, match_type_id, is_wicket, total_wicket, ball_by_ball_id)
        )
        
        insert_logs(commentary_id,"predictscore",str(data))
        #player_prediction = PlayerPrediction(commentary_id, match_type_id, event_id, current_team_id, current_score, current_ball)
        #response_msg = player_prediction.run(data["player_details"])
        # await run(commentary_id,current_team_id,match_type_id,current_ball,current_score,event_id, data["player_details"],data["partnership_details"],ball_by_ball_id)
        
        # await process_all_markets(ball,run,commentary_id,total_score,strike_team_id,match_type_id,is_wicket,total_wicket, ball_by_ball_id)
        
            
        response = SuccessResponse(status_code=200)

    except Exception as err:
        response = FailureResponse(status_code=500, error_msg=str(err))
        insert_logs(commentary_id,"predictscore",str(err))


    return response

@router.post("/playerpredictscore/")
async def player_predict_score(data: PredictPlayerScore = Body(default=None)):    
    try:
        data = data.dict()
        print("data ", data)
        commentary_id = data["commentary_id"]
        match_type_id = data["match_type_id"]
        event_id = data["event_id"]
        current_team_id = data["current_team_id"]
        current_score = data["total_score"]
        current_ball = data["current_ball"]
        ball_by_ball_id=data["ball_by_ball_id"]
        insert_logs(commentary_id,"playerpredictscore",str(data))
        
        #player_prediction = PlayerPrediction(commentary_id, match_type_id, event_id, current_team_id, current_score, current_ball)
        #response_msg = player_prediction.run(data["player_details"])
        await run(commentary_id,current_team_id,match_type_id,current_ball,current_score,event_id, data["player_details"],data["partnership_details"],ball_by_ball_id)
        
        # if response_msg and response_msg.get("status"):
        #     response = SuccessResponse(status_code=200, msg=response_msg.get("msg"))
        # else:
        #     response = FailureResponse(status_code=500, error_msg=response_msg.get("msg", ""))
        response = SuccessResponse(status_code=200)
    except Exception as err:
        response = FailureResponse(status_code=500, error_msg=str(err))
        insert_logs(commentary_id,"playerpredictscore",str(err))


    return response

@router.post("/updateplayerstatus/")
async def update_player_status(data: UpdatePlayerStatus= Body(default=None)):    
    try:
        data = data.dict()
        print("data ", data)
        commentary_id = data["commentary_id"]
        event_market_id = data["event_market_id"]
        status = data["status"]
        
        insert_logs(commentary_id,"updateplayerstatus",str(data))
        #player_status= PlayerPrediction()
        await update_player_market_status(commentary_id,None,status)        
        response = SuccessResponse(status_code=200)

       
    except Exception as err:
        response = FailureResponse(status_code=500, error_msg=str(err))
        insert_logs(commentary_id,"updateplayerstatus",str(err))


    return response

@router.post("/updatemarketstatus/")
async def update_market_status_code(data: UpdateMarketStatus = Body(default=None)):    
    try:
        data = data.dict()
        commentary_id=data.get("commentary_id")
        status=data.get("status")
        match_type_id=data.get("match_type_id")
        is_open_market=data.get("is_open_market")
        player_id=data.get("player_id")
        insert_logs(commentary_id,"updatemarketstatus",str(data))

        await update_market_status(commentary_id,match_type_id,status,None, is_open_market,player_id)
        
        
         
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"updatemarketstatus",str(e))

    return response


@router.post("/updatemarketpredictors/")
def update_market_predictors(data: UpdateMarketPredictor = Body(default=None)):    
    try:
        data = data.dict()
        match_type_id=data.get("match_type_id")
        is_market_template=data.get("is_market_template")
        update_tables(match_type_id,is_market_template)
        response = SuccessResponse(status_code=200)
    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))

    return response


@router.post("/suspendallmarkets/")
async def suspend_markets(data: SuspendAllMarket = Body(default=None)):    
    try:
        data = data.dict()
        commentary_id = data.get("commentary_id")
        match_type_id = data.get("match_type_id")
        insert_logs(commentary_id,"suspendallmarkets",str(data))

        await suspend_all_markets(data)
        
        
         
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"suspendallmarkets",str(e))

    return response

@router.post("/updateline/")
def update_line(data: UpdateLine = Body(default=None)):    
    try:
        data = data.dict()
        print("data ", data)
        commentary_id=data.get("commentary_id")
        match_type_id=data.get("match_type_id")
        strike_team=data.get("strike_team_id")
        values=data.get("overs")
        current_score=data.get("current_score")
        current_over=data.get("current_over")
        insert_logs(commentary_id,"updateline",str(data))

        update_market_line(commentary_id,match_type_id,strike_team,values,current_score,current_over)
        
        
         
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"updateline",str(e))

    return response

@router.post("/updateplayerline/")
def update_player_line(data: UpdatePlayerLine= Body(default=None)):    
    try:
        data = data.dict()
        print("data ", data)
        commentary_id = data["commentary_id"]
        strike_team_id = data["strike_team_id"]
        match_type_id = data["match_type_id"]
        players_data=data["players"]
        fall_of_wicket_data=data["fallOfWicket"]
        partnership_data=data["partnershipBoundaries"]
        lostballs_data=data["wicketLostBalls"]
        insert_logs(commentary_id,"updateplayerline",str(data))
        update_player_markets_line(commentary_id,strike_team_id,players_data, fall_of_wicket_data,partnership_data,lostballs_data)
        #player_status = PlayerPrediction(commentary_id, match_type_id,None , strike_team_id, None, 0)
        #player_status= PlayerPrediction()
        # response_msg = player_status.update_player_markets_line(players_data)
        
        
        # with open(os.path.join(DATA_TEMP_BASE_PATH, str(commentary_id)+"_timecomsumed.csv"), mode='a', newline='') as file:
        #     writer = csv.writer(file)
            
        #     writer.writerow(['updateplayerline', str(time.time()-start_time)])
         
        
        # if response_msg.get("status"):
        #     response = SuccessResponse(status_code=200, msg=response_msg.get("msg"))
        # else:
        #     response = FailureResponse(status_code=500, error_msg=response_msg.get("msg", ""))
        response = SuccessResponse(status_code=200)
    except Exception as err:
        response = FailureResponse(status_code=500, error_msg=str(err))
        insert_logs(commentary_id,"updateplayerline",str(err))


    return response

@router.post("/marketmanualclose/")
async def market_manual_close(data: MarketManualClose = Body(default=None)):    
    try:
        data = data.dict()
        commentary_id=data.get("commentary_id")
        status=data.get("status")
        match_type_id=data.get("match_type_id")
        event_market_id=data.get("event_market_id")
        strike_team=data.get("strike_team")
        insert_logs(commentary_id,"marketmanualclose",str(data))

        await set_market_manual_close(commentary_id,match_type_id,strike_team,status,event_market_id)
        
        
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"marketmanualclose",str(e))

    return response

@router.post("/marketmanualsettle/")
async def market_manual_settle(data: MarketManualSettle = Body(default=None)):    
    try:
        data = data.dict()
        commentary_id=data.get("commentary_id")
        status=data.get("status")
        event_market_id=data.get("event_market_id")
        strike_team=data.get("strike_team")
        result=data.get("result")
        
        insert_logs(commentary_id,"marketmanualclose",str(data))

        await set_market_manual_result(commentary_id,strike_team,status,event_market_id, result)
         
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"marketmanualsettle",str(e))

    return response

@router.post("/endcommentary/")
def end_commentary(data: EndCommentaryModel = Body(default=None)):    
    try:
        data = data.dict()
        commentary_id=data.get("commentary_id")
        insert_logs(commentary_id,"endcommentary",str(data))

        end_current_commentary(commentary_id)
        
        
       
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"endcommentary",str(e))

    return response

@router.post("/endinnings/")
async def end_innings(data: EndInningsModel = Body(default=None)):    
    try:
        data = data.dict()
        commentary_id=data.get("commentary_id")
        strike_team_id=data.get("strike_team_id")
        match_type_id=data.get("match_type_id")
        
        insert_logs(commentary_id,"endinnings",str(data))

        await end_current_innings(commentary_id,match_type_id,strike_team_id)
       
        
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"endinnings",str(e))

    return response

@router.post("/undoscore/")
async def undoscore(data: UndoScoreModel = Body(default=None)):    
    try:
        data = data.dict()
        ball=data.get("ball")
        run=data.get("run")
        commentary_id=data.get("commentary_id")
        total_score=data.get("total_score")
        strike_team_id=data.get("strike_team_id")
        match_type_id=data.get("match_type_id")
        is_wicket=data.get("wicket")
        total_wicket=data.get("total_wicket")
        ball_by_ball_id=data.get("ball_by_ball_id")

        insert_logs(commentary_id,"undoscore",str(data))

        await undo_score(ball,run,commentary_id,total_score,strike_team_id,match_type_id,is_wicket,total_wicket,ball_by_ball_id)
        
        
        
         
        
        response = SuccessResponse(status_code=200)

    except Exception as e:
        response = FailureResponse(status_code=500, error_msg=str(e))
        insert_logs(commentary_id,"undoscore",str(e))

    return response
