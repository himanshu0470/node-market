from typing import Optional,List
from pydantic import BaseModel

class LineRatioDataItem(BaseModel):
    over: Optional[float]=None
    line_ratio: Optional[float]=None
    line_diff:Optional[float]=None
    is_onlyover: Optional[int]=None
    is_allow:bool
    is_senddata:bool
    is_active:bool
    data:str
    market_type_category_id:int
    lay_size: int
    back_size: int
    rate_diff: int
    team_id:Optional[int]=None
    market_id:int

class LoadCommentaryDataItem(BaseModel):
    over: Optional[int]=None
    line_ratio: Optional[float]=None
    value:Optional[int]=None
    is_onlyover: Optional[int]=None
    
    
class LoadCommentary(BaseModel):
    commentary_id: int
    match_type_id: int
    event_id: int
    #line_ratio_data: Optional[List[LoadCommentaryDataItem]]=None
    default_ball_faced:int
    default_player_boundaries:int
    default_player_runs:int




class PredictSessionScore(BaseModel):
    ball: float
    run: int
    commentary_id: int
    total_score: int
    strike_team_id: int
    match_type_id: int
    wicket: int
    total_wicket: int
    ball_by_ball_id: int

class PlayerRunDetails(BaseModel):
    player_id: int
    player_name: str
    team_id: int
    batRun: int
    current_boundaries: int
    isWicket: bool
    balls_faced:int

class PartnershipDetails(BaseModel):
    partnership_no: int
    partnership_boundaries:int

class PredictPlayerScore(BaseModel):
    commentary_id: int
    match_type_id: int
    event_id: str
    current_team_id: int
    total_score: int   
    current_ball: float
    ball_by_ball_id:int
    player_details: List[PlayerRunDetails]
    partnership_details:List[PartnershipDetails]
    
class PredictScore(BaseModel):
    predictscore:PredictSessionScore
    playerpredictscore:PredictPlayerScore
    
class UpdatePlayerStatus(BaseModel):
    commentary_id: int
    event_market_id: List[int]
    status: int

class UpdateMarketStatus(BaseModel):
    commentary_id: int
    match_type_id: int
    status: int
    is_open_market: bool
    player_id:Optional[int]=None

class UpdateMarketPredictor(BaseModel):
    match_type_id: int
    is_market_template: int

class SuspendAllMarket(BaseModel):
    commentary_id: int
    match_type_id: int

class UpdateLine(BaseModel):
    commentary_id: int
    match_type_id: int
    strike_team_id: int
    overs: Optional[List[LineRatioDataItem]]=None
    current_score: int
    current_over: float

class PlayersLineItems(BaseModel):
    commentary_player_id:int
    market_type_category_id: int
    line: float 
    is_allow:bool
    is_senddata:bool
    is_active:bool
    data:str
    lay_size: int
    back_size: int
    rate_diff: int
    line_diff: float

class FallOfWicketLineItems(BaseModel):
    market_id:int
    market_type_category_id: int
    line: float 
    is_allow:bool
    is_senddata:bool
    is_active:bool
    data:str
    lay_size: int
    back_size: int
    rate_diff: int
    line_diff: float
  
class PartnershipBoundariesLineItems(BaseModel):
    market_id:int
    market_type_category_id: int
    line: float 
    is_allow:bool
    is_senddata:bool
    is_active:bool
    data:str
    lay_size: int
    back_size: int
    rate_diff: int
    line_diff: float
    
class WicketLostBallsLineItems(BaseModel):
    market_id:int
    market_type_category_id: int
    line: float 
    is_allow:bool
    is_senddata:bool
    is_active:bool
    data:str
    lay_size: int
    back_size: int
    rate_diff: int
    line_diff: float
    
class UpdatePlayerLine(BaseModel):
    commentary_id: int
    match_type_id: int
    strike_team_id: int
    players: Optional[List[PlayersLineItems]]=None
    fallOfWicket:Optional[List[FallOfWicketLineItems]]=None
    partnershipBoundaries:Optional[List[PartnershipBoundariesLineItems]]=None
    wicketLostBalls:Optional[List[WicketLostBallsLineItems]]=None
    
class EndCommentaryModel(BaseModel):
    commentary_id: int

class EndInningsModel(BaseModel):
    commentary_id: int
    strike_team_id: int
    match_type_id:int 
    
class UndoScoreModel(BaseModel):
    commentary_id: int
    ball: float
    run: int
    total_score: int
    match_type_id: int
    wicket : int
    total_wicket : int
    strike_team_id: int
    ball_by_ball_id:int

class MarketManualClose(BaseModel):
    commentary_id: int
    status: int
    match_type_id:int 
    event_market_id: int
    strike_team: int

class MarketManualSettle(BaseModel):
    commentary_id: int
    status: int
    event_market_id: int
    strike_team: int 
    result: int
    
class SuccessResponse(BaseModel):
    status_code: int

class FailureResponse(BaseModel):
    status_code: int
    error_msg: Optional[str]
