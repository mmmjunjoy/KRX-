import logging
import time
import requests
from datetime import date ,timedelta,datetime
from fastapi import APIRouter, Depends
from fastapi_utils.cbv import cbv
from pydantic import BaseModel
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from config import config
from db import get_db_krx
from libs.airi_krx_scraper.data import AiriKrxDataScraper
from models.models_krx import AllListedIssues as db_is
# from user import Admin, get_jwt_user
# from utils.SlackBot import SlackBot


router = APIRouter()
logger = logging.getLogger('uvicorn')
# slack = SlackBot()


@cbv(router=router)
class KrxStockIssue:
  db:AsyncSession = Depends(get_db_krx)
  # user:Admin = Depends(get_jwt_user)
  scraper = AiriKrxDataScraper()

  class AllListedIssue(BaseModel):
    date:date
    lisin_code:str
    ticker_symbol:int
    ticker_name_full:str
    ticker_name:str
    ticker_name_eng:str
    listing_date:str
    market_type:str
    security_type:str
    company_category:str
    share_type:str
    par_value:int
    listed_share:int



  @router.get('/all',
    summary='전종목 기본정보',
    response_model=list[AllListedIssue]
  )
  async def get_issued_all(self, market_type):
    try:
      res = self.scraper.get_issued_all(market_type)
      return res
    except Exception:
      logger.exception('전종목 기본정보')
      # slack.send('#airi-ra-error', traceback.format_exc())

    return None


  # # class AllListedIssueParam(BaseModel):
  #   market_type : str = 'ALL'
  #   curr_date : str = datetime.now().strftime("%Y%m%d")

  @router.post('/all',
    summary='전종목 기본정보 업데이트',
    response_model=int,
  )

  async def update_issued_all(self):
    try:
      res = await self.get_issued_all(self)
      # print(res)

      if res and len(res) > 0:
        await self.db.execute(
          insert(db_is),
          res,
        )
        await self.db.commit()

      return len(res)
    except Exception:
      await self.db.rollback()
      logger.exception('전종목 기본정보 업데이트')
      # slack.send('#airi-ra-error', traceback.format_exc())

    return None

  #class AllListedIssuePeriod(BaseModel):
   # start_date:date
    #end_date:date

  






  
