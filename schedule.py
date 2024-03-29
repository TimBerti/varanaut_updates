from updates.updates import daily
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os
import logging

logging.basicConfig(filename='error.log', level=logging.ERROR)

try:
    load_dotenv()
except:
    pass


engine = create_engine(os.environ['DATABASE_URL'])
db = scoped_session(sessionmaker(bind=engine))

daily(db, os.environ["EOD_URL"],
      os.environ["EOD_TOKEN"], os.environ["NASDAQ_KEY"])
