from apscheduler.schedulers.blocking import BlockingScheduler
from updates.updates import daily
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os


try:
    load_dotenv()
except:
    pass


engine = create_engine(os.environ['DATABASE_URL'])
db = scoped_session(sessionmaker(bind=engine))


sched = BlockingScheduler()


@sched.scheduled_job('cron', day_of_week='mon-fri', hour=10, minute=17)
def scheduled_job():
    daily(db, os.environ["API_URL"], os.environ["API_TOKEN"])


if __name__ == '__main__':
    sched.start()
