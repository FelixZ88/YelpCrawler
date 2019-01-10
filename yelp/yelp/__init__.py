

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, Float, BLOB, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
logger.addHandler(handler)

DB_HOST = 'localhost'
DB_USERNAME = 'root'
DB_PASSWORD = 'Instrumments'
DB_DBNAME = 'Food_Crawler'

str_connect = 'mysql+mysqlconnector://%s:%s@%s:3306/%s?charset=utf8mb4' % (DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DBNAME)
engine = create_engine(str_connect, encoding="utf8")

base = declarative_base()


class TaskBean(base):
    __tablename__ = 'yelp_tasks'
    id = Column(Integer, primary_key=True)
    city = Column(String(256), nullable=False)
    url = Column(String(1024), nullable=False)
    is_finished = Column(Boolean, default=False)
    parent_id = Column(Integer, default=-1)
    type = Column(Integer)  # 1. YelpBean, 2. CityBean, 3. CityPageBean, 4. RestaurantBean, 5. RestaurantReviewBean

    def to_dict(self):
        return {'task_id': self.id, 'city': self.city, 'url': self.url, 'is_finished': self.is_finished, 'type': self.type}


class RestaurantBean(base):
    __tablename__ = 'yelp_restaurants'
    id = Column(Integer, primary_key=True)
    city = Column(String(256), nullable=False)
    restaurant_name = Column(String(256))
    alter_names = Column(String(256))
    star = Column(Integer, default=0)
    star_detail = Column(String(256), default="0,0,0,0,0")
    url_restaurant = Column(String(1024), nullable=False)
    url_host = Column(String(1024), default="")
    address = Column(String(256), nullable=False)
    category = Column(String(256), nullable=True)
    latitude = Column(String(256))
    langitude = Column(String(256))
    phone = Column(String(256))
    count_reviews = Column(Integer, default=0)
    tags = Column(String(256), nullable=False, default="")
    reviews = relationship("RestaurantReviewBean", backref="restaurant", lazy=True)
    task_id = Column(Integer, ForeignKey("yelp_tasks.id"))

    def to_dict(self):
        return {"id": self.id,
                "city": self.city,
                "restaurant_name": self.restaurant_name,
                "star": self.score_taste,
                "star_detail": self.score_env,
                "url_restaurant": self.url_restaurant,
                "url_host": self.url_host,
                "address": self.address,
                "latitude": self.latitude,
                "langitude": self.langitude,
                "phone": self.phone,
                "count_reviews": self.count_reviews,
                "tags": self.tags}


class RestaurantReviewBean(base):
    __tablename__ = 'yelp_reviews'
    id = Column(Integer, primary_key=True)
    restaurant_id = Column(Integer, ForeignKey("yelp_restaurants.id"))
    star = Column(Float, default=-0.1)
    language = Column(String(256))
    content = Column(BLOB, nullable=False)
    date = Column(DateTime, nullable=False)
    images = Column(String(4096), nullable=False, default="")
    task_id = Column(Integer, ForeignKey("yelp_tasks.id"))


def init_db():
    base.metadata.create_all(engine)


def drop_db():
    base.metadata.drop_all(engine)


init_db()

Session = sessionmaker(bind=engine)
session = Session()