# coding: utf-8
from sqlalchemy import Column, String, TIMESTAMP, Text, text, INT, engine, create_engine
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
metadata = Base.metadata


class InfoDataBase:
    def __init__(self, conn):
        self.engine = create_engine(engine.url.URL(**conn))
        self.DBSession = sessionmaker(bind=self.engine)

    def insert_data(self, weibo_info):
        session = self.DBSession()
        row_to_insert = TWeiboInfo(**weibo_info)
        session.add(row_to_insert)
        session.commit()


class TWeiboInfo(Base):
    __tablename__ = 't_weibo_info'

    FuiId = Column(INT(), primary_key=True, autoincrement=True)
    FstrCommentContent = Column(Text)
    FstrCommentMaker = Column(Text)
    FstrReplyTo = Column(Text)
    FstrWeiboContent = Column(Text)
    FuiWeiboCt = Column(BIGINT(20), nullable=False)
    FuiCommentCount = Column(INT(), nullable=False)
    FuiCommentCt = Column(BIGINT(20))
    FuiCommentId = Column(BIGINT(20))
    FuiReplyType = Column(BIGINT(20))
    FuiWeiboId = Column(BIGINT(20))
    FstrUrl = Column(String(1024))
    FstrPageUrl = Column(String(1024))
    FstrWeiboContentHash = Column(String(1024))
    FstrCommentContentHash = Column(String(1024))
    FuiIfDelete = Column(INT(), nullable=False, default=0)
    FstrUpdateTime = Column(TIMESTAMP, nullable=False,
                            server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
