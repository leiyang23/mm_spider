from sqlalchemy import Column, String, Integer, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

base = declarative_base()


class Tag(base):
    """ 标签 """
    __tablename__ = "tag"

    tag_id = Column("tag_id", Integer, primary_key=True, autoincrement=True)
    tag_name = Column("tag_name", String(50), )


collection_tag = Table('collection_tag', base.metadata,
                       Column('collection_id', Integer, ForeignKey('collection.collection_id')),
                       Column('tag_id', Integer, ForeignKey('tag.tag_id'))
                       )


class Collection(base):
    """ 合集 """
    __tablename__ = "collection"

    collection_id = Column("collection_id", Integer, primary_key=True, autoincrement=True)
    collection_num = Column("collection_num", String(15), )
    name = Column("name", String(100))
    total_num = Column("image_num", Integer)
    tags = relationship("Tag", secondary=collection_tag)


class Image(base):
    """ 图片 """
    __tablename__ = "image"

    image_id = Column("image_id", Integer, primary_key=True, autoincrement=True)
    year = Column("year", String(6))
    month = Column("month", String(6))
    day = Column("day", String(6))
    width = Column("width", Integer)
    height = Column("height", Integer)
    meizitu_url = Column("meizitu_url", String(100))

    collection_num = Column("collection_num", String(15), ForeignKey("collection.collection_num"))


class DownloadRecord(base):
    """ 下载记录 """
    __tablename__ = "download_record"

    collection_num = Column("collection_num", String(15), primary_key=True)
    status = Column("status", Integer, default=0)  # 合集信息获取状态
    dl_status = Column("dl_status", Integer, default=0)  # 合集图片下载状态
