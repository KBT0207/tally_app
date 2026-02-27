from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()