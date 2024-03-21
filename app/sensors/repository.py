import json
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    db_sensor_data = sensor.dict()
    mongodb_client.insertOne(db_sensor_data)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    sensor_data = data
    redis.set(sensor_id, json.dumps(data.dict()))
    return sensor_data

def get_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data
    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor