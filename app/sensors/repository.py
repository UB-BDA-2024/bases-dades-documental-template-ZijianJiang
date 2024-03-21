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

def get_data(redis: Session, sensor_id: int) -> schemas.Sensor:
    db_data = redis.get(sensor_id)
    if db_data is None:
        raise HTTPException(status_code=404, detail="Sensor data not found")
    return schemas.SensorData(**json.loads(db_data))

def delete_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient, redis_client = RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    
    mongodb_client.deleteOne(db_sensor.name)
    redis_client.delete(sensor_id)
    return db_sensor

def get_sensors_near(latitude: float, longitude: float, radius: float, db: Session, mongodb_client: MongoDBClient, redis_client = RedisClient):
    lat_min, lat_max = latitude - radius, latitude + radius
    long_min, long_max = longitude - radius, longitude + radius
    query = {
        "latitude": {"$gte": lat_min, "$lte": lat_max},
        "longitude": {"$gte": long_min, "$lte": long_max}
    }
    sensors = mongodb_client.collection.find(query)
    
    sensors_nearby = []
    for sensor in sensors:
        db_sensor = get_sensor_by_name(db, sensor['name'])
        if db_sensor:
            sensor_data = get_data(redis_client, db_sensor.id)
            sensors_nearby.append(sensor_data)

    return sensors_nearby