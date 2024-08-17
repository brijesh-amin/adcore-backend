from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import pandas as pd
import logging
from datetime import datetime
from bson import ObjectId
from fastapi.middleware.cors import CORSMiddleware
import requests
from io import StringIO
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Load environment variables from .env file

# Read MongoDB configuration from environment variables
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
EXPIRATION_SECONDS = int(os.getenv('EXPIRATION_SECONDS', 600))
DATA_FETCH_API = os.getenv(
    'DATA_FETCH_API', 'https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0')
logging.basicConfig(level=logging.INFO)
app = FastAPI()

# CORS Middleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# MongoDB setup
client = MongoClient(MONGODB_URI)
db = client['courses_db']
courses_collection = db['courses']
countries_collection = db['countries']
cities_collection = db['cities']
# Data Model for Course


class Course(BaseModel):
    _id: str
    university: str
    city: str
    country: str
    coursename: str
    coursedescription: str
    startdate: datetime
    enddate: datetime
    price: float
    currency: str

# Define TTL Index on 'timestamp' field


def create_ttl_index():
    courses_collection.create_index(
        [("timestamp", 1)],
        expireAfterSeconds=EXPIRATION_SECONDS  # 10 minutes
    )

# Helper: Normalize CSV and load into MongoDB


def normalize_and_load_data():
    url = DATA_FETCH_API

    # Download CSV file
    response = requests.get(url)
    csv_data = response.text

    # Load CSV into DataFrame
    df = pd.read_csv(StringIO(csv_data))

    # Normalize Data
    df.columns = df.columns.str.lower().str.replace(
        ' ', '_')  # Normalize column names
    df['startdate'] = pd.to_datetime(df['startdate'])
    df['enddate'] = pd.to_datetime(df['enddate'])
    df['price'] = pd.to_numeric(df['price'])
    df = df.drop_duplicates()  # Remove duplicates

    # Add a timestamp column for TTL indexing
    df['timestamp'] = datetime.utcnow()

    # Convert DataFrame to dictionary format and load into MongoDB
    courses_collection.drop()  # Clean the collection
    courses_collection.insert_many(df.to_dict(orient='records'))

    # Extract unique countries and cities
    countries = df['country'].dropna().unique().tolist()
    cities = df['city'].dropna().unique().tolist()

    # Load countries and cities into separate collections
    countries_collection.drop()  # Clean the collection
    cities_collection.drop()  # Clean the collection
    countries_collection.insert_many(
        [{'country': country} for country in countries])
    cities_collection.insert_many([{'city': city} for city in cities])

    # Create TTL Index
    create_ttl_index()

# Load courses initially


if courses_collection.count_documents({}) == 0:
    normalize_and_load_data()
# Start the scheduler to refresh data every 10 minutes
scheduler = BackgroundScheduler()
scheduler.add_job(normalize_and_load_data, IntervalTrigger(
    minutes=(EXPIRATION_SECONDS / 60)))
scheduler.start()


def convert_objectid_to_str(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, ObjectId):
                data[key] = str(value)
            elif isinstance(value, dict):
                convert_objectid_to_str(value)
            elif isinstance(value, list):
                for i in range(len(value)):
                    if isinstance(value[i], ObjectId):
                        value[i] = str(value[i])
                    elif isinstance(value[i], dict):
                        convert_objectid_to_str(value[i])
    elif isinstance(data, list):
        for i in range(len(data)):
            if isinstance(data[i], ObjectId):
                data[i] = str(data[i])
            elif isinstance(data[i], dict):
                convert_objectid_to_str(data[i])

# API Endpoints


@ app.get("/courses/")
def get_courses(page: int = 1, limit: int = 10, search: str = None):
    query = {}
    if search:
        query = {
            "$or": [
                {"university": {"$regex": search, "$options": "i"}},
                {"city": {"$regex": search, "$options": "i"}},
                {"country": {"$regex": search, "$options": "i"}},
                {"coursename": {"$regex": search, "$options": "i"}},
                {"coursedescription": {"$regex": search, "$options": "i"}},
                {"startdate": {"$regex": search, "$options": "i"}},
                {"enddate": {"$regex": search, "$options": "i"}},
                {"price": {"$regex": search, "$options": "i"}},
                {"currency": {"$regex": search, "$options": "i"}},
            ]
        }
    skip = (page - 1) * limit
    total = courses_collection.count_documents(query)
    courses = list(courses_collection.find(query).skip(skip).limit(limit))
    convert_objectid_to_str(courses)  # Convert ObjectId to string
    return {"total": total, "page": page, "courses": courses}


@ app.get("/courses/{course_id}")
def get_course(course_id: str):
    try:
        obj_id = ObjectId(course_id)
    except Exception as e:
        logging.error(f"Error converting course_id to ObjectId: {e}")
        raise HTTPException(status_code=400, detail="Invalid course ID format")

    course = courses_collection.find_one({"_id": obj_id})
    if course is None:
        raise HTTPException(status_code=404, detail="Course not found")

    convert_objectid_to_str(course)  # Convert ObjectId to string
    return course


@ app.post("/courses/")
def create_course(course: Course):
    course_dict = course.dict()
    inserted = courses_collection.insert_one(course_dict)
    return {"id": str(inserted.inserted_id)}


@ app.put("/courses/{course_id}")
def update_course(course_id: str, course: Course):
    try:
        obj_id = ObjectId(course_id)
    except Exception as e:
        logging.error(f"Error converting course_id to ObjectId: {e}")
        raise HTTPException(status_code=400, detail="Invalid course ID format")

    # Fetch the existing course from the database
    existing_course = courses_collection.find_one({"_id": obj_id})
    if not existing_course:
        raise HTTPException(status_code=404, detail="Course not found")

    # Validate that restricted fields are not being updated
    if (course.coursename != existing_course["coursename"] or
            course.university != existing_course["university"] or
            course.city != existing_course["city"] or
            course.country != existing_course["country"]):
        raise HTTPException(
            status_code=400,
            detail="Updates to Course Name, university, city, or country are not allowed"
        )

    # Perform the update (excluding the restricted fields)
    result = courses_collection.update_one(
        {"_id": obj_id},
        {"$set": {
            "coursedescription": course.coursedescription,
            "startdate": course.startdate,
            "enddate": course.enddate,
            "price": course.price,
            "currency": course.currency
        }}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Course not found")
    return {"status": "success"}


@ app.delete("/courses/{course_id}")
def delete_course(course_id: str):
    try:
        obj_id = ObjectId(course_id)
    except Exception as e:
        logging.error(f"Error converting course_id to ObjectId: {e}")
        raise HTTPException(status_code=400, detail="Invalid course ID format")

    result = courses_collection.delete_one({"_id": obj_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Course not found")
    return {"status": "deleted"}


@ app.get("/universities/")
def get_universities():
    universities = courses_collection.distinct("university")
    return {"universities": universities}


@ app.get("/cities/")
def get_cities():
    cities = list(cities_collection.find({}, {"_id": 0, "city": 1}))
    return {"cities": [city["city"] for city in cities]}


@ app.get("/countries/")
def get_countries():
    countries = list(countries_collection.find({}, {"_id": 0, "country": 1}))
    return {"countries": [country["country"] for country in countries]}


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
