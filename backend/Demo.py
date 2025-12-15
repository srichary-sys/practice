from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import motor.motor_asyncio
import re

# ---------------------------------------------------
# APP INIT
# ---------------------------------------------------
app = FastAPI(title="MongoDB Aggregation Video Filter API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# DATABASE CONFIG
# ---------------------------------------------------
MONGO_URI = "mongodb://192.168.2.163:27017/"
DB_NAME = "gia__ytgrabber"
COLLECTION_NAME = "gia_ytg__video_output"

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
videos_collection = db[COLLECTION_NAME]

# ---------------------------------------------------
# MODELS
# ---------------------------------------------------
class VideoModel(BaseModel):

    titleDocs: Optional[int] = 0
    descDocs: Optional[int] = 0
    transDocs: Optional[int] = 0
    totalDocs: Optional[int] = 0

# ---------------------------------------------------
# ROUTES
# ---------------------------------------------------

@app.get("/api/filter_counts")
async def get_filter_counts(keyword: str = Query(..., min_length=1, max_length=100)):
    """
    Aggregates the counts for the keyword search across title, description, and transcriptText.
    """
    try:
        # MongoDB Aggregation Pipeline
        aggregation_pipeline = [
            {
                "$match": {
                    "$text": {"$search": f'"{keyword}"'}  # Use the passed search keyword
                }
            },
            {
                "$project": {
                    "titleText": {
                        "$cond": {
                            "if": {"$isArray": "$title"},
                            "then": {
                                "$reduce": {
                                    "input": "$title",
                                    "initialValue": "",
                                    "in": {"$concat": ["$$value", " ", {"$toString": "$$this"}]}
                                }
                            },
                            "else": {"$ifNull": [{"$toString": "$title"}, ""]}
                        }
                    },
                    "descText": {
                        "$cond": {
                            "if": {"$isArray": "$description"},
                            "then": {
                                "$reduce": {
                                    "input": "$description",
                                    "initialValue": "",
                                    "in": {"$concat": ["$$value", " ", {"$toString": "$$this"}]}
                                }
                            },
                            "else": {"$ifNull": [{"$toString": "$description"}, ""]}
                        }
                    },
                    "transText": {
                        "$cond": {
                            "if": {"$isArray": "$transcriptText"},
                            "then": {
                                "$reduce": {
                                    "input": "$transcriptText",
                                    "initialValue": "",
                                    "in": {"$concat": ["$$value", " ", {"$toString": "$$this"}]}
                                }
                            },
                            "else": {"$ifNull": [{"$toString": "$transcriptText"}, ""]}
                        }
                    }
                }
            },
            {
                "$project": {
                    "titleMatch": {"$regexMatch": {"input": "$titleText", "regex": keyword, "options": "i"}},
                    "descMatch": {"$regexMatch": {"input": "$descText", "regex": keyword, "options": "i"}},
                    "transMatch": {"$regexMatch": {"input": "$transText", "regex": keyword, "options": "i"}}
                }
            },
            {
                "$addFields": {
                    "anyMatch": {"$or": ["$titleMatch", "$descMatch", "$transMatch"]}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "titleDocs": {"$sum": {"$cond": ["$titleMatch", 1, 0]}},
                    "descDocs": {"$sum": {"$cond": ["$descMatch", 1, 0]}},
                    "transDocs": {"$sum": {"$cond": ["$transMatch", 1, 0]}},
                    "totalDocs": {"$sum": {"$cond": ["$anyMatch", 1, 0]}}
                }
            }
        ]
        
        # Run the aggregation pipeline on the MongoDB collection
        result = await videos_collection.aggregate(aggregation_pipeline).to_list(length=1)
        
        if not result:
            return {"message": "No matches found for the provided keyword."}

        # Extract the aggregation results
        filter_counts = result[0]

        # Return the results in a structured format
        return VideoModel(
            titleDocs=filter_counts.get("titleDocs", 0),
            descDocs=filter_counts.get("descDocs", 0),
            transDocs=filter_counts.get("transDocs", 0),
            totalDocs=filter_counts.get("totalDocs", 0),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------
# ROOT ROUTE
# ---------------------------------------------------
@app.get("/")
async def root():
    return {"message": "MongoDB Aggregation Video Filter API is running 🚀"}
