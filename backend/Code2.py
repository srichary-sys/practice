from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from bson import ObjectId
import motor.motor_asyncio
import re

# ---------------------------------------------------
# APP INIT
# ---------------------------------------------------
app = FastAPI(title="3CR MongoDB Video CRUD API (Cursor Pagination + Mandatory Title)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# DATABASE CONFIG
# ---------------------------------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "gia__ytgrabber"
COLLECTION_NAME = "gia_ytg__video_output"

client = motor.motor_asyncio.AsyncIOMotorClient(
    MONGO_URI,
    maxPoolSize=200,
    socketTimeoutMS=0
)
db = client[DB_NAME]
videos_collection = db[COLLECTION_NAME]

# ---------------------------------------------------
# MODELS
# ---------------------------------------------------
class VideoModel(BaseModel):
    Id: Optional[str]
    title: Optional[str]
    description: Optional[str]
    publishedAt: Optional[str]
    viewCount: Optional[int]
    sourceUrl: Optional[str]
    transcriptText: Optional[str]

class FilterCountsModel(BaseModel):
    titleDocs: Optional[int] = 0
    descDocs: Optional[int] = 0
    transDocs: Optional[int] = 0
    totalDocs: Optional[int] = 0

# ---------------------------------------------------
# SERIALIZER USING RIGHT-SIDE DB FIELDS
# ---------------------------------------------------
def video_serializer(video) -> dict:
    youtube_id = video.get("youtubeId", "")
    return {
        "_id": str(video.get("_id", "")),
        "Id": video.get("Id", None),
        "title": video.get("title", None),
        "description": video.get("description", None),
        "sourceUrl": f"https://www.youtube.com/watch?v={youtube_id}" if youtube_id else None,
        "publishedAt": video.get("publishedAt", None),
        "viewCount": video.get("viewCount", None),
        "transcriptText": video.get("transcriptText", None),
    }

# ---------------------------------------------------
# ALLOWED FIELDS (LEFT-SIDE SIMPLIFIED)
# ---------------------------------------------------
ALLOWED_FIELDS = [
    "Id",
    "title",
    "description",
    "publishedAt",
    "viewCount",
    "sourceUrl",
    "transcriptText",
]

# ---------------------------------------------------
# ROUTES
# ---------------------------------------------------

@app.get("/")
async def root():
    return {"message": "3CR MongoDB Optimized Cursor API is running 🚀"}

# ---------------------------------------------------
# CURSOR-BASED PAGINATED FETCH WITH TEXT SEARCH USING $match
# ---------------------------------------------------
@app.get("/api/videos")
async def list_videos(
    limit: int = Query(10000, ge=1, le=30000000),
    last_id: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    transcriptText: Optional[str] = None,
    search: Optional[str] = None,
    search_field: Optional[str] = None,
):
    # TITLE IS MANDATORY, ADDING A MANDATORY TITLE QUERY
    query = {
        "$and": [
            {"title": {"$exists": True}},
            {"title": {"$type": "string"}},
            {"title": {"$ne": None}},
            {"title": {"$ne": ""}},
            {"title": {"$not": {"$regex": r"^\s*$"}}}
        ]
    }

    # Search Handling with $text Search (if provided)
    if search:
        conditions = []
        if search_field in ALLOWED_FIELDS:
            field_map = {
                "title": "title",
                "description": "description",
                "publishedAt": "publishedAt",
                "viewCount": "viewCount",
                "sourceUrl": "youtubeId",
                "transcriptText": "transcriptText",
                "Id": "Id"
            }
            db_field = field_map.get(search_field)
            if db_field:
                conditions = [{"$text": {"$search": search}}]
        else:
            # If `search_field` is "any", search across multiple fields with $text
            conditions = [{"$text": {"$search": search}}]

        query["$and"].append({"$or": conditions})

    # Handle Title Filter
    if title:
        query["$and"].append({"title": {"$regex": re.escape(title), "$options": "i"}})

    # Handle Description Filter
    if description:
        query["$and"].append({"description": {"$regex": re.escape(description), "$options": "i"}})

    # Handle TranscriptText Filter
    if transcriptText:
        query["$and"].append({"transcriptText": {"$regex": re.escape(transcriptText), "$options": "i"}})

    # Handle pagination (cursor)
    if last_id:
        try:
            query["_id"] = {"$gt": ObjectId(last_id)}
        except:
            raise HTTPException(status_code=400, detail="Invalid last_id format")

    # Aggregation pipeline for data retrieval
    pipeline = [
        {"$match": query},
        {"$project": {
            "_id": 1,
            "Id": 1,
            "title": 1,
            "description": 1,
            "publishedAt": 1,
            "viewCount": 1,
            "youtubeId": 1,
            "transcriptText": 1,
        }},
        {"$limit": limit}
    ]
    # Run the aggregation pipeline for data retrieval
    cursor = videos_collection.aggregate(pipeline)
    results = [video_serializer(doc) async for doc in cursor]

    # Pagination cursor
    next_cursor = results[-1]["_id"] if len(results) == limit else None

    # Use estimated_document_count() for total count (faster)
    estimated_count = await videos_collection.estimated_document_count()

    return {
        "data": results,
        "next_cursor": next_cursor,
        "count": len(results),  # Number of videos returned in this page
        "estimated_count": estimated_count,  # Approximate total count (faster)
    }

# ---------------------------------------------------
# CREATE VIDEO
# ---------------------------------------------------
@app.post("/api/videos")
async def add_video(payload: dict):
    data = {k: v for k, v in payload.items() if k in ALLOWED_FIELDS}

    title = data.get("title", None)
    if not title or not str(title).strip():
        raise HTTPException(status_code=400, detail="Title is mandatory and cannot be empty.")

    result = await videos_collection.insert_one(data)
    new_video = await videos_collection.find_one({"_id": result.inserted_id})

    return video_serializer(new_video)

# ---------------------------------------------------
# UPDATE VIDEO
# ---------------------------------------------------
@app.put("/api/videos/{video_id}")
async def update_video(video_id: str, payload: dict):
    update_data = {k: v for k, v in payload.items() if k in ALLOWED_FIELDS}

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    try:
        result = await videos_collection.update_one(
            {"_id": ObjectId(video_id)},
            {"$set": update_data}
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Video not found")

        updated = await videos_collection.find_one({"_id": ObjectId(video_id)})
        return video_serializer(updated)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ---------------------------------------------------
# DELETE VIDEO
# ---------------------------------------------------
@app.delete("/api/videos/{video_id}")
async def delete_video(video_id: str):
    try:
        result = await videos_collection.delete_one({"_id": ObjectId(video_id)})

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Video not found")

        return {"message": "Video deleted successfully"}

    except:
        raise HTTPException(status_code=400, detail="Invalid video ID")

# ---------------------------------------------------
# FILTER COUNTS BASED ON SEARCH KEYWORD
# ---------------------------------------------------
@app.get("/api/videos/filter_counts", response_model=FilterCountsModel)
async def get_filter_counts(search: str = Query(..., min_length=1, max_length=100)):
    """
    Aggregates the counts for the search keyword across title, description, and transcriptText.
    """
    try:
        # MongoDB Aggregation Pipeline
        aggregation_pipeline = [
            {
                "$match": {
                    "$text": {"$search": search}  # Use the passed search keyword
                }
            },
            {
                "$project": {
                    "titleText": {
                        "$cond": {
                            "if": {"$gt": [{"$type": "$title"}, "missing"]},
                            "then": "$title",  # Directly use the field as it's a string, not an array
                            "else": ""
                        }
                    },
                    "descText": {
                        "$cond": {
                            "if": {"$gt": [{"$type": "$description"}, "missing"]},
                            "then": "$description",  # Directly use the field as it's a string, not an array
                            "else": ""
                        }
                    },
                    "transText": {
                        "$cond": {
                            "if": {"$gt": [{"$type": "$transcriptText"}, "missing"]},
                            "then": "$transcriptText",  # Directly use the field as it's a string, not an array
                            "else": ""
                        }
                    }
                }
            },
            {
                "$project": {
                    "titleMatch": {"$regexMatch": {"input": "$titleText", "regex": search, "options": "i"}},
                    "descMatch": {"$regexMatch": {"input": "$descText", "regex": search, "options": "i"}},
                    "transMatch": {"$regexMatch": {"input": "$transText", "regex": search, "options": "i"}}
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
            return {"message": "No matches found for the provided search keyword."}

        # Extract the aggregation results
        filter_counts = result[0]

        # Return the results in a structured format
        return FilterCountsModel(
            titleDocs=filter_counts.get("titleDocs", 0),
            descDocs=filter_counts.get("descDocs", 0),
            transDocs=filter_counts.get("transDocs", 0),
            totalDocs=filter_counts.get("totalDocs", 0),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
