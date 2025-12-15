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
MONGO_URI = "mongodb://192.168.30.2:27017/"
DB_NAME = "gia__ytgrabber"
COLLECTION_NAME = "gia_ytg__video"

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
        "Id": video.get("sqlId", None),
        "title": video.get("details", {}).get("snippet", {}).get("title", None),
        "description": video.get("details", {}).get("snippet", {}).get("description", None),
        "sourceUrl": f"https://www.youtube.com/watch?v={youtube_id}" if youtube_id else None,
        "publishedAt": video.get("details", {}).get("snippet", {}).get("publishedAt", None),
        "viewCount": video.get("details", {}).get("statistics", {}).get("viewCount", None),
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
            {"details.snippet.title": {"$exists": True}},
            {"details.snippet.title": {"$type": "string"}},
            {"details.snippet.title": {"$ne": None}},
            {"details.snippet.title": {"$ne": ""}},
            {"details.snippet.title": {"$not": {"$regex": r"^\s*$"}}}
        ]
    }

    # Initialize count for each filter
    filter_counts = {
        "title": 0,
        "description": 0,
        "transcriptText": 0,
        "search": 0,
    }
    
        # Search Handling with $text Search (if provided)
    if search:
        conditions = []
        if search_field in ALLOWED_FIELDS:
            field_map = {
                "title": "details.snippet.title",
                "description": "details.snippet.description",
                "publishedAt": "details.snippet.publishedAt",
                "viewCount": "details.statistics.viewCount",
                "sourceUrl": "youtubeId",
                "transcriptText": "transcriptText",
                "Id": "sqlId"
            }
            db_field = field_map.get(search_field)
            if db_field:
                conditions = [{"$text": {"$search": search}}]
        else:
            # If `search_field` is "any", search across multiple fields with $text
            conditions = [{"$text": {"$search": search}}]

        query["$and"].append({"$or": conditions})
        # Count documents for the search filter
        filter_counts["search"] = await videos_collection.count_documents({"$or": conditions})

    # Handle Title Filter
    if title:
        query["$and"].append({"details.snippet.title": {"$regex": re.escape(title), "$options": "i"}})
        

    # Handle Description Filter
    if description:
        query["$and"].append({"details.snippet.description": {"$regex": re.escape(description), "$options": "i"}})
        

    # Handle TranscriptText Filter
    if transcriptText:
        query["$and"].append({"transcriptText": {"$regex": re.escape(transcriptText), "$options": "i"}})
        
    # Aggregation pipeline for title count
    title_pipeline = [
        {
            "$match": {
                "details.snippet.title": {"$regex": re.escape(title), "$options": "i"} if title else {"$exists": True}
            }
        },
        {
            "$project": {
                "titleMatch": {
                    "$regexMatch": {
                        "input": "$details.snippet.title",
                        "regex": re.escape(title) if title else "",
                        "options": "i"  # Case-insensitive search
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "titleDocs": {"$sum": {"$cond": ["$titleMatch", 1, 0]}}  # Count title matches
            }
        }
    ]

    # Aggregation pipeline for description count
    description_pipeline = [
        {
            "$match": {
                "details.snippet.description": {"$regex": re.escape(description), "$options": "i"} if description else {"$exists": True}
            }
        },
        {
            "$project": {
                "descriptionMatch": {
                    "$regexMatch": {
                        "input": "$details.snippet.description",
                        "regex": re.escape(description) if description else "",
                        "options": "i"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "descriptionDocs": {"$sum": {"$cond": ["$descriptionMatch", 1, 0]}}  # Count description matches
            }
        }
    ]

    # Aggregation pipeline for transcriptText count
    transcript_pipeline = [
        {
            "$match": {
                "transcriptText": {"$regex": re.escape(transcriptText), "$options": "i"} if transcriptText else {"$exists": True}
            }
        },
        {
            "$project": {
                "transcriptTextMatch": {
                    "$regexMatch": {
                        "input": "$transcriptText",
                        "regex": re.escape(transcriptText) if transcriptText else "",
                        "options": "i"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "transcriptDocs": {"$sum": {"$cond": ["$transcriptTextMatch", 1, 0]}}  # Count transcript matches
            }
        }
    ]

    # Run the aggregation pipelines for title, description, and transcriptText
    if title:
        cursor = videos_collection.aggregate(title_pipeline)
        title_result = await cursor.to_list(length=1)
        filter_counts["title"] = title_result[0]["titleDocs"] if title_result else 0

    if description:
        cursor = videos_collection.aggregate(description_pipeline)
        description_result = await cursor.to_list(length=1)
        filter_counts["description"] = description_result[0]["descriptionDocs"] if description_result else 0

    if transcriptText:
        cursor = videos_collection.aggregate(transcript_pipeline)
        transcript_result = await cursor.to_list(length=1)
        filter_counts["transcriptText"] = transcript_result[0]["transcriptDocs"] if transcript_result else 0

    # Apply the filters to the main query
    if title:
        query["$and"].append({"details.snippet.title": {"$regex": re.escape(title), "$options": "i"}})
    if description:
        query["$and"].append({"details.snippet.description": {"$regex": re.escape(description), "$options": "i"}})
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
            "sqlId": 1,
            "details.snippet.title": 1,
            "details.snippet.description": 1,
            "details.snippet.publishedAt": 1,
            "details.statistics.viewCount": 1,
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
        "filter_counts": filter_counts,  # Independent counts for each filter
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
                            "if": {"$gt": [{"$type": "$details.snippet.title"}, "missing"]},
                            "then": "$details.snippet.title",  # Directly use the field as it's a string, not an array
                            "else": ""
                        }
                    },
                    "descText": {
                        "$cond": {
                            "if": {"$gt": [{"$type": "$details.snippet.description"}, "missing"]},
                            "then": "$details.snippet.description",  # Directly use the field as it's a string, not an array
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
        result = await videos_collection.aggregate(aggregation_pipeline).to_list(length=None)  # Remove the limit

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

