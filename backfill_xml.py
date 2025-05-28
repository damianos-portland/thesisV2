
import os
from pymongo import MongoClient

# ─── Configuration ──────────────────────────────────
MONGO_URI       = "mongodb://localhost:27017/"
DB_NAME         = "judgmentsV2"
COLLECTION_NAME = "courtDecisions"
XML_DIR         = "XML"   # top-level folder where your XML files live

# ─── Connect ────────────────────────────────────────
client     = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]

# ─── Iterate & backfill ─────────────────────────────
for doc in collection.find({}, {"file_name":1}):
    fn = doc["file_name"]
    # assume the file lives somewhere under XML_DIR
    # if you have subfolders by court/year, you may need to walk
    for root, dirs, files in os.walk(XML_DIR):
        if fn in files:
            path = os.path.join(root, fn)
            with open(path, "r", encoding="utf-8") as f:
                xml = f.read()
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"xml": xml}}
            )
            print(f"Backfilled {fn}")
            break
    else:
        print(f"⚠️  File not found on disk: {fn}")

print("✅ Done backfilling XML.")