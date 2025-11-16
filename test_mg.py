# test_mongo.py
from pymongo import MongoClient
import certifi, os
from dotenv import load_dotenv
load_dotenv()
uri = os.getenv("MONGO_URI")
print("Using URI:", uri[:80]+"..." if uri else "MONGO_URI empty")
client = MongoClient(uri, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=10000)
try:
    print("Server info:", client.server_info())   # will raise on failure
except Exception as e:
    print("Mongo connect failed:", repr(e))
finally:
    client.close()
