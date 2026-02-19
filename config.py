import os
from pathlib import Path


class Config:
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "8317547456:AAE4utyhtqIWGkT83T5itJT_t9qgXCvuxEU")
    API_ID = int(os.environ.get("API_ID", "20043348"))
    API_HASH = os.environ.get("API_HASH", "d05c630f6328b0ed4ff64b09f99d1151")
    OWNER_ID = int(os.environ.get("OWNER_ID", "5158184800"))

    # MongoDB
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://DARKCODE:SAHILIITIAN202709@cluster0.m6zadpy.mongodb.net/?appName=Cluster0")  # mongodb+srv://user:pass@cluster.mongodb.net/
    DB_NAME = os.environ.get("DB_NAME", "ads_pro_bot")

    # Sessions directory (file-based fallback)
    SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", "sessions"))

    @classmethod
    def validate(cls):
        missing = []
        if not cls.BOT_TOKEN:
            missing.append("BOT_TOKEN")
        if not cls.API_ID:
            missing.append("API_ID")
        if not cls.API_HASH:
            missing.append("API_HASH")
        if not cls.OWNER_ID:
            missing.append("OWNER_ID")
        if not cls.MONGO_URI:
            missing.append("MONGO_URI")
        if missing:
            raise ValueError(f"Missing config: {', '.join(missing)}")
        cls.SESSIONS_DIR.mkdir(exist_ok=True)
        print(f"  ✓ Owner: {cls.OWNER_ID}")
        print(f"  ✓ MongoDB: {cls.MONGO_URI[:30]}...")