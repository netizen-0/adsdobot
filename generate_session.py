"""
Simple Session String Generator for Telegram
Run this once to generate your session string, then use it in the main bot.

This avoids OTP timing issues!
"""

from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio
import sys
import os

# Add root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import Config

async def generate_session():
    print("=" * 60)
    print("Telegram Session String Generator")
    print("=" * 60)
    print()
    
    # Use StringSession (starts empty)
    async with TelegramClient(StringSession(), Config.API_ID, Config.API_HASH) as client:
        print("✅ Connected to Telegram!")
        print()
        
        # Get the session string
        session_string = client.session.save()
        
        print("=" * 60)
        print("YOUR SESSION STRING:")
        print("=" * 60)
        print(session_string)
        print("=" * 60)
        print()
        print("📋 Copy this session string and use it in the bot!")
        print("⚠️  Keep this secret - it gives full access to your account!")
        print()
        
        # Save to file as backup
        with open("session_string.txt", "w") as f:
            f.write(session_string)
        
        print("✅ Also saved to: session_string.txt")
        print()

if __name__ == "__main__":
    print()
    print("⚡ This will ask for your phone number and OTP code.")
    print("   But only ONCE! Then you can reuse the session string.")
    print()
    input("Press ENTER to continue...")
    print()
    
    asyncio.run(generate_session())
