import traceback
import time

from forward_content_news import main

if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            print("Bot stopped by user.")
            break
        except Exception as e:
            print(f"Error starting bot: {e}")
            traceback.print_exc()
            print("Retrying startup in 15 seconds...")
            time.sleep(15)
