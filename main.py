import traceback

from forward_content_news import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        print(f"Fatal startup/runtime error: {e}")
        traceback.print_exc()
        raise
