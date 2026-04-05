#!/usr/bin/env python3
"""Simple TikTok live comment listener using TikTokLive library.

Usage:
  python tiktok_live_listener.py <username> [--output out.txt]

Note: Install dependencies: `pip install -r requirements.txt`.
"""
from __future__ import annotations

import argparse
import logging
import sys

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import CommentEvent
except Exception as e:
    print("Missing dependency: TikTokLive. Install via `pip install TikTokLive`.")
    raise


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Listen to TikTok live comments")
    parser.add_argument("username", help="TikTok live username (unique id)")
    parser.add_argument("--output", "-o", help="File to append comments (optional)")
    args = parser.parse_args()

    client = TikTokLiveClient(unique_id=args.username)

    @client.on(CommentEvent)
    def on_comment(event) -> None:
        # Extract author and comment text from the CommentEvent
        try:
            user = getattr(event, "user_info", None) or getattr(event, "base_message", None) or {}
            author = getattr(user, "username", None) or getattr(user, "nick_name", None) or getattr(user, "sec_uid", None) or "<unknown>"
            comment = getattr(event, "content", None) or getattr(event, "comment", None) or str(event)
        except Exception:
            author = "<unknown>"
            comment = str(event)

        line = f"{author}: {comment}"
        print(line, flush=True)
        if args.output:
            try:
                with open(args.output, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                logging.exception("Failed to write to output file")

    try:
        logging.info("Connecting to TikTok live for %s", args.username)
        client.run()
    except KeyboardInterrupt:
        logging.info("Stopped by user")
        try:
            client.stop()
        except Exception:
            pass
        sys.exit(0)


if __name__ == "__main__":
    main()
