import os
import sys
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException
import config
import time
from datetime import datetime
import requests
import webbrowser
from framework.backfiller.core import historicals
import psycopg2


def main():

    api_key = config.KITE_API_KEY
    api_secret = config.KITE_API_SECRET
    access_token_api_url = config.ACCESS_TOKEN_API_URL
    conn = config.db_conn()

    request = requests.get(access_token_api_url)
    access_token = request.json().get("access_token", "")

    kite = KiteConnect(api_key=api_key)
    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
    except Exception as e:
        print("Error setting access token:", e)
        # open login URL in browser

        loginurl = kite.login_url()
        kite = None
        print("Login URL:", loginurl)
        webbrowser.open(loginurl)
        return

    first_run = True
    period = 1
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        if first_run:
            try:
                period = int(
                    input("Initial run - Enter number of days to backfill: 0-31: "))
            except ValueError:
                period = 1
                print("Invalid input. Defaulting to 1 day.")

            if period > 31:
                print("Invalid input. Defaulting to 31 days.")
                period = 31
            if period < 0:
                print("Invalid input. Defaulting to 1 day.")
                period = 1

            first_run = False
        else:
            period = 1

        historicals(exchange='nfo', period=period,
                    interval='minute', api=kite, conn=conn)

        if not check_market_hours():
            break

        wait_until_next(waiting_minutes=1)


def check_market_hours():
    now = datetime.now()
    start_time = datetime.strptime("09:15:00", "%H:%M:%S").time()
    end_time = datetime.strptime("15:30:00", "%H:%M:%S").time()
    current_time = now.time()

    # If current time is outside market hours, exit loop
    if current_time < start_time or current_time > end_time:
        print(
            f"\n⛔ Outside market hours ({current_time.strftime('%H:%M:%S')}). Exiting loop.")
        return False  # indicate stop condition

    return True  # indicate continue condition


def wait_until_next(waiting_minutes=1):
    now = datetime.now()
    next_minute = (now.minute // waiting_minutes + 1) * waiting_minutes
    if next_minute == 60:
        next_run = now.replace(hour=(now.hour + 1) %
                               24, minute=0, second=1, microsecond=0)
    else:
        next_run = now.replace(minute=next_minute, second=1, microsecond=0)

    wait_seconds = int((next_run - now).total_seconds())
    print(f"Next run scheduled at {next_run.strftime('%H:%M:%S')}")

    try:
        while True:
            remaining = int((next_run - datetime.now()).total_seconds())
            if remaining <= 0:
                break
            mins, secs = divmod(remaining, 60)
            print(
                f"\r⏳ Sleeping... {mins:02d}m {secs:02d}s remaining", end="", flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⛔️ Interrupted by user.")
        exit(0)

    print("\r✅ Woke up for next run!                      ", end="\n")


if __name__ == "__main__":
    main()
