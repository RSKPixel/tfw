import psycopg2
from kiteconnect import KiteConnect
import requests
import webbrowser

KITE_API_KEY = 'tw96psyyds0yj8vj'
KITE_API_SECRET = '3iewov9onkbytzramkt263r9lvcdzks9'
ACCESS_TOKEN_API_URL = "http://kite.trialnerror.in/accesstoken/"

DB_CONFIG = {
    'host': 'kite.trialnerror.in',
    'port': 5432,
    'user': 'sysadmin',
    'password': 'Apple@1239',
    'database': 'tradersframework',
}


def db_conn():
    return psycopg2.connect(**DB_CONFIG)


def kite_connect() -> KiteConnect | None:
    api_key = KITE_API_KEY
    api_secret = KITE_API_SECRET
    access_token_api_url = ACCESS_TOKEN_API_URL

    request = requests.get(access_token_api_url)
    access_token = request.json().get("access_token", "")

    kite = KiteConnect(api_key=api_key)
    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
    except Exception as e:
        print("Error setting access token:", e)

        loginurl = kite.login_url()
        kite = None
        print("Login URL:", loginurl)
        webbrowser.open(loginurl)
        return None

    return kite
