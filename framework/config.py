import psycopg2

KITE_API_KEY = 'tw96psyyds0yj8vj'
KITE_API_SECRET = '3iewov9onkbytzramkt263r9lvcdzks9'
ACCESS_TOKEN_API_URL = "http://kite.trialnerror.in/accesstoken/"

DB_CONFIG = {
    'host': 'kite.trialnerror.in',
    'port': 5432,
    'user': 'sysadmin',
    'password': 'Apple@1239',
    'database': 'tradersframework'
}


def db_conn():
    return psycopg2.connect(**DB_CONFIG)
