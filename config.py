# config.py
from datetime import datetime, time

DB_CONFIG = {
    'host': 'localhost',
    'database': 'forex_ai',
    'user': 'postgres',
    'password': 'posTine4365#',
    'port': 5432
}

def gold_trading_hours(day: int, hour: int) -> bool:
    """
    day: 0-6 (Monday = 0, Sunday = 6)
    hour: 0-23
    Returns True if gold is trading at this day/hour
    """
    if day in [0, 1, 2, 3]:  # Monday through Thursday
        return (hour < 22) or (hour >= 23)  # Trading 00:00-21:59 AND 23:00-23:59
    elif day == 4:  # Friday
        return hour < 22  # Trading 00:00-21:59
    elif day == 5:  # Saturday
        return False  # No trading
    else:  # Sunday
        return hour >= 23  # Trading starts at 23:00

INSTRUMENTS = {
    'XAUUSD': {
        'symbol': 'XAUUSD',
        'point_value': 1000,
        'table_name': 'ticks_xauusd',
        'start_date': datetime(2003, 5, 5), #year, month, day
        'trading_hours': gold_trading_hours
    }
}