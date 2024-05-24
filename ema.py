from tvDatafeed import TvDatafeed, Interval
import time

from talipp.indicators import EMA, RSI, BB, StdDev
from talipp.ohlcv import OHLCVFactory
from datetime import datetime, timezone

# fetch symbols,cat,subcat,exchange
import psycopg2
import requests

# PostgreSQL connection parameters
db_params = {
    "host": "13.200.42.226",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "Taher@123"
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)

    # Create a cursor object
    cursor = conn.cursor()

    # Execute a SQL query to fetch only the 'symbol' column
    cursor.execute("SELECT symbol FROM currency_currency")
    list1 = cursor.fetchall()
    list1 = [a[0] for a in list1]

    cursor.execute("SELECT exchange FROM currency_currency")
    exchange1 = cursor.fetchall()
    exchange1 = [a[0] for a in exchange1]

    cursor.execute("SELECT category FROM currency_currency")
    subcat = cursor.fetchall()
    subcat = [a[0] for a in subcat]

    cursor.execute("SELECT subcategory FROM currency_currency")
    cat = cursor.fetchall()
    cat = [a[0] for a in cat]

    # Close the cursor and connection
    cursor.close()
    conn.close()

except psycopg2.Error as e:
    print("Error connecting to PostgreSQL database:", e)

# Database

def utctime():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# send_message("Bot Launched")
print("Bot Launched")
print(list1)
print(utctime())

def checkdecimal(number1):
    count = 0
    while number1 - int(number1) > 0.000001:
        number1 = number1 * 10
        count += 1
    return count

username = ""
password = ""
tv = TvDatafeed(username, password)

emalen1 = 21
emalen2 = 50
emalen3 = 100
emalen4 = 200

datalist = []
firstcycle = True
while True:
    time.sleep(1)
    current_time = datetime.now(timezone.utc)
    chour = current_time.time().hour
    cminute = current_time.time().minute
    cseconds = current_time.time().second
    cdate = current_time.day
    cmonth = current_time.month
    cyear = current_time.year
    cday = current_time.strftime("%A")

    cdayint = current_time.weekday()

    dayindex = 0
    if cdayint == 0:
        dayindex = -8
    else:
        dayindex = (cdayint * -1) - 1

    run1 = cminute % 15 == 0 and cseconds == 3
    dailycond = chour == 0 and cseconds == 3 and cminute == 0

    tfcond = [
        cminute % 15 == 0 and cseconds == 3,
        cminute % 15 == 0 and cseconds == 3,
        cminute == 0 and cseconds == 3,
        chour % 4 == 0 and cseconds == 3 and cminute == 0,
        cdayint == 0 and chour == 0 and cminute == 0 and cseconds == 3
    ]

    tftext = ["1 day", "00:15:00", "01:00:00", "04:00:00", "7 days"]

    if run1 or firstcycle:
        tv = TvDatafeed(username, password)
        sindex = 0
        for symbol in list1:
            dataday = None
            data1 = []

            tfs = [Interval.in_daily, Interval.in_15_minute, Interval.in_1_hour, Interval.in_4_hour, Interval.in_weekly]
            tfcount = 0
            for tf in tfs:
                if tfcond[tfcount] or firstcycle:
                    print(f"{symbol} : {tftext[tfcount]} - {utctime()} - {firstcycle}")
                    data = None
                    while data is None:
                        data = tv.get_hist(symbol=symbol, exchange=exchange1[sindex], interval=tf, n_bars=700)
                        if data is None:
                            print(symbol + " - Fetching Error")
                            time.sleep(1)
                    data1.append(data)
                    if tf == Interval.in_daily:
                        dataday = data

                    open1 = data['open'].tolist()
                    high1 = data['high'].tolist()
                    low1 = data['low'].tolist()
                    close1 = data['close'].tolist()
                    decimal = max(checkdecimal(open1[-1]), checkdecimal(close1[-1]))

                    gain = []
                    loss = []
                    avg_gain = 0
                    avg_loss = 0
                    rsi1 = []
                    rsi1avg = []
                    rsilen = 14
                    rsiavglen = 14
                    bbmult = 2.0
                    ema1 = [0 if v is None else round(v, decimal) for v in EMA(period=emalen1, input_values=close1)]
                    ema2 = [0 if v is None else round(v, decimal) for v in EMA(period=emalen2, input_values=close1)]
                    ema3 = [0 if v is None else round(v, decimal) for v in EMA(period=emalen3, input_values=close1)]
                    ema4 = [0 if v is None else round(v, decimal) for v in EMA(period=emalen4, input_values=close1)]

                    bulltrend = True
                    beartrend = True

                    for i in range(-2, -23, -1):
                        if close1[i] > ema3[i] or close1[i] > ema4[i]:
                            beartrend = False
                        if close1[i] < ema3[i] or close1[i] < ema4[i]:
                            bulltrend = False

                    trend = 0
                    if bulltrend:
                        trend = 1
                    if beartrend:
                        trend = -1

                    cclose = close1[-2]
                    ema21 = ema1[-2]
                    ema50 = ema2[-2]
                    ema100 = ema3[-2]
                    ema200 = ema4[-2]

                    highm = dataday['high'].tolist()
                    lowm = dataday['low'].tolist()
                    monhigh = highm[dayindex]
                    monlow = lowm[dayindex]
                    monmid = (monhigh + monlow) / 2
                    cond20_50 = ema21 > ema50
                    cond50_100 = ema50 > ema100
                    cond100_200 = ema100 > ema200
                    condprice_100 = cclose > ema100
                    tftext1 = tftext[tfcount]
                    tfcount += 1

                    ema_records_url = 'https://betrendcatch.tmvcrypto.com/api/v1/ema-records/'
                    headers = {
                        'x-api-key': 'ChjVbEQ5.g4qeDDGMpdqcUqU1TaLvz91zC3bUByCf',
                        'Content-Type': 'application/json'
                    }

                    ema_records_data = {
                        "timeframe": tftext1,
                        "currency_symbol": symbol,
                        "close": cclose,
                        "ema20": ema21,
                        "ema50": ema50,
                        "ema100": ema100,
                        "ema200": ema200,
                        "trend": trend,
                        "monhigh": monhigh,
                        "monlow": monlow,
                        "monmid": monmid,
                        "twenty_greater_than_fifty": cond20_50,
                        "fifty_greater_than_hundred": cond50_100,
                        "hundred_greater_than_twohundred": cond100_200,
                        "close_greater_than_hundred": condprice_100
                    }

                    create_record_response = requests.post(ema_records_url, headers=headers, json=ema_records_data)

                    if create_record_response.status_code == 201:
                        print("Record created successfully.")
                    else:
                        print("Failed to create record. Status code:", create_record_response.status_code)
                        print("Response:", create_record_response.text)
                else:
                    print("Login failed. Status code:", login_response.status_code)
                    print("Response:", login_response.text)

            sindex += 1

    firstcycle = False
