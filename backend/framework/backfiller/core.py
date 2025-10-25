import sys
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta
import pandas as pd
import time
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.progress import Progress
import requests
from rich.align import Align
from decimal import Decimal, ROUND_HALF_UP
import psycopg2
from psycopg2.extras import execute_values

console = Console()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def historicals(exchange='NFO', segment='NFO-FUT', period=1, interval='minute', api=None, conn=None):

    if not api:
        console.print("[bold red]Kite connection failed[/bold red]")
        return None

    # Fetch instrument list and expiry dates
    instrument_list, current_expiry, previous_expiry = instruments(exchange)
    # instrument_list = instrument_list[:5]

    # Calculate date range based on period
    if period == 0:
        delta = (datetime.now().date() - (previous_expiry.date())).days - 1
    else:
        delta = period - 1

    from_date = datetime.now().date() - timedelta(days=delta)
    if from_date < previous_expiry.date():
        from_date = previous_expiry.date() + timedelta(days=1)

    to_date = datetime.now().date()

    # Print initial info
    print_info(exchange, segment, instrument_list, interval,
               current_expiry, previous_expiry, from_date, to_date, no_of_days=delta+1, profile=api.profile())

    # Fetch historical data using Kite API
    complete_data = api_request(
        api, instrument_list, from_date, to_date, interval)

    if complete_data is None or complete_data.empty:
        console.print(
            "[bold red]No data fetched from API for the given range[/bold red]")
        return None

    # Resample data if interval is 'minute'
    resampled_data = resample_data(complete_data, interval)

    # Postgresql Storage (To be implemented)
    store_data_non_orm(resampled_data, conn=conn)


def store_data_non_orm(resampled_data, conn):
    start_time = time.time()
    console.print("\n[bold cyan]Storing data to database...[/bold cyan]")

    cursor = conn.cursor()

    for key, df in resampled_data.items():
        if df.empty:
            continue

        df.reset_index(inplace=True)

        records = [
            (
                row['date'],
                row['symbol'],
                Decimal(str(row['open'])).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP),
                Decimal(str(row['high'])).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP),
                Decimal(str(row['low'])).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP),
                Decimal(str(row['close'])).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP),
                int(row['volume'])
            )
            for _, row in df.iterrows()
        ]

        query = f"""
            INSERT INTO {key} (date, symbol, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (date, symbol) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume
        """

        try:
            execute_values(cursor, query, records, page_size=10000)
            conn.commit()
        except Exception as e:
            console.print(f"[red]Error inserting data into {key}: {e}[/red]")

    # cursor.close()
    # conn.close()
    end_time = time.time()
    console.print(
        f"[green]Data storage completed in {end_time - start_time:.2f}s[/green]")


def resample_data(complete_data: pd.DataFrame, interval: str):
    if interval == 'minute' and not complete_data.empty:
        resampling_start = time.time()
        console.print(
            "\n[bold cyan]Resampling data (5m, 15m, 60m, 1d)...[/bold cyan]")

        sampling = {'open': 'first', 'high': 'max',
                    'low': 'min', 'close': 'last', 'volume': 'sum'}
        grouped_data = complete_data.groupby('symbol')

        # Filter only BANKNIFTY-I for 3min data
        data3data = complete_data[complete_data['symbol']
                                  == 'BANKNIFTY-I'].reset_index()
        data3data = data3data.groupby('symbol')

        data3 = data3data.resample('3min', on='date').agg(sampling).dropna()

        data5 = grouped_data.resample('5min', on='date').agg(sampling).dropna()
        data15 = data5.reset_index().groupby('symbol').resample(
            '15min', on='date').agg(sampling).dropna()
        data60 = data15.reset_index().groupby("symbol").resample(
            '60min', on='date').agg(sampling).dropna()
        data1d = data60.reset_index().groupby('symbol').resample(
            '1d', on='date').agg(sampling).dropna()

        resampled_data = {
            "idata_3min": data3.reset_index(),
            "idata_5min": data5.reset_index(),
            "idata_15min": data15.reset_index(),
            "idata_60min": data60.reset_index(),
            "idata_1day": data1d.reset_index(),
        }

        resample_time = time.time() - resampling_start

        summary_table = Table(
            box=box.SQUARE, title="Resampling Summary", header_style="bold cyan")
        summary_table.add_column("Interval", justify="center")
        summary_table.add_column("Records", justify="right")
        for k, v in resampled_data.items():
            summary_table.add_row(k, str(len(v)))
        console.print(summary_table)

        console.print(
            f"[green]Resampling completed in {resample_time:.2f}s[/green]")
        return resampled_data


def api_request(api, instrument_list, from_date, to_date, interval):
    req_start = time.time()
    request_count = 0
    complete_data = pd.DataFrame()
    max_retries = 3
    retry_delay = 2

    with Progress(
        "[progress.description]{task.description}",
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        "{task.completed}/{task.total}",
        "•",
        "[cyan]{task.fields[rate]}[/cyan]",
        transient=False,
    ) as progress:
        task = progress.add_task("Downloading", total=len(
            instrument_list), rate="0.00 req/s")

        for index, instrument in instrument_list.iterrows():
            attempt = 0
            while True:
                if attempt >= max_retries:
                    console.print(
                        f"[red]Max retries reached for {instrument['tradingsymbol']}. Skipping...[/red]")
                    break

                try:
                    data = api.historical_data(
                        instrument["instrument_token"],
                        from_date=from_date,
                        to_date=to_date,
                        interval=interval,
                    )

                    request_count += 1
                    if len(data) != 0:
                        data = pd.DataFrame(data)
                        data['symbol'] = instrument['name'] + '-I'
                        data["tradingsymbol"] = instrument['tradingsymbol']
                        data['open'] = data['open'].astype(float).round(2)
                        data['high'] = data['high'].astype(float).round(2)
                        data['low'] = data['low'].astype(float).round(2)
                        data['close'] = data['close'].astype(float).round(2)
                        data['volume'] = data['volume'].astype(int)
                        complete_data = pd.concat(
                            [complete_data, data], ignore_index=True)
                    else:
                        time.sleep(0.01)  # slight delay for empty data

                    if instrument['name'] == 'M&M':
                        print(data)
                    if attempt > 0:
                        console.print(
                            f"[green]Successfully downloaded {instrument['tradingsymbol']} after {attempt} retries.[/green]")
                    break  # exit retry loop on success
                except Exception as e:
                    console.print(
                        f"[red]Error downloading {instrument['tradingsymbol']}:[/red] {e}")
                    if str(e) == "Too many requests":
                        console.print(
                            "[yellow]Rate limit exceeded. Waiting before retrying...[/yellow]")
                        time.sleep(0.001)
                        attempt += 1
                        continue

            elapsed = time.time() - req_start
            rate = f"{request_count / elapsed:.2f} req/s"
            if request_count / elapsed > 15:
                time.sleep(0.01)  # brief pause to respect rate limits
            progress.update(task, advance=1, rate=rate)

    console.print(
        f"[green]Download completed for {len(instrument_list)} instruments.[/green]")
    console.print(f"Time taken: {time.time() - req_start:.2f}s")
    return complete_data


def print_info(exchange, segment, instrument_list, interval, current_expiry, previous_expiry, from_date, to_date, no_of_days, profile):
    table_info = Table(box=box.SIMPLE_HEAVY)
    table_info.title = f"[bold magenta]Historical Data Fetcher"
    table_info.add_column("User Id", style="cyan", justify="right")
    table_info.add_column("Exchange", style="cyan", justify="right")
    table_info.add_column("Segment", style="bold white")
    table_info.add_column("Total Instruments", style="bold white")
    table_info.add_column("Interval", style="bold white")

    table_info.add_row(
        profile["user_id"],
        exchange.upper(),
        segment,
        str(len(instrument_list)),
        interval,
    )

    console.print(Align.center(table_info))

    table_info = Table(box=box.SIMPLE_HEAVY)
    table_info.title = "[bold magenta]Expiry and Date Range Info[/bold magenta]"
    table_info.add_column("Current Expiry", style="bold white")
    table_info.add_column("Previous Expiry", style="bold white")
    table_info.add_column("From Date", style="bold white")
    table_info.add_column("To Date", style="bold white")
    table_info.add_column("No. of Days", style="bold white")
    table_info.add_row(
        str(current_expiry.date()),
        str(previous_expiry.date()),
        str(from_date),
        str(to_date),
        str(no_of_days)
    )

    console.print(Align.center(table_info))


def instruments(exchange='nfo'):

    expiry_dates = pd.read_csv(os.path.join(
        BASE_DIR, "instruments/expiries.csv"))
    expiry_dates.sort_values(by="expiry", inplace=True)
    expiry_dates["expiry"] = pd.to_datetime(expiry_dates["expiry"])

    current_expiry = expiry_dates[expiry_dates["expiry"] >= pd.to_datetime(
        datetime.now().date())].iloc[0]["expiry"]
    previous_expiry = expiry_dates[expiry_dates["expiry"] < pd.to_datetime(
        datetime.now().date())].iloc[-1]["expiry"]

    # if instruments-{exchange}.csv is not todays date then need to dowload todays from https://api.kite.trade/instruments
    if not os.path.exists(os.path.join(BASE_DIR, f"instruments/instruments-{exchange}.csv")) or \
            pd.to_datetime(os.path.getmtime(os.path.join(BASE_DIR, f"instruments/instruments-{exchange}.csv")), unit='s').date() != datetime.now().date():
        console.print(
            f"[bold yellow]Instruments file for {exchange} is outdated or missing. Downloading...[/bold yellow]")
        # Download the instruments file
        url = f"https://api.kite.trade/instruments"
        response = requests.get(url)
        if response.status_code == 200:
            with open(os.path.join(BASE_DIR, f"instruments/instruments-{exchange}.csv"), "wb") as f:
                f.write(response.content)
        else:
            console.print(
                f"[bold red]Failed to download instruments file for {exchange}[/bold red]")
            return None, None, None

    instrument_list = pd.read_csv(os.path.join(
        BASE_DIR, f"instruments/instruments-{exchange}.csv"))
    instrument_list['expiry'] = pd.to_datetime(
        instrument_list['expiry'], errors='coerce')
    instrument_list = instrument_list[instrument_list['exchange'] == exchange.upper(
    )]
    instrument_list = instrument_list[instrument_list['segment'] == 'NFO-FUT']

    instrument_list = instrument_list[instrument_list['expiry'] == pd.to_datetime(
        current_expiry)]
    instrument_list.sort_values(by="tradingsymbol", inplace=True)
    instrument_list.reset_index(drop=True, inplace=True)

    return instrument_list, current_expiry, previous_expiry


def banknifty_options_chain() -> pd.DataFrame:
    expiry_dates = pd.read_csv(os.path.join(
        BASE_DIR, "instruments/expiries.csv"))
    expiry_dates.sort_values(by="expiry", inplace=True)
    expiry_dates["expiry"] = pd.to_datetime(expiry_dates["expiry"])

    current_expiry = expiry_dates[expiry_dates["expiry"] >= pd.to_datetime(
        datetime.now().date())].iloc[0]["expiry"]

    instrument_list = pd.read_csv(os.path.join(
        BASE_DIR, f"instruments/instruments-nfo.csv"))
    instrument_list['expiry'] = pd.to_datetime(
        instrument_list['expiry'], errors='coerce')
    instrument_list = instrument_list[instrument_list['exchange'] == 'NFO']
    instrument_list = instrument_list[instrument_list['segment'] == 'NFO-OPT']
    instrument_list = instrument_list[instrument_list['name'] == 'BANKNIFTY']
    instrument_list = instrument_list[instrument_list['expiry'] == pd.to_datetime(
        current_expiry)]
    instrument_list.sort_values(by="tradingsymbol", inplace=True)
    instrument_list.reset_index(drop=True, inplace=True)

    return instrument_list
