import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pushover_complete import PushoverAPI
import datetime
import time
import logging
import sys
import requests.exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('scalping_bot.log'),  # Save logs to file
        logging.StreamHandler(sys.stdout)  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("scalpingagent-470902-416ea6e01076.json", scope)
try:
    client = gspread.authorize(creds)
    sheet = client.open_by_key("16-qcnZ6NcUZ_dAtnYbXEzmFInxVuTAe9S37ZiC7f9BI").sheet1  # Assumes default sheet
    logger.info("Successfully connected to Google Sheets")
except Exception as e:
    logger.error(f"Failed to connect to Google Sheets: {e}")
    raise

# Pushover setup
pushover_client = PushoverAPI("avbah8u29fyhgfixb7z9sb7r93wnk7")  # API Token
logger.info("Initialized Pushover client")

# Deep link for Chivo Wallet
CHIVO_DEEP_LINK = "chivo://"  # Test this; fallback: "https://chivowallet.com"
GOOGLE_FORM_URL = "https://docs.google.com/forms/d/your_form_id/viewform"  # Replace with your Google Form URL

# Fetch Bitso price with retry
def fetch_price(max_retries=3, retry_delay=5):
    url = "https://api.bitso.com/v3/ticker/?book=btc_usd"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "payload" in data and "last" in data["payload"]:
                price = float(data["payload"]["last"])
                logger.info(f"Fetched Bitso price: ${price:.2f}")
                return price
            else:
                logger.error(f"Invalid Bitso API response: {data}")
                raise ValueError("Invalid response structure")
        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error(f"Failed to fetch Bitso price (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    logger.warning("All retries failed, falling back to alert price")
    return None

# Send Pushover notification with deep link
def send_notification(title, message, user_key="uagwgwxtw1na3dv7hmpftxqw1oh9t7", url=None, url_title="Open Chivo Wallet"):
    try:
        pushover_client.send_message(
            user=user_key,
            message=message,
            title=title,
            url=url,
            url_title=url_title
        )
        logger.info(f"Push notification sent: {title} - {message} (URL: {url})")
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

# Calculate next 3-minute boundary
def get_next_execution_time(last_signal_time=None):
    INTERVAL_SECONDS = 180  # 3 minutes
    OFFSET_SECONDS = 20  # Run 20 seconds after bar close
    now = datetime.datetime.now(datetime.timezone.utc)
    
    if last_signal_time:
        try:
            # Try parsing MM/DD/YYYY HH:MM:SS or ISO format
            try:
                last_time = datetime.datetime.strptime(last_signal_time, '%m/%d/%Y %H:%M:%S')
                last_time = last_time.replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                last_time = datetime.datetime.fromisoformat(last_signal_time.replace('Z', '+00:00'))
            
            # Calculate next boundary in the future
            seconds_since_epoch = int(last_time.timestamp())
            current_seconds = int(now.timestamp())
            intervals_since_signal = ((current_seconds - seconds_since_epoch) // INTERVAL_SECONDS + 1)
            next_boundary = seconds_since_epoch + (intervals_since_signal * INTERVAL_SECONDS)
            next_execution = datetime.datetime.fromtimestamp(next_boundary + OFFSET_SECONDS, datetime.timezone.utc)
            
            # Ensure next execution is in the future
            if next_execution <= now:
                logger.warning(f"Calculated execution time {next_execution.isoformat()} is in the past, advancing to next interval")
                intervals_since_signal += 1
                next_boundary = seconds_since_epoch + (intervals_since_signal * INTERVAL_SECONDS)
                next_execution = datetime.datetime.fromtimestamp(next_boundary + OFFSET_SECONDS, datetime.timezone.utc)
            
            logger.info(f"Calculated next execution from signal time {last_signal_time}: {next_execution.isoformat()}")
            return next_execution
        except Exception as e:
            logger.error(f"Failed to parse last signal time {last_signal_time}: {e}")
    
    # Fallback: Use current time, round up to next 3-minute boundary
    seconds_since_epoch = int(now.timestamp())
    next_boundary = ((seconds_since_epoch // INTERVAL_SECONDS) + 1) * INTERVAL_SECONDS
    next_execution = datetime.datetime.fromtimestamp(next_boundary + OFFSET_SECONDS, datetime.timezone.utc)
    logger.info(f"Fallback to current time for next execution: {next_execution.isoformat()}")
    return next_execution

# Main loop
while True:
    start_time = time.time()
    logger.info(f"Starting cycle at {datetime.datetime.now(datetime.timezone.utc).isoformat()}")

    try:
        rows = sheet.get_all_values()
        logger.info(f"Retrieved {len(rows)} rows from Google Sheets")
        open_positions = [row for row in rows[1:] if row[1].lower() == "buy" and not any(r[1].lower() == "sell" and r[2] == row[2] for r in rows)]
        alerts = [row for row in rows if row[1].lower() in ["buy_signal", "sell_signal"]][-1:]  # Latest alert
        logger.info(f"Found {len(open_positions)} open positions, {len(alerts)} new alerts")

        # Get timestamp of latest alert for synchronization
        last_signal_time = alerts[0][0] if alerts else None

        for alert in alerts:
            action, alert_price = alert[1].lower(), float(alert[3]) if alert[3] else 0
            current_price = fetch_price() or alert_price
            logger.info(f"Processing alert: {action}, Price: ${alert_price:.2f}, Current Price: ${current_price:.2f}")
            if action == "buy_signal" and not open_positions:
                message = f"BUY SIGNAL: Consider trading in Chivo at ${current_price:.2f}, log via Form if executed."
                send_notification("BTC Buy Signal", message, url=CHIVO_DEEP_LINK, url_title="Open Chivo Wallet")
            elif action == "sell_signal" and open_positions:
                pos = open_positions[-1]
                entry_price, btc_amount = float(pos[3]), float(pos[2])
                profit = btc_amount * (current_price - entry_price)
                message = f"SELL SIGNAL: Close position at ${current_price:.2f}, Profit: ${profit:.2f}"
                send_notification("BTC Sell Signal", message, url=CHIVO_DEEP_LINK, url_title="Open Chivo Wallet")
                logger.info(f"Log the sell trade at: {GOOGLE_FORM_URL}")

        if open_positions:
            current_price = fetch_price()
            if current_price:
                for pos in open_positions:
                    entry_price, btc_amount = float(pos[3]), float(pos[2])
                    profit = btc_amount * (current_price - entry_price)
                    logger.info(f"Open Position: {btc_amount:.6f} BTC at ${entry_price:.2f}, Current: ${current_price:.2f}, Profit: ${profit:.2f}")
            else:
                logger.warning("No current price available for open position updates")
    except Exception as e:
        logger.error(f"Error in cycle: {e}")

    # Synchronize next execution
    next_execution = get_next_execution_time(last_signal_time)
    seconds_until_next = (next_execution - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    while seconds_until_next > 0:
        logger.info(f"Awaiting next execution... {int(seconds_until_next)} seconds remaining")
        time.sleep(min(seconds_until_next, 10))  # Update every 10 seconds
        seconds_until_next = (next_execution - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    logger.info("Cycle completed, starting next cycle")
