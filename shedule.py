from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
import datetime

app = Flask(__name__)

def main():
    # Get the current system time in the system's local timezone
    system_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Hello, World! Printed at system time: {system_time}")

# Create a BackgroundScheduler instance
scheduler = BackgroundScheduler(daemon=True)

# Schedule the job to run Monday to Friday at 11:39 IST
scheduler.add_job(
    main,
    'cron',
    day_of_week='mon-fri',
    hour=11,
    minute=42,
    timezone='Asia/Kolkata'
)

scheduler.start()

app.run()
