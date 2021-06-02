from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from datetime import datetime

def sensor():
    """ Function for test purposes. """
    now = datetime.now().strftime("%H:%M:%S")
    print(f"Scheduler is alive! {now}")

sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor,'interval',minutes=1)
sched.start()

app = Flask(__name__)

@app.route("/home")
def home():
    """ Function for test purposes. """
    return "Welcome Home :) !"

if __name__ == "__main__":
    app.run()
