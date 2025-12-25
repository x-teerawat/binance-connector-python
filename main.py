from flask import Flask
from update_data import update_data

app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_job():
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    for symbol in symbols:
        update_data(symbol)
    return "OK", 200
