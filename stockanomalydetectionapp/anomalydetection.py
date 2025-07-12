
import os
from collections import defaultdict, deque
from sklearn.ensemble import IsolationForest
from turtledemo.paint import changecolor

import numpy as np
import json
from dotenv import load_dotenv
from pygments.lexer import default
from quixstreams import Application


load_dotenv()

#quix application initialization
app = Application(
    consumer_group="stock_anomaly_detector",
    auto_create_topics=True,
    auto_offset_reset="earliest",
    loglevel="INFO"
)

#Topics
input_topic=app.topic(os.environ["input_topic"]) #stock
output_topic=app.topic(os.environ["output_topic"]) #anomalies

high_volume_threshold = defaultdict(lambda: 10000)
rapid_price_change_threshold = defaultdict(lambda: 0.05) #5% increase
transaction_history = defaultdict(lambda: deque(maxlen=100))
fit_prices_all = []
is_fitted = False

isolation_forest = IsolationForest(contamination=0.01, n_estimators=1000)

processed_sequence = set()

def high_volume_rule(trade_data, high_volume_threshold):
    return trade_data["size"] > high_volume_threshold[trade_data['symbol']]

def rapid_price_change(trade_data, history, rapid_price_change_threshold):
    symbol = trade_data['symbol']
    current_price = trade_data['price']

    if not history[symbol]:
        history[symbol].append(current_price)
        return False

    last_price = history[symbol][-1]
    price_change = abs((current_price - last_price)/last_price)

    history[symbol].append(current_price)

    return price_change > rapid_price_change_threshold[symbol]

def isolation_forest_anomaly(trade_date, fit_prices):
    global is_fitted
    current_price = trade_date['price']
    fit_prices.append(current_price)

    if len(fit_prices) < 100:
        return False

    fit_price_normalized = (np.array(fit_prices) - np.mean(fit_prices)) / np.std(fit_prices)
    prices_reshaped = fit_price_normalized.reshape(-1, 1)

    if len(fit_prices) % 1000 == 0:
        isolation_forest.fit(prices_reshaped)
        is_fitted = True

    if not is_fitted:
        return False

    current_price_normalized = (current_price - np.mean(fit_prices)) / np.std(fit_prices)
    score = isolation_forest.decision_function(np.array([[current_price_normalized]]))

    return score < 0 #anomalies indicated by negative scores

def process_trade(raw_trade_data):

    try:
        trade_data = json.loads(raw_trade_data) if isinstance(raw_trade_data, str) else raw_trade_data
    except json.JSONDecodeError:
        print(f"Failed to parse trade data: {raw_trade_data}")
        return

    sequence = trade_data['sequence']

    if sequence in processed_sequence:
        return

    processed_sequence.add(sequence)

    with app.get_producer() as producer:
        anomalies = []

        # 1.  high volume rule
        # 2.  rapid price change
        # 3.  isolation forest anomaly

        if high_volume_rule(trade_data, high_volume_threshold):
            anomalies.append("High Volume")

        if rapid_price_change(trade_data, transaction_history, rapid_price_change_threshold):
            anomalies.append("Rapid Price Change")

        if isolation_forest_anomaly(trade_data, fit_prices_all):
            anomalies.append("Isolation Forest")

        if anomalies:
            trade_data["anomalies"] = anomalies
            print(f"Anomaly detected: {trade_data}")

            #Publish Anomaly to Kafka
            anomaly_data = json.dumps(trade_data)
            producer.produce(
                topic=output_topic.name,
                key=str(trade_data["sequence"]),
                value=anomaly_data
            )



def main():
    stream_df = app.dataframe(input_topic)

    stream_df = stream_df.apply(process_trade)

    # stream_df.to_topic(output_topic)

    app.run(stream_df)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Exiting....')
    # main()