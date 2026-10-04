import time
import queue
import pandas as pd
import threading
from datetime import datetime
from typing import Dict, Optional
import warnings

warnings.filterwarnings("ignore")

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import BarData

#excuse the absolute yap here, but being new to the IKBR API i'm just making sure i know absolutely everything that's going on and don't forget it later


class IBConnector(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        
        #placeholder for when orders become relevant
        #bear in mind you might want to actually check what the next valid id is via the API rather than incrementing (which is the simpler solution for now)
        #this is only relevant if more than one client connects at once (like if i place manual trades while the script runs)
        self.next_order_id = None
        self.connected_event = threading.Event()

        #keys will be the request id
        #entries will be lists of dictionaries representing each bar
        self.historical_data = {}

        #again the keys are request ids
        #the entries will be a threading.Event to say if that request is ready
        self.historical_data_ready = {}

        #will hold arrays, where [0] is the message and [1] is whether it is a critical error or not
        self.req_logs = {}
        self.benign_errors = [2104, 2106, 2158]

        #counter for IDs
        self.next_req_id = 1

        self.api_thread = None


    #OVERRIDES

    #this only exists to check if the connection is LIVE
    #overriding the APIs method for now, just so we can check. might want to actually return something later
    def nextValidId(self, orderId):
        self.connected_event.set()

    #this is called by the API per bar, an override here appends it to the correct request id entry.
    def historicalData(self, reqId, bar: BarData):
        self.historical_data[reqId].append({
            "timestamp": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })
    
    
    def historicalDataEnd(self, reqId, start, end):
        self.historical_data_ready[reqId].set()


    #overriding the basic error handling to differentiate between benign and serious errors
    #for req_logs index 0 just contains the error message, index 1 contains wether it's critcal or not
    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        try:
            if reqId == -1:
                print(f"[TIME: {errorTime}][CONNECTION STATUS: {errorCode}] {errorString}")
            elif (not (errorCode in self.benign_errors)):
                self.req_logs[reqId] = [f"[TIME: {errorTime}][ERROR {errorCode}][ID: {reqId}] {errorString}", True]
                self.historical_data_ready[reqId].set()
            else:
                self.req_logs[reqId] = [f"[TIME: {errorTime}][LOG {errorCode}][ID: {reqId}] {errorString}", False]
        except KeyError:
            print(f"[TIME: {errorTime}][ERROR {errorCode}][ID: {reqId}] {errorString}")

    #END OF OVERRIDES
    
    
    def fetch_price_data(self, symbol, duration, bar_size="1 day", end_datetime="", exchange="SMART", currency="USD", sec_type="STK", timeout=10):
        contract = Contract()
        contract.symbol, contract.secType, contract.exchange, contract.currency = symbol, sec_type, exchange, currency

        req_id = self.next_req_id
        self.next_req_id += 1

        self.historical_data[req_id] = []
        self.historical_data_ready[req_id] = threading.Event()

        
        self.reqHistoricalData(
            reqId=req_id,
            contract=contract,
            endDateTime=end_datetime, #format is yyyymmdd-hh:mm:ss, a blank entry means "today"
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=1,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[]
        )

        finished_in_time = self.historical_data_ready[req_id].wait(timeout=timeout)

        
        critical_error = False

        if req_id in self.req_logs:
            log_message = self.req_logs[req_id][0]
            print(f"{log_message}")

            if self.req_logs[req_id][1]:
                critical_error = True
            
            self.req_logs.pop(req_id)
                
        bars = self.historical_data.pop(req_id)
        self.historical_data_ready.pop(req_id)


        if (not finished_in_time) and (not critical_error):
            raise TimeoutError(f"Historical data request for {symbol} timed out after {timeout}s")
        elif critical_error:
            raise RuntimeError(log_message)

        
        

        if bars:
            df = pd.DataFrame(bars)
        else:
            raise ValueError(f"No data returned for {symbol}")


        if bar_size in ("1 day", "1 week", "1 month"):
            # IBKR ignores formatDate=2 for these, it's always yyyymmdd regardless.
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="s")
        df = df.set_index("timestamp")

        if df is not None:
            print("[DATAFRAME READY]")
            
        return df
    
    #not to be confused with connect(), which is an inherited method.
    def connect_and_start(self, ip="127.0.0.1", port=7497, clientId=1):
        self.connect(ip, port, clientId=clientId)

        self.api_thread = threading.Thread(target=self.run, daemon=True)
        self.api_thread.start()

        finished_in_time = self.connected_event.wait(timeout=10)
        
        if not finished_in_time:
            raise TimeoutError("Attempt to connect to API failed due to timeout")

        



if __name__ == "__main__":
    api = IBConnector()
    api.connect_and_start()

    df = api.fetch_price_data("SPY", duration="30 D", bar_size="1 day", timeout=30)
    print(df)

    api.disconnect()
