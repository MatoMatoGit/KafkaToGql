import time
from datetime import datetime


def TimestampToDatetime(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")


def DatetimeToTimestamp(dt):
    d = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ")
    return time.mktime(d.timetuple())


class TimestampGen:

    def __init__(self, sample_interval_sec, transmit_interval_sec):
        self.Ts = sample_interval_sec
        self.Tt = transmit_interval_sec
        self.tr = 0
        self.N = 0
        self.Tstart = 0
        self.Index = 0
        self.callcount = 0
        return

    def SetReceiveTimestamp(self, receive_timestamp):
        self.tr = DatetimeToTimestamp(receive_timestamp)
        print("Receive time: {} | {}".format(TimestampToDatetime(self.tr), self.tr))
        self.Tstart = self.tr - self.Tt
        print("Start time: {} | {}".format(TimestampToDatetime(self.Tstart), self.Tstart))

    def SetNumberOfSamples(self, num_samples):
        self.N = num_samples

    def Reset(self):
        self.Index = 0

    def Next(self):
        raise NotImplementedError


class TimestampGenA(TimestampGen):

    def Next(self):
        print("[GenA] Sample index: {}".format(self.Index))
        timestamp = self.Tstart + (self.Index + 1) * self.Ts
        print("[GenA] Sample timestamp: {}".format(timestamp))
        timestamp = TimestampToDatetime(timestamp)
        print("[GenA] Sample datetime: {}".format(timestamp))
        self.Index += 1

        return timestamp


class TimestampGenB(TimestampGen):

    def __init__(self, sample_interval_sec, transmit_interval_sec):
        super().__init__(sample_interval_sec, transmit_interval_sec)
        self.tr_prev = 0

    def SavePreviousReceiveTimestamp(self):
        self.tr_prev = self.tr
        print("[GenB] Saving timestamp: {}".format(self.tr_prev))

    def Next(self):
        print("[GenB] Sample index: {}".format(self.Index))

        # Handle the case where previous receive timestamp is not known.
        if self.tr_prev is 0:
            self.tr_prev = self.tr
        
        f = (self.N - self.Index) / (self.N + 1)
        print("[GenB] Factor: {}".format(f))
        print("[GenB] Prev timestamp: {}".format(self.tr_prev))
        print("[GenB] Timestamp: {}".format(self.tr))
        dT = self.tr - self.tr_prev
        print("[GenB] Delta: {}".format(dT))
        timestamp = self.tr - (f * dT)
        print("[GenB] Sample timestamp: {}".format(timestamp))
        timestamp = TimestampToDatetime(timestamp)
        print("[GenB] Sample datetime: {}".format(timestamp))
        self.Index += 1
        return timestamp

