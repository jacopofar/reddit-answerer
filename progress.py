import time
from datetime import datetime

class Progress_tracker:
    def __init__(self,total,start_position=0,decay=0.001):
        self.total = total
        self.latest_time = time.time()
        self.latest_position = start_position
        self.steps_per_second = -1
        self.decay = decay
    def report_progress(self,position):
        latest_speed = (position-self.latest_position) / (time.time()-self.latest_time)
        if(self.steps_per_second == -1):
            self.steps_per_second = latest_speed
        else:
            self.steps_per_second = latest_speed * self.decay + self.steps_per_second *(1-self.decay)
        self.latest_position = position
        self.latest_time = time.time()
    def estimate_remaining_seconds(self):
        #print("total: {0} latest position: {1} steps per second: {2}".format(self.total,self.latest_position,self.steps_per_second))
        return (self.total-self.latest_position)/self.steps_per_second
    def estimate_end_time(self):
        return time.time()+self.estimate_remaining_seconds()
    def estimate_end_timestamp(self):
        return datetime.fromtimestamp(self.estimate_end_time()).isoformat()
    def speed(self):
        return self.steps_per_second
    def percentage(self):
        return 100*self.latest_position/self.total