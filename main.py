from src.grabber import grabber
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count


def list_days():
    delta = datetime.date(pd.to_datetime('2015/02/28 00:00')) - datetime.date(pd.to_datetime('2014/11/01 00:00'))
    return [datetime.date(pd.to_datetime('2014/11/01 00:00')) + timedelta(days=i) for i in range(delta.days + 1)]

if __name__ == "__main__":
    pool = Pool(processes=(cpu_count() - 1))
    pool.map(grabber, list_days())
    pool.close()
