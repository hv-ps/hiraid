from datetime import datetime

def now(format='%Y-%m-%d %H:%M:%S.%f'):
    return datetime.now().strftime(format)

def timediff(start):
    timerdateformat = '%Y-%m-%d %H:%M:%S.%f'
    end = now()
    fromtime = datetime.strptime(start, timerdateformat)
    totime = datetime.strptime(end, timerdateformat)
    elapsedmilliseconds = int(round(abs((totime - fromtime).total_seconds() * 1000)))
    elapsedseconds = int(round(abs((totime - fromtime).total_seconds())))
    return { 'elapsedmilliseconds':elapsedmilliseconds, 'elapsedseconds':elapsedseconds, 'start': start, 'end': end}