from datetime import  date
from dateutil.parser import parse

my_date = now()
# print(my_date.isoformat())

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

def splunk_calc_earliest_time(date_1):
    today = date.today()
    print(f"Today's date: {str(today)}")
    # datetime object containing current date and time
    now = datetime.now()
    print("now =", now)

    if is_date(date_1):
        date_1 = datetime.strptime(date_1, '%Y-%m-%d %H:%M:%S.%f')
        earliest_time = date_1
        earliest_time = earliest_time.isoformat()

        time_delta = (now - date_1)
        total_seconds = time_delta.total_seconds()
        print(f"total_seconds: {str(total_seconds)}")

        minutes = total_seconds / 60
        print(f"total_minutes: {str(minutes)}")

        # FORMAT:
        # earliest_time = YYYY-MM-DDTHH:MM:SS
        # Example: earliest_time = 2017-03-14T10:0:00
        return earliest_time




print(splunk_calc_earliest_time('2020-06-07 14ddd:30:08.000'))



