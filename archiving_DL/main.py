import os
import datetime
from datetime import datetime
import shutil
import os
import sys
import time
import datetime
from datetime import datetime as dt

def archiving(name):
    today = datetime.date.today().strftime('%m/%d/%Y')
    print( today)
    for folders in os.listdir("V:\מערכות מידע\מערכות מידע\מחסן נתונים\DataLake\YOSI_SHARED\python\datafortest"):
        print(folders)
        for item in os.listdir(f"V:\מערכות מידע\מערכות מידע\מחסן נתונים\DataLake\YOSI_SHARED\python\datafortest" + '\\' +folders):
            print(item.split('-')[1])
            pattern = '%Y%m%d'
            mydate = dt.strptime(item.split('-')[1], pattern)
            # date = dt.strptime(item.split('-')[1], '%Y%m%d').strftime('%m/%d/%Y')
            epoch = int(time.mktime(time.strptime(mydate, pattern)))
            print(epoch)
        #     for day in os.listdir(os.path.join(year, month)):
        #         date = datetime.date(int(year), int(month), int(day))
        #         print(date)
            age = today - date
            print(age)
        #     if age > datetime.timedelta(days=20):
        #         # shutil.rmtree(os.path.join(year, month, day))
        #         print(os.path.join(year, month, day))

def remove(path):
    """
    Remove the file or directory
    """
    if os.path.isdir(path):
        try:
            # os.rmdir(path)
            print(path)
        except OSError:
            print("Unable to remove folder: %s" % path)
    else:
        try:
            if os.path.exists(path):
                # os.remove(path)
                print(path)
        except OSError:
            print("Unable to remove file: %s" % path)


def cleanup(number_of_days, path):
    """
    Removes files from the passed in path that are older than or equal
    to the number_of_days
    """
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for root, dirs, files in os.walk(path, topdown=False):
        for file_ in files:
            full_path = os.path.join(root, file_)
            stat = os.stat(full_path)

            if stat.st_mtime <= time_in_secs:
                remove(full_path)

        if not os.listdir(root):
            remove(root)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    archiving('PyCharm')
    # days, path = int(sys.argv[1]), sys.argv[2]
    # cleanup(days, path)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
