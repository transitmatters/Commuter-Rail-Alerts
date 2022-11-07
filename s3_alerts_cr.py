import json
from chalicelib import MbtaPerformanceAPI, s3


def routes_for_alert(alert):
    routes = set()
    try:
        for alert_version in alert["alert_versions"]:
            for informed_entity in alert_version["informed_entity"]:
                if "route_id" in informed_entity:
                    routes.add(informed_entity["route_id"])
    except KeyError as e:
        print(f"Handled KeyError: Couldn't access {e} from alert {alert}")

    return routes


def key(day):
    return f"Alerts/{str(day)}.json.gz"


def get_alerts(day, routes):
    alerts_str = s3.download(key(day), "utf8")
    alerts = json.loads(alerts_str)[0]["past_alerts"]

    def matches_route(alert):
        targets = routes_for_alert(alert)
        return any(r in targets for r in routes)

    return list(filter(matches_route, alerts))


def store_alerts(day):
    api_data = MbtaPerformanceAPI.get_api_data("pastalerts", {}, day)
    alerts = json.dumps(api_data).encode("utf8")
    s3.upload(key(day), alerts, True)

##========= == Testing get alerts with start date as 10/1 and end date as today - timedelta(days = 2) == ========##

# set date and time

import datetime
import sys
from datetime import date
from datetime import timedelta


# alt_names = [Framingham/Worcester, Middleborough/Lakeville, Newburyport/Rockport, Franklin/Foxboro, Providence/Stoughton, 
# Foxboro Event Service]

#cr_lines =  ["CR-Fairmount", "CR-Framingham/Worcester", "CR-Greenbush", "CR-Kingston", "CR-Middleborough/Lakeville", "CR-Newburyport/Rockport", "CR-Fitchburg", "CR-Franklin/Foxboro", "CR-Haverhill", "CR-Lowell", "CR-Needham", "CR-Providence/Stoughton", "CR-Foxboro Event Service"]

cr_lines = ["CR-Fairmount", "CR-Franklin", "CR-Framingham/Worcester", "CR-Greenbush", "CR-Kingston", "CR-Middleborough/Lakeville", "CR-Newburyport/Rockport", "CR-Fitchburg", "CR-Franklin/Foxboro", "CR-Haverhill", "CR-Lowell","CR-Needham","CR-Providence/Stoughton", "CR-Foxboro Event Service"] # "CR-Fairmount", 

canned_reasons = ["signal issue", "train traffic", "mechanical issue", "late arrival of equipment", "speed restriction", "draw bridge issue", "drawbridge issue", "crew availability issue", "late arrival of train", "bus traffic", "street traffic", "bus/street traffic", "passenger accommodation", "disabled bus", "accident", "track work", "fire or police dept. activity"]

"""
def cr_loop(cr_lines, action_to_do):
    iterator = iter(cr_lines)
    done_looping = False
    while not done_looping:
        try:
            item = next(iterator)
        except StopIteration:
            done_looping = True
        else:
            action_to_do(item)
"""
# for line in cr_lines:
    
    # iterator = iter(cr_lines)
    
    # while start_date <= end_date:
        # item = next(l_iter, "end")
        # alerts = (get_alerts(start_date, [line]))
        # start_date += delta
        # alerts = (get_alerts(start_date, [line]))
        # start_date += delta


        #alerts = (get_alerts("2022-09-12", ["CR-Franklin"]))



# def delay_reasons():
    
start_date = datetime.date(2022, 10, 1)
end_date = date.today() - datetime.timedelta(days = 2) # datetime.date(2022, 10, 19)
# delta time
delta = datetime.timedelta(days=1)

#cr_iter = iter(cr_lines)

import pandas as pd

a = pd.DataFrame()

df = pd.DataFrame(columns=["line_name","alert_ids","full_text","start_time","end_time","reasons_set"])

# i < 0

# while i < len(cr_lines):

while start_date <= end_date:
    for line in cr_lines:
        print(line)
        alerts = (get_alerts(start_date, [line]))
        #start_date += delta
        for alert in alerts:
            #df['line'].concat(line)
            #a.append(alert)
            #df.append(pd.DataFrame([line], columns=['line']))
            print(type(alert))
            row = {}
            row['line_name'] = line
            print(alert["alert_id"])
            row['alert_ids'] = (alert["alert_id"])
            full_text = ""
            end_time = 0
            start_time = 9999999999
            for version in alert["alert_versions"]:
                full_text = full_text + " " + version["header_text"]
                if (len(version["active_period"]) > 0):
                    start = int(version["active_period"][0]["start"])
                    print(version["active_period"][0])
                    end = int(version['active_period'][0]['end']) if 'end' in version['active_period'][0] else sys.maxsize
                    if start < start_time:
                        start_time = start
                    if end > end_time:
                        end_time = end
            print(full_text)
            row['full_text'] = full_text
            # df['full_text'].append(full_text)
            print(start_time)
            try:
                row['start_time'] = datetime.datetime.fromtimestamp(start_time)
            except:
                print("An exception occurred")
            # df['start_time'].append(start_time)
            print(end_time)
            try:
                row['end_time'] = datetime.datetime.fromtimestamp(end_time)
            except:
                print("An exception occurred")
            if "late" or "behind schedule" in full_text.lower():
                # might want to add a set of delay range in the alert
                reasons_set = set()
                for reason in canned_reasons:
                    if reason in full_text.lower():
                        reasons_set.add(reason)
                print(reasons_set)
                row['reasons_set'] = str(reasons_set)
                row['reasons_set'] = row['reasons_set'].replace('set()', 'None') 
                #row[['reason_1','reason_2']]=row['reasons_set'].str.split(',',expand=True)
                
                #expand = row['reasons_set'].apply(pd.Series)
                #row = row.append(expand)
                #row = pd.DataFrame(row['reasons_set'].tolist()).add_prefix('reason_') # last piece
                #if reasons_set is not None:
                #    row['reasons_set'] = str(reasons_set)
                #else:
                #    row['reasons_set'] = 'None'
                print(row)
                # df.join(df['reasons_set'].apply(pd.Series)) # last piece
                df = df.append(row, ignore_index=True)
    start_date += delta
print('==========')
print(df)
df.to_csv('cr_delay.csv', encoding = 'utf-8')
                
                
        #else next(cr_iter)
    
    
# run function
#cr_loop(cr_lines, delay_reasons())


