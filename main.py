import time
from multiprocessing import Pool
from datetime import datetime, timedelta
import os
import calendar
import re

from bs4 import BeautifulSoup
import requests
import pandas as pd
import xlsxwriter

regex = '[+-]?[0-9]*[.]?[0-9]+[m]'
p = re.compile(regex)
regex2 = '[+-]?[0-9]*[.]?[0-9]+'
p2 = re.compile(regex2)


def get_states_urls(url):
    urls = []

    r = requests.get(url)
    s = BeautifulSoup(r.text, "html.parser")
    home_menu = s.find("div", {"id": "home-menu"})
    menu = home_menu.find("ul")
    tabs = menu.findAll("li", recursive=False)
    states = tabs[4].findAll("li")
    for state in states:
        if state.text not in ['Tasmania', 'Northern Territory']:
            href_to_state = state.find("a", href=True)['href']
            urls.append('https://racingaustralia.horse' + href_to_state)
    return urls


def get_meetings_urls(state_url):
    print(state_url)
    urls = []

    r = requests.get(state_url)
    s = BeautifulSoup(r.text, "html.parser")
    table = s.find("table", {"class": "race-fields"})
    rows = table.findAll("tr")[1:]
    today_rows = [x for x in rows if x.find("td").text == complete_day]
    for row in today_rows:
        meeting_name = row.find('a').text
        href_to_meeting = row.find("a", href=True)['href']
        href_to_meeting = href_to_meeting.replace(' ', '%20')
        url = 'https://racingaustralia.horse' + href_to_meeting
        urls.append((meeting_name, url))
    return urls


def get_races_info(meeting_tuple):
    print(meeting_tuple[1])
    data = {'DATE': [], 'TRACK': [], 'STATUS': [], 'RAIL': [], 'TYPE': [], 'PEN': [],
            'RACE': [], 'DIST': [], 'STAKE': [], 'TRK COND': [], 'TIME': [], '600M': [],
            'FINISH': [], 'No.': [], 'HORSE': [], 'TRAINER': [], 'JOCKEY': [], 'MGN': [],
            'Bar.': [], 'WEIGHT': [], 'FAV': [], 'SPR': []}

    r = requests.get(meeting_tuple[1])
    s = BeautifulSoup(r.text, "html.parser")

    # Meeting info
    date = input_date.replace('-', '/')
    track = meeting_tuple[0].split("-")[0].strip()[:-3]
    status = meeting_tuple[0].split("-")[1].strip()[:4]
    if 'prof' not in status.lower() and 'tria' not in status.lower():
        track = meeting_tuple[0].split("-")[0].strip() + ' ' + meeting_tuple[0].split("-")[1].strip()[:-3]
        track = track.replace('Synthetic', '')
        status = meeting_tuple[0].split("-")[2].strip()[:4]

    meeting_details = s.find("div", {"class": "race-venue-bottom"}).find("div", {"class": "col1"})
    cells = meeting_details.text.split(':')

    rail = cells[1][:-18].strip()
    result = p.search(rail)
    if result:
        rail = result.group()[:-1]
        if '+' in rail:
            rail = rail[1:]
    elif 'True' in rail or 'Normal' in rail or rail == '':
        rail = 0
    elif 'metres' in rail.lower() or 'meters' in rail.lower():
        rail = p2.search(rail).group()

    type = cells[3][:-15].strip()
    if type == 'Sy':
        type = 'Synthetic'
    pen = cells[6][:-17].strip()
    try:
        pen = float(pen)
    except:
        pen = ''

    # Races
    races_details_tables = s.findAll("table", {"class": "race-title"})
    races_horses_tables = s.findAll("table", {"class": "race-strip-fields"})

    # For Races
    for i in range(len(races_details_tables)):
        race_title = races_details_tables[i].find("span").text
        race = race_title.split('-')[0].strip().split(" ")[-1]
        dist = race_title.split(')')[-2].split('(')[1].split(' ')[0]
        race_details = races_details_tables[i].find('td')
        if 'ABANDONED' in race_details.text:
            continue

        race_details_text = race_details.text
        try:
            stake = race_details_text.split('Of $')[1].split('.')[0].strip().replace(',', '')
        except:
            stake = 0
        try:
            trk_cond = race_details_text.split('Track Condition: ')[1].split('Time')[0].strip().split(' ')[-1]
        except:
            continue
        time = race_details_text.split('Time: ')[1].split('Last')[0].strip()
        try:
            m600 = race_details_text.split('Last 600m: ')[1].split('Timing')[0].strip()
        except:
            m600 = 0
            time = time.split('Timing')[0].strip()

        # For Race Rows
        horses_rows = races_horses_tables[i].findAll('tr')[1:]
        for horse_row in horses_rows:
            data['DATE'].append(date)
            data['TRACK'].append(track)
            data['STATUS'].append(status)
            data['RAIL'].append(float(rail))
            data['TYPE'].append(type)
            data['PEN'].append(pen)
            data['RACE'].append(int(race))
            data['DIST'].append(dist)
            data['STAKE'].append(int(stake))
            data['TRK COND'].append(trk_cond)
            data['TIME'].append(time)
            data['600M'].append(m600)
            get_horse_info(horse_row, data)

    df = pd.DataFrame.from_dict(data)
    return df


def get_horse_info(horse_row, data):
    columns = horse_row.findAll("td")
    finish = columns[1].text
    if finish == '' or finish == 'SB':
        data['DATE'].pop()
        data['TRACK'].pop()
        data['STATUS'].pop()
        data['RAIL'].pop()
        data['TYPE'].pop()
        data['PEN'].pop()
        data['RACE'].pop()
        data['DIST'].pop()
        data['STAKE'].pop()
        data['TRK COND'].pop()
        data['TIME'].pop()
        data['600M'].pop()
        return
    no = columns[2].text
    horse = columns[3].text
    trainer = columns[4].text
    jockey = columns[5].text
    mgn = columns[6].text
    bar = columns[7].text
    weight = columns[8].text
    if columns[10].text[-1] == 'F':
        fav = 'F'
        spr = columns[10].text[:-1][1:]
        if spr[-1] == 'E':
            spr = spr[:-1]
    else:
        fav = ''
        spr = columns[10].text[1:]
    if spr == '':
        spr = 999

    data['FINISH'].append(finish)
    data['No.'].append(no)
    data['HORSE'].append(horse)
    data['TRAINER'].append(trainer)
    data['JOCKEY'].append(jockey)
    data['MGN'].append(mgn)
    data['Bar.'].append(int(bar))
    data['WEIGHT'].append(weight)
    data['FAV'].append(fav)
    data['SPR'].append(float(spr))


if __name__ == "__main__":
    print('Base url = https://racingaustralia.horse')
    print('Default date = yesterday')
    input_date = input('Enter date (ex.: 2020-07-13): ')
    if not input_date:
        input_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    date_format = datetime.strptime(input_date, '%Y-%m-%d')
    day_name = calendar.day_abbr[date_format.weekday()]
    day_number = date_format.strftime('%d')
    month = calendar.month_abbr[date_format.month]
    complete_day = day_name + " " + day_number + '-' + month
    print(complete_day)

    print('Started scrapping')
    start = time.time()

    final_df = pd.DataFrame()
    meetings_tuples = []
    states_urls = get_states_urls('https://racingaustralia.horse')
    for state_url in states_urls:
        state_meetings_urls = get_meetings_urls(state_url)
        meetings_tuples = meetings_tuples + state_meetings_urls

    for mt in meetings_tuples:
        final_df = final_df.append(get_races_info(mt))

    # Test
    # mt =
    # ('Grafton NSW - Professional', 'https://racingaustralia.horse/FreeFields/Results.aspx?Key=2020Jul09,NSW,Grafton')
    # final_df = final_df.append(get_races_info(mt))

    end = time.time()
    print(end - start)
    print('Finished scrapping')

    print('Generating excel')

    path = os.getcwd()
    writer = pd.ExcelWriter(path + '/' + input_date + ' RA' + '.xlsx', engine='xlsxwriter')
    final_df.to_excel(writer, sheet_name='Sheet1', index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.freeze_panes(1, 0)
    workbook.close()

    print('Excel exported successfully')
