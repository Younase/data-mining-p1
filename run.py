import csv
import datetime
import os
import re  # rejex
import sys
import threading
import time
import traceback
from contextlib import closing

from bs4 import BeautifulSoup
from requests import get
from requests.exceptions import RequestException

# 15 mai 2015 11:11
# csv date format yyyy-mm-dd HH:mm:ss

# redacted domain name, take a guess?
DOMAIN="website"

def main(argv):
    global today, yesterday, datetable, data, filename, link, threads, attributs

    num_threads = 40
    epoch = 10
    attributs = [
        "title",
        "date",
        "city",
        "Domaine ",
        "Fonction ",
        "Type de contrat ",
        "Nom de la société ",
        "Salaire ",
        "Niveau d'études ",
        "desc",
    ]  # ,'description'
    threads = []
    filename = "informatique.db"
    data = []  # contains ad info (each ad=dict)
    datetable = {
        "Mai": "5",
        "Avr": "4",
        "Mar": "3",
        "Fév": "2",
        "Jan": "1",
        "Déc": "12",
        "Nov": "11",
        "Oct": "10",
        "Sep": "9",
        "Aoû": "8",
        "Jul": "7",
        "Jun": "6",
    }
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    
    link = f"https://www.{DOMAIN}.com/maroc/offres-emploi-b309.html?f_3=Informatique&pge="
    p = []
    if len(argv) < 4:
        print(
            "not enough args given!\n a1: ads to scrape from first page only\n a2:scrape all pages\n a3:start index\n a4:init csv "
        )
        print(
            'Exemples:\n "py -3 run.py 10 0 1 1" pour collecter 10 annonces de la premiere page '
        )
        print(' "py -3 run.py 0 1 1 1"  pour collecter toutes les annonces')
        return
    # scrape_all=
    if int(argv[3]) == 1:
        process(0, [])  # create file
    i = int(argv[2])
    p = get_ads(link + "1")
    if int(argv[0]) != 0:
        p = p[: int(argv[0])]
        for e in p:
            c = get_desc(e)
            if c != None:
                data.append(c)
        process(1, data)
        return

    if int(argv[1]) != 0:
        p = get_ads(link + str(i))
        while len(p):
            while len(threads) < num_threads:
                threads.append(
                    threading.Thread(
                        target=scrape,
                        args=(
                            i,
                            epoch,
                        ),
                    )
                )
                threads[-1].start()
                i += epoch
            while len(threads) > num_threads - 3:
                for t in threads:
                    if not t.isAlive():
                        threads.remove(t)
                try:
                    time.sleep(0.3)
                except KeyboardInterrupt:
                    print("Keyboard interrupt")
                    os.system("taskkill /f /pid " + str(os.getpid()))

            p = get_ads(link + str(i))

        return


# create scraper class with interruption flag
def scrape(start, epoch):
    data = []
    p = get_ads(link + str(start))
    if not len(p):
        return
    for i in range(start, start + epoch):
        p = get_ads(link + str(i))
        print("page " + str(i))
        if p != None:
            for e in p:
                # try:
                c = get_desc(e)
                if c != None:
                    data.append(c)
    process(1, data)
    print(str(start) + "->" + str(start + epoch - 1))
    return


def process(a, data):
    # global data

    if a == 0:
        with open(filename + ".csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                ["title"]
                + ["date"]
                + ["city"]
                + ["Domaine "]
                + ["Fonction "]
                + ["Type de contrat "]
                + ["Nom de la société "]
                + ["Salaire "]
                + ["Niveau d'études "]
                + ["description"]
            )
    else:
        with open(filename + ".csv", "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for e in data:
                res = []
                # title date city ,'Domaine ','Fonction ','Type de contrat ','Nom de la société ','Salaire ',"Niveau d'études ",. desc
                for a in attributs:
                    if a in e.keys():
                        res.append(e[a])
                    else:
                        res.append("")
                # for k in e.keys():
                #     res.append(e[k])
                try:
                    writer.writerow(res)
                except Exception:
                    print("exception")
                    pass


def get_ads(link):  # returns list of ads on given link
    raw_html = simple_get(link)
    if raw_html == None:
        print("Error getting link")
        return
    html = BeautifulSoup(raw_html, "html.parser")
    date = html.select(".time")
    list = html.select(".cars-list")[0]  # liste d'annonces LOL

    def filtre(tag):
        return ("class" in tag) and tag["class"] != "adslistingpos"

    list = list.find_all("li")
    # print(type(list))
    links = []
    j = 0
    for e in list:
        if e.has_attr("class") and "adslistingpos" in e["class"]:
            # print(e['class'])
            # print('true')
            continue
        # print(date[j].text)
        date[j] = date[j].text.replace("\r", "").replace("\n", "").replace("\t", "")
        date[j] = re.sub("^ +", "", date[j])

        date[j] = re.sub(" +", " ", date[j])
        # print(date[j])
        if "hui" in date[j]:
            date[j] = str(today) + " " + date[j].split(" ", 1)[1]
        elif "Hier" in date[j]:
            date[j] = str(yesterday) + " " + date[j].split("Hier", 1)[1]
        else:
            tempo = date[j].split(" ")
            date[j] = (
                tempo[2] + "-" + datetable[tempo[1]] + "-" + tempo[0] + " " + tempo[3]
            )
            # print(date[j])
        date[j] += ":00"  # seconds
        links.append({"link": e.find("a")["href"], "date": date[j]})
        j += 1
    return links


def get_desc(e):
    html = simple_get(f"https://www.{DOMAIN}.com/" + e["link"])

    if html == None:
        # print('noni')
        return None
    html = BeautifulSoup(html, "html.parser")
    if len(html.select(".erreur_404")):
        # print('404')
        return None

    # title
    extra = html.select(".description")[0]
    info = html.select(".info-holder")[0].find_all("li")

    res = {}
    res["title"] = extra.h1.text
    res["date"] = e["date"]
    res["city"] = info[0].text

    extra = extra.select(".extraQuestionName")  # extraquestions not always available
    if len(extra) > 0:
        extra = extra[0].find_all("li")

        # extra questions
        for e in extra:
            a, b = e.text.split(":", 1)
            res[a] = b
    # cleaning description from stray characters
    desc = (
        html.select(".block")[1]
        .text.replace("\r", " ")
        .replace("\n", " ")
        .replace("\t", " ")
    )
    desc = re.sub("^ +", "", desc)
    desc = re.sub(" +", " ", desc)
    res["desc"] = desc

    # for a in res.keys():
    #     print(a+':'+res[a])
    return res


def simple_get(url):
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error("Error during requests to {0} : {1}".format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers["Content-Type"].lower()
    return (
        resp.status_code == 200
        and content_type is not None
        and content_type.find("html") > -1
    )


def log_error(e):
    print(e)


if __name__ == "__main__":
    main(sys.argv[1:])
