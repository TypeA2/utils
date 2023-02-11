#!/usr/bin/env python3

"""
Some recent update some months ago apparently broke my endsong.json file
recording that I received in a Spotify data request (in order to import them on
stats.fm), leading to almost all fields being nulled out. However, I noticed
that the StreamingHistory.json files did contain these streams, but without all
information.

Given a directory with all these files, this script will:
 - Create a sqlite3 database at data.db

 - Read all endsong_*.json into the table "endsong" and all
    StreamingHistory*.json into the table "history"

 - Assign all plays in both kinds of files a unique ID for identification

 - For all broken entries endsong, find an entry in history with the same end
    time and play duration. If found, copy the entry from endsong to
    "endsong_modified", and add the song and artist names as extracted from the
    history entry.
    - Note that endsong is second-accurate but history is only minute-accurate.
    - In the case of multiple matches, match them by sorting sort by the
        "offline timestamp" in endsong and by sequential ID in history (so we
        assume the StreamingHistory*.json fiels are chronological, which they
        appear to be).
    - In the case of no matches, the song was listened to for the first time
        after endsong.json broke. We now search history for entries with the
        same number of miliseconds played.
            - For full plays, this usually identifies the song (when amboguous,
                the user is prompted to choose).
            - For partial plays, it sadly is not possible to find the song

 - Track data (album name and spotify URI) are added in a few ways:
    - Search for the song and artist in endsong. If a match is exact, copy.
    - For songs that weren't found, use Selenium to scrape the Spotify search.
        - If the title and artist of the first result match, use the album name
            and Spotify URI from that entry.
        - If these don't match, the user is prompted for action:
            - The user can signal no song matches
            - The user can signal another search result index to use other
                than the first one.
                - During this time the user may also navigate to a different
                    page or modify the search query to find the correct song

 - Finally all modified data is exported as a .json that can be imported.
"""

import sqlite3
import argparse
import json
import itertools
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from tqdm import tqdm
from pathlib import Path

parser = argparse.ArgumentParser(description="Merge StreamingHistory.json and endsong.json files into endsong-style files")
parser.add_argument("indir", help="input directory, output is in a subdirectory")

args = parser.parse_args()

indir = Path(args.indir).resolve()
outdir = indir / "fixed"
outdir.mkdir(parents=True, exist_ok=True)

db_path = indir / "data.db"
db_exists =  db_path.exists()
db_source = sqlite3.connect(db_path)
db = sqlite3.connect(":memory:")
if db_exists:
    db_source.backup(db)
else:
    # Create database
    with db:
        db.execute("""
        CREATE TABLE history(
            endTime TEXT,
            artistName TEXT,
            trackName TEXT,
            msPlayed INTEGER,
            source TEXT,
            id INTEGER PRIMARY KEY
        )
        """)

        db.execute("""
        CREATE TABLE endsong(
            ts TEXT,
            username TEXT,
            platform TEXT,
            ms_played INTEGER,
            conn_country TEXT,
            ip_addr_decrypted TEXT,
            user_agent_decrypted TEXT,
            master_metadata_track_name TEXT,
            master_metadata_album_artist_name TEXT,
            master_metadata_album_album_name TEXT,
            spotify_track_uri TEXT,
            episode_name TEXT,
            episode_show_name TEXT,
            spotify_episode_uri TEXT,
            reason_start TEXT,
            reason_end TEXT,
            shuffle INTEGER,
            skipped INTEGER,
            offline INTEGER,
            offline_timestamp INTEGER,
            incognito_mode INTEGER,
            source TEXT,
            id INTEGER PRIMARY KEY
        )
        """)

    i = 0
    for hist in itertools.chain(indir.glob("StreamingHistory*.json"), indir.glob("endsong_*.json")):
        print(f"Reading {hist.relative_to(indir)}")

        table = "history" if hist.name.startswith("StreamingHistory") else "endsong"

        with db:
            obj: dict
            for obj in json.load(hist.open("r", encoding="utf-8")):
                obj["source"] = hist.name
                obj["id"] = i
                i += 1
                keys_str = ",".join([ f":{name}" for name in obj.keys() ])
                db.execute(f"INSERT INTO {table} VALUES ({keys_str})", obj)

    with db:
        db.execute("CREATE TABLE endsong_modified AS SELECT * FROM endsong WHERE 1 = 2")

i = 0

with db:
    # Entries with multiple or no matches, to be re-inspected
    multiple: set[tuple[int]] = set()
    none: set[int] = set()
    # All entries with issues
    row: tuple[str]
    print("Initial pass")
    count = db.execute("SELECT COUNT(*) FROM endsong WHERE platform = 'ios'").fetchone()[0]
    for row in tqdm(db.execute("SELECT * FROM endsong WHERE platform = 'ios'"), total=count):
        ts: str = row[0][:16].replace("T", " ", 1)
        matches = db.execute("SELECT * FROM history WHERE endTime = ? AND msPlayed = ?", (ts, row[3])).fetchall()
        if len(matches) == 1:
            # Check if row was already updated
            count = db.execute("SELECT COUNT(id) FROM endsong_modified WHERE id = ?", (row[-1],)).fetchone()[0]
            if count == 0:
                db.execute("INSERT INTO endsong_modified SELECT * FROM endsong WHERE id = ?", (row[-1],))
                db.execute("UPDATE endsong_modified SET master_metadata_track_name = ?, master_metadata_album_artist_name = ? WHERE id = ?", (matches[0][2], matches[0][1], row[-1]))
                i += 1

        elif len(matches) > 1:
            match_ids = [
                v[-1] for v in matches
            ]
            match_ids.sort()

            multiple.add(tuple(match_ids))
        else:
            none.add(row[-1])

    print("Fix entries with multiple matches")
    for entry in tqdm(multiple):
        entry_str = ','.join(map(str, entry))
        history = db.execute(f"SELECT * FROM history WHERE id IN ({entry_str})").fetchall()

        ts_like = history[0][0].replace(" ", "T") + "%"
        endsong = db.execute(f"SELECT id FROM endsong WHERE platform = 'ios' AND ms_played = {history[0][3]} AND ts LIKE '{ts_like}' ORDER BY offline_timestamp").fetchall()

        for hist_ent, endsong_id in zip(history, endsong):
            endsong_id = endsong_id[0]

            # Copy row and update values
            count = db.execute("SELECT COUNT(id) FROM endsong_modified WHERE id = ?", (endsong_id,)).fetchone()[0]
            if count == 0:
                db.execute("INSERT INTO endsong_modified SELECT * FROM endsong WHERE id = ?", (endsong_id,))
                db.execute("UPDATE endsong_modified SET master_metadata_track_name = ?, master_metadata_album_artist_name = ? WHERE id = ?", (hist_ent[2], hist_ent[1], endsong_id))
                i += 1

    missing = set()
    unsure = set()
    print("Fix entries with no matches")
    for entry in tqdm(none):
        count = db.execute("SELECT COUNT(id) FROM endsong_modified WHERE id = ?", (entry,)).fetchone()[0]
        if count == 0:
            ts, ms_played = db.execute("SELECT ts, ms_played FROM endsong WHERE id = ?", (entry,)).fetchone()
            matches = db.execute("SELECT artistName, trackName FROM history WHERE msPlayed = ?", (ms_played,)).fetchall()
            if len(matches) > 0:
                if len(set(matches)) > 1:
                    options = list(set(matches))
                    print(f"Unsure for entry at {ts}:")
                    for j in range(len(options)):
                        print(f"[{j}] {options[j][0]} - {options[j][1]}")

                    x = input("Index or u/U for unsure: ")

                    if x == "U" or x == "u":
                        unsure.add(entry)
                        continue

                    matches[0] = options[int(x)]

                db.execute("INSERT INTO endsong_modified SELECT * FROM endsong WHERE id = ?", (entry,))
                db.execute("UPDATE endsong_modified SET master_metadata_track_name = ?, master_metadata_album_artist_name = ? WHERE id = ?", (matches[0][1], matches[0][0], entry))
                i += 1
            else:
                missing.add(entry)

    no_extra: dict[tuple[str, str], int] = dict()
    print("Add track data if applicable")
    count = db.execute("SELECT COUNT(*) FROM endsong_modified WHERE master_metadata_album_album_name IS NULL OR spotify_track_uri IS NULL").fetchone()[0]
    for track_name, artist_name, idnum in tqdm(db.execute("SELECT master_metadata_track_name, master_metadata_album_artist_name, id FROM endsong_modified WHERE master_metadata_album_album_name IS NULL OR spotify_track_uri IS NULL"), total=count):
        data = db.execute("SELECT DISTINCT master_metadata_album_album_name, spotify_track_uri FROM endsong WHERE master_metadata_track_name = ? AND master_metadata_album_artist_name = ? ORDER BY ts DESC LIMIT 1", (track_name, artist_name)).fetchall()
        if len(data) == 0:
            no_extra[(track_name, artist_name)] = idnum
        else:
            db.execute("UPDATE endsong_modified SET master_metadata_album_album_name = ?, spotify_track_uri = ? WHERE id = ?", (data[0][0], data[0][1], idnum))

    del no_extra[("Unknown Track", "Unknown Artist")]
    skipped: dict[tuple[str, str], int] = dict()
    if len(no_extra) > 0:
        print(f"Manual entry of final {len(no_extra)} entries")
        j = 1
        driver = webdriver.Chrome()
        result_xpath = "//*[@id=\"searchPage\"]/div/div/div/div[1]/div[2]/div[2]/div[1]/div"
        for k, v in no_extra.items():
            print(f"{j} / {len(no_extra)} remaining: {k[0]} - {k[1]}")
            driver.get(f"https://open.spotify.com/search/{k[0]} - {k[1]}/tracks")
            
            try:
                element: WebElement = WebDriverWait(driver, 10).until(expected_conditions.presence_of_element_located((By.XPATH, result_xpath)))
            except:
                j += 1
                continue
            
            found_name = element.find_element(By.CSS_SELECTOR, "div:nth-child(2) > div > div").text
            found_artist = element.find_element(By.CSS_SELECTOR, "div:nth-child(2) > div > span").text
            found_album = element.find_element(By.CSS_SELECTOR, "div:nth-child(3) > span > a").text

            matches = found_name == k[0] and found_artist == k[1]

            if not matches:
                print(f"Expected: {k[1]} - {k[0]}")
                print(f"Got:      {found_artist} - {found_name}")
                res = input("Correct? (y/n/index) ")

                if res == "y" or res == "Y":
                    matches = True
                elif res != "n" and res != "N":
                    index = int(res)
                    element = driver.find_element(By.XPATH, f"//*[@id=\"searchPage\"]/div/div/div/div[1]/div[2]/div[2]/div[{index}]/div")
                    found_name = element.find_element(By.CSS_SELECTOR, "div:nth-child(2) > div > div").text
                    found_artist = element.find_element(By.CSS_SELECTOR, "div:nth-child(2) > div > span").text
                    found_album = element.find_element(By.CSS_SELECTOR, "div:nth-child(3) > span > a").text
                    matches = True

            if matches:
                button = element.find_element(By.CSS_SELECTOR, "div:nth-child(4) > button:nth-child(3)")
                button.click()
                ctx: WebElement = WebDriverWait(driver, 10).until(expected_conditions.presence_of_element_located((By.ID, "context-menu")))
                #first_el = ctx.find_element(By.CSS_SELECTOR, "li:first-child > button")
                share_el = ctx.find_element(By.CSS_SELECTOR, "li:nth-child(7) > button")
                webdriver.ActionChains(driver).move_to_element(share_el).perform()
                copy_link: WebElement = WebDriverWait(driver, 10).until(expected_conditions.presence_of_element_located((By.CSS_SELECTOR, "#context-menu li:nth-child(7) > div > ul > :first-child > button")))
                copy_link.click()

                search_bar = driver.find_element(By.XPATH, "//*[@id=\"main\"]/div/div[2]/div[1]/header/div[3]/div/div/form/input")
                webdriver.ActionChains(driver).move_to_element(search_bar).click().key_down(Keys.CONTROL).send_keys("av").key_up(Keys.CONTROL).perform()
                
                spotify_uri = search_bar.get_attribute("value").split("?")[0][31:]

                db.execute("UPDATE endsong_modified SET master_metadata_album_album_name = ?, spotify_track_uri = ? WHERE master_metadata_track_name = ? AND master_metadata_album_artist_name = ?", (found_album, spotify_uri, k[0], k[1]))
            else:
                skipped[v] = v

            j += 1
            
        driver.quit()

    # I messed up formatting twice :)
    print("Fixing up Spotify URIs")
    count = db.execute("SELECT COUNT(id) FROM endsong_modified WHERE spotify_track_uri LIKE 'https%'").fetchone()[0]
    for idnum, uri in tqdm(db.execute("SELECT id, spotify_track_uri FROM endsong_modified WHERE spotify_track_uri LIKE 'https%'"), total=count):
        fixed = "spotify:track:" + uri.split("?")[0][31:]
        db.execute("UPDATE endsong_modified SET spotify_track_uri = ? WHERE id = ?", (fixed, idnum))

    print("Fixing up Spotify URIs (2)")
    count = db.execute("SELECT COUNT(id) FROM endsong_modified WHERE spotify_track_uri NOT LIKE 'spotify%'").fetchone()[0]
    for idnum, uri in tqdm(db.execute("SELECT id, spotify_track_uri FROM endsong_modified WHERE spotify_track_uri NOT LIKE 'spotify%'"), total=count):
        fixed = f"spotify:track:{uri}"
        db.execute("UPDATE endsong_modified SET spotify_track_uri = ? WHERE id = ?", (fixed, idnum))

    print("Deleting \"Unknown Track\" entries")
    db.execute("DELETE FROM endsong_modified WHERE master_metadata_track_name = 'Unknown Track' AND master_metadata_album_artist_name = 'Unknown Artist'")

    print("Saving modified data")
    db.row_factory = sqlite3.Row
    result = []
    for row in db.execute("SELECT * FROM endsong_modified"):
        row_dict = dict(row)
        del row_dict["source"]
        del row_dict["id"]
        result.append(row_dict)

    outfile = outdir / "endsong_00.json"

    with outfile.open("w") as f:
        json.dump(result, f)

    print(f"Updated: {i}")

    if len(missing) > 0:
        print("Missing:")
        print(json.dumps(list(missing), indent=2))

    if len(unsure) > 0:
        print("Unsure:")
        print(json.dumps(list(unsure), indent=2))

    if len(skipped) > 0:
        print("No extra data:")
        print(json.dumps(list(skipped), indent=2, ensure_ascii=False))

db.backup(db_source)

db_source.close()
db.close()
