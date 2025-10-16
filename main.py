print("Importing libraries...")

import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import time
from datetime import datetime
import mysql.connector
import os
import numpy as np
from bs4 import BeautifulSoup
from datetime import date
import pymysql
from ftplib import FTP
import io
import json

week = 7  # change this to fetch a different week
year = 2025  # change this to fetch a different week

print("Scraping Data for week: ", week)


print("starting NFL Fantasy scrape...")

BASE_URL = "https://fantasy.nfl.com/research/projections"

PARAMS = {
    "position": "O",
    "sort": "projectedPts",
    "statCategory": "projectedStats",
    "statSeason": year,
    "statType": "weekProjectedStats",
    "statWeek": week,
    "offset": 0,   # pagination starts here
}

def fetch_page(offset):
    """Fetch one page of projections."""
    params = PARAMS.copy()
    params["offset"] = offset
    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    return r.text

def parse_table(html):
    """Extract headers + rows from one table, matching number of <td> columns."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return [], []

    # Use only the LAST header row, since that's aligned with <td>
    header_rows = table.find("thead").find_all("tr")
    headers = [th.get_text(strip=True) for th in header_rows[-1].find_all("th")]

    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    return headers, rows

def scrape_all():
    all_rows = []
    headers = []
    offset = 0

    while True:
        html = fetch_page(offset)
        h, rows = parse_table(html)
        if not rows:
            break

        if not headers and h:
            headers = h

        all_rows.extend(rows)
        offset += 25  # next page

    nfl_df = pd.DataFrame(all_rows, columns=headers)
    return nfl_df

# Run it
nfl_df = scrape_all()

print("cleaning NFL Fantasy data...")

 # Extract data using pattern recognition
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


#add keys to the df

#add timestamp and convert to datetime
nfl_df['Timestamp'] = current_time
nfl_df['Timestamp'] = pd.to_datetime(nfl_df["Timestamp"], errors="coerce")

#strip other stuff from player name
nfl_df["PlayerName"] = nfl_df["Player"].str.replace(r"(QB|RB|WR|TE).*", "", regex=True).str.strip()

#create PlayerDateKey
nfl_df['PlayerDateKey'] = nfl_df['PlayerName'].str.replace(' ', '', regex=True) + nfl_df['Timestamp'].dt.strftime('%Y%m%d')

#construct final dataframe
nfl_df = nfl_df[["PlayerName","Opp", "Points", "Timestamp", "PlayerDateKey"]]

#delete duplicates
nfl_df = nfl_df.drop_duplicates(subset=['PlayerDateKey'])



db = mysql.connector.connect(
    host= "giowm1136.siteground.biz",
    user = "ur2n2kkxdd6uc",
    password = "g71__m1c3<m5",
    database = "dbhutvbrbdxm0c"   
)

cursor = db.cursor()

print("creating database connection...")

#create table if it doesnt exists
create_table_query = """
CREATE TABLE IF NOT EXISTS nflfantasy_weekly_projections (
    PlayerName VARCHAR(255),  
    Opp VARCHAR(255),
    Points FLOAT,
    Timestamp DATETIME,
    PlayerDateKey VARCHAR(255) PRIMARY KEY
);
"""
cursor.execute(create_table_query)

# Insert data into MySQL
query = """
INSERT IGNORE INTO nflfantasy_weekly_projections (PlayerName, Opp, Points, Timestamp, PlayerDateKey)
VALUES (%s, %s, %s, %s, %s);
"""

# Build list of tuples in correct order
data = [
    (
        row['PlayerName'],
        row['Opp'],
        row['Points'],
        row['Timestamp'],
        row['PlayerDateKey']
    )
    for _, row in nfl_df.iterrows()
]

# Bulk insert
if data:
    cursor.executemany(query, data)
    db.commit()
    print(f"Inserted {cursor.rowcount} rows into nflfantasy_weekly_projections.")



cursor.close()
db.close()

print("NFL Fantasy data successfully written to MySQL with timestamp!")

#FANTASY PROS SCRAPE

print("Beginning Fantasy Pros scrape...")

# ---------------- CONFIG ----------------
POSITIONS = ["qb", "rb", "wr", "te"]
BASE_URL = "https://www.fantasypros.com/nfl/projections/{pos}.php?scoring=PPR"


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

# ---------------- COLUMN MAPPINGS ----------------
POSITION_COL_MAP = {
    "QB": [
        "Player", "Team", "Passing_ATT", "Passing_CMP", "Passing_YDS", "Passing_TDS", "Passing_INT",
        "Rushing_ATT", "Rushing_YDS", "Rushing_TDS", "Fumbles", "FPTS"
    ],
    "RB": [
        "Player", "Team", "Rushing_ATT", "Rushing_YDS", "Rushing_TDS",
        "Receiving_REC", "Receiving_YDS", "Receiving_TDS", "Fumbles", "FPTS"
    ],
    "WR": [
        "Player", "Team", "Receiving_REC", "Receiving_YDS", "Receiving_TDS",
        "Rushing_ATT", "Rushing_YDS", "Rushing_TDS", "Fumbles", "FPTS"
    ],
    "TE": [
        "Player", "Team", "Receiving_REC", "Receiving_YDS", "Receiving_TDS",
        "Fumbles", "FPTS"
    ],
}

# ---------------- FUNCTIONS ----------------
def fetch_page(position):
    url = BASE_URL.format(pos=position)
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.text

def parse_table(html, position):
    """Parse table and rename columns safely."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        print(f"Warning: No table found for {position}.")
        return pd.DataFrame()
    
    # Use last header row
    header_rows = table.find("thead").find_all("tr")
    headers = [th.get_text(strip=True) for th in header_rows[-1].find_all("th")]

    # Extract rows
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        tds = tr.find_all("td")
        row = [td.get_text(strip=True) for td in tds]
        if row:
            # Pad or truncate row to match headers
            if len(row) < len(headers):
                row += [""] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[:len(headers)]
            rows.append(row)

    fp_df = pd.DataFrame(rows, columns=headers)

    # Map to clean column names
    col_map = POSITION_COL_MAP.get(position.upper())
    if col_map:
        # Pad/truncate col_map to match df.columns length
        if len(col_map) < len(fp_df.columns):
            col_map += [f"Extra_{i}" for i in range(len(fp_df.columns) - len(col_map))]
        elif len(col_map) > len(fp_df.columns):
            col_map = col_map[:len(fp_df.columns)]
        fp_df.columns = col_map
    else:
        # fallback: make headers unique
        fp_df.columns = [f"{c}_{position.upper()}" for c in fp_df.columns]

    # Add position column
    fp_df["Position"] = position.upper()
    return fp_df

# ---------------- SCRAPE ALL POSITIONS ----------------
all_data = []

for pos in POSITIONS:
    print(f"Scraping {pos.upper()} projections...")
    html = fetch_page(pos)
    fp_df = parse_table(html, pos)
    if not fp_df.empty:
        all_data.append(fp_df)

# Combine all positions
final_df = pd.concat(all_data, ignore_index=True, sort=False)

# File with today's date
today_str = date.today().isoformat()

# Add today's date column
final_df['Timestamp'] = pd.to_datetime(today_str, errors="coerce")

print("cleaning Fantasy Pros data...")

# Remove trailing 2-3 uppercase letters
final_df["Player_Clean"] = final_df["Player"].str.replace(r"[A-Z]{2,3}$", "", regex=True)

#create PlayerDateKey
final_df['PlayerDateKey'] = final_df['Player_Clean'].str.replace(' ', '', regex=True) + final_df['Timestamp'].dt.strftime('%Y%m%d')

#rename columns
final_df = final_df.rename(columns={"Fumbles": "Points", "Player_Clean": "PlayerName"})

#construct final dataframe
final_df = final_df[["PlayerName", "Points", "Timestamp", "PlayerDateKey"]]

#delete duplicates
final_df = final_df.drop_duplicates(subset=['PlayerDateKey'])


db = mysql.connector.connect(
    host= "giowm1136.siteground.biz",
    user = "ur2n2kkxdd6uc",
    password = "g71__m1c3<m5",
    database = "dbhutvbrbdxm0c"   
)

cursor = db.cursor()

print("creating database connection...")

#create table if it doesnt exists
create_table_query = """
CREATE TABLE IF NOT EXISTS fantasypros_weekly_projections (
    PlayerName VARCHAR(255),  
    Points FLOAT,
    Timestamp DATETIME,
    PlayerDateKey VARCHAR(255) PRIMARY KEY
);
"""
cursor.execute(create_table_query)

# Insert data into MySQL
for _, row in final_df.iterrows():
    query = """
    INSERT IGNORE INTO fantasypros_weekly_projections (PlayerName,  Points, Timestamp, PlayerDateKey)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(query, tuple(row))

print("inserting data into table...")



db.commit()
cursor.close()
db.close()

print("Fantasy Pros data successfully written to MySQL with timestamp!")


#SPORTSLINE
#SAVE_FOLDER = Path(r"C:\Users\lyle.morrison\OneDrive - Slalom\Desktop\dynasty\data\weekly\projections\espn")  # <-- change this

print("Starting Sportline scrape...")

SPORTSLINE_URL = "https://www.sportsline.com/nfl/expert-projections/simulation/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

response = requests.get(SPORTSLINE_URL, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

rows = soup.select("tbody tr")

data = []
for row in rows:
    cols = [c.get_text(strip=True) for c in row.select("td")]
    if not cols:
        continue
    player = {
        "player": cols[0],
        "pos": cols[1],
        "team": cols[2],
        "game": cols[3],
        "proj_fp": cols[4],
        "fd": cols[5],
        "exp": cols[6],
        "dk_exp": cols[7],
        "ppr": cols[8],
        "pass_yds": cols[9],
        "rush_yds": cols[10] if len(cols) > 10 else None,
        "rec_yds": cols[11] if len(cols) > 11 else None
    }
    data.append(player)



#convert to dataframe
sl_df = pd.DataFrame(data)

print("Cleaning Sportline data...")


#add timestamp and convert to datetime
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
sl_df ['Timestamp'] = current_time
sl_df ['Timestamp'] = pd.to_datetime(sl_df ["Timestamp"], errors="coerce")


#create PlayerDateKey
sl_df ['PlayerDateKey'] = sl_df ['player'].str.replace(' ', '', regex=True) + sl_df ['Timestamp'].dt.strftime('%Y%m%d')


#rename columns
sl_df  = sl_df .rename(columns={"dk_exp": "Points", "player": "PlayerName"})

#construct final dataframe
sl_df  = sl_df [["PlayerName", "Points", "Timestamp", "PlayerDateKey"]]

#delete duplicates
sl_df  = sl_df .drop_duplicates(subset=['PlayerDateKey'])


db = mysql.connector.connect(
    host= "giowm1136.siteground.biz",
    user = "ur2n2kkxdd6uc",
    password = "g71__m1c3<m5",
    database = "dbhutvbrbdxm0c"   
)

cursor = db.cursor()

print("creating database connection...")

#create table if it doesnt exists
create_table_query = """
CREATE TABLE IF NOT EXISTS sportsline_weekly_projections (
    PlayerName VARCHAR(255),  
    Points FLOAT,
    Timestamp DATETIME,
    PlayerDateKey VARCHAR(255) PRIMARY KEY
);
"""
cursor.execute(create_table_query)

# Insert data into MySQL
for _, row in sl_df.iterrows():
    query = """
    INSERT IGNORE INTO sportsline_weekly_projections (PlayerName,  Points, Timestamp, PlayerDateKey)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(query, tuple(row))

print("inserting data into table...")



db.commit()
cursor.close()
db.close()

print("Sportsline data successfully written to MySQL with timestamp!")



#SLEEPER

print("Starting Sleeper scrape...")

# Configurable parameters
positions_slp = ['RB', 'WR', 'QB', 'TE']  # positions to iterate over


all_data = []

for pos in positions_slp:
    print(f"Fetching {pos} projections for week {week}...")
    url = f"https://api.sleeper.com/projections/nfl/2025/{week}"
    params = {
        "season_type": "regular",
        "position": pos,
        "order_by": "pts_ppr"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if len(data) == 0:
            print(f"No data returned for {pos}.")
            continue
        
        # Flatten nested 'player' and 'stats'
        for item in data:
            flat_item = item.copy()
            
            if 'player' in flat_item and isinstance(flat_item['player'], dict):
                for k, v in flat_item['player'].items():
                    flat_item[f'player_{k}'] = v
                del flat_item['player']
            
            if 'stats' in flat_item and isinstance(flat_item['stats'], dict):
                for k, v in flat_item['stats'].items():
                    flat_item[f'stats_{k}'] = v
                del flat_item['stats']
            
            # Add position column for reference
            flat_item['position'] = pos
            
            all_data.append(flat_item)
    else:
        print(f"Failed to fetch data for {pos}: {response.status_code}")

# Convert all data to DataFrame
slp_df = pd.DataFrame(all_data)

print("Cleaning Sleeper data...")


#add timestamp and convert to datetime
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
slp_df['Timestamp'] = current_time
slp_df['Timestamp'] = pd.to_datetime(slp_df["Timestamp"], errors="coerce")


#select relevant columns
slp_df = slp_df[["week", "player_id", "opponent", "player_first_name", "player_last_name","stats_pts_ppr", "Timestamp"]]

#create PlayerDateKey
slp_df['PlayerDateKey'] = slp_df['player_first_name'] + slp_df['player_last_name'] + slp_df['Timestamp'].dt.strftime('%Y%m%d')

#create player name
slp_df['PlayerName'] = slp_df['player_first_name'] + " " + slp_df['player_last_name']


#rename columns
slp_df = slp_df.rename(columns={"stats_pts_ppr": "Points"})

#final df
slp_df = slp_df[["week", "player_id", "opponent", "PlayerName", "Points","PlayerDateKey", "Timestamp"]]

#delete duplicates
slp_df = slp_df.drop_duplicates(subset=['PlayerDateKey'])

# Replace all NaN/NaT with None so MySQL sees NULL
slp_df = slp_df.replace({np.nan: None})
slp_df = slp_df.where(pd.notnull(slp_df), None)


db = mysql.connector.connect(
    host= "giowm1136.siteground.biz",
    user = "ur2n2kkxdd6uc",
    password = "g71__m1c3<m5",
    database = "dbhutvbrbdxm0c"   
)

cursor = db.cursor()

print("creating database connection...")

#create table if it doesnt exists
create_table_query = """
CREATE TABLE IF NOT EXISTS sleeper_weekly_projections (
    week INT,  
    player_id VARCHAR(255),
    opponent VARCHAR(255),
    PlayerName VARCHAR(255),  
    Points FLOAT,
    PlayerDateKey VARCHAR(255) PRIMARY KEY,
    Timestamp DATETIME
);
"""
cursor.execute(create_table_query)

# Insert data into MySQL
for _, row in slp_df.iterrows():
    query = """
    INSERT IGNORE INTO sleeper_weekly_projections (week, player_id, opponent, PlayerName, Points, PlayerDateKey, Timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(query, tuple(row))

print("inserting data into table...")



db.commit()
cursor.close()
db.close()

print("Sleeper data successfully written to MySQL with timestamp!")
print("SCrape Workflow Complete! Fuck Yeah")
time.sleep(15)
print("Beginning FTP upload...")



#upload weekly data to FTP site


# -----------------------------
# MySQL configuration
# -----------------------------
mysql_config = {
    'host': 'giowm1136.siteground.biz',
    'user': 'ur2n2kkxdd6uc',
    'password': 'g71__m1c3<m5',
    'database': 'dbhutvbrbdxm0c',
    'port': 3306
}

view_name = 'vw_latest_weekly_projections'  # the MySQL view you want to fetch

# -----------------------------
# FTP configuration
# -----------------------------
ftp_config = {
    'host': 'giowm1136.siteground.biz',
    'user': 'rankingsftplyle@dynastypulse.com',
    'passwd': '12264n6$e^s)',
    'remote_path': 'dynastypulse.com/public_html/ffrankings/',  # path on FTP server
    'remote_file_name': 'testfile.json'  # filename to use on FTP
}

# -----------------------------
# Step 1: Fetch MySQL view data
# -----------------------------

print("Getting scraped data from MySQL...")

try:
    conn = pymysql.connect(**mysql_config)
    df = pd.read_sql(f"SELECT * FROM {view_name}", conn)
    conn.close()
    print(f"Fetched {len(df)} rows from view '{view_name}'.")
except Exception as e:
    print(f"❌ Error fetching data from MySQL: {e}")
    exit(1)

print("Cleaning data...")

# Step 2.1: Round selected numeric columns
columns_to_round = [
    "Average PPR Points/Week",
    "Consensus Proj.",
    "FantasyPros Proj Fan Pts",
    "NFL Fantasy Proj.",
    "Sportsline Proj.",
    "Sleeper Proj."
    
    
]
for col in columns_to_round:
    if col in df.columns:
        df[col] = df[col].round(1)
        
# Step 2.2: Drop unwanted column
column_to_drop = "PlayerNameKey"
if column_to_drop in df.columns:
    df = df.drop(columns=[column_to_drop])
    
# Step 2.3: Rename columns
df = df.rename(columns={
    "Overall Rank": "Weekly Overall"
})

# -----------------------------
# Step 2: Convert to pretty JSON
# -----------------------------

print("Converting to JSON...")
try:
    # Convert DataFrame to a list of dicts
    records = json.loads(df.to_json(orient='records'))
    
    # Pretty-print JSON with indentation
    pretty_json = json.dumps({"players": records}, indent=2, ensure_ascii=False)
except Exception as e:
    print(f"❌ Error converting to JSON: {e}")
    exit(1)

# -----------------------------
# Step 3: Upload JSON to FTP
# -----------------------------
print("Uploading to DynastyPulse FTP site...")
try:
    ftp = FTP(ftp_config['host'])
    ftp.login(ftp_config['user'], ftp_config['passwd'])
    ftp.cwd(ftp_config['remote_path'])

    # Convert JSON string to bytes (in-memory)
    bio = io.BytesIO(pretty_json.encode('utf-8'))
    ftp.storbinary(f"STOR {ftp_config['remote_file_name']}", bio)
    ftp.quit()

    print(f"✅ Successfully uploaded {ftp_config['remote_file_name']} to FTP!")
except Exception as e:
    print(f"❌ Error uploading to FTP: {e}")
    exit(1)

print("Workflow Complete")
