# db.py
import sqlite3
import os
import json
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "data/users.db"

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Tabla usuarios
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        edad INTEGER,
        deporte TEXT
    )""")

    # Tabla cuestionarios (respuestas en JSON)
    c.execute("""
    CREATE TABLE IF NOT EXISTS questionnaires (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        questionnaire_id TEXT,
        responses TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    # Tabla sensores (BPM, HRV)
    c.execute("""
    CREATE TABLE IF NOT EXISTS sensors_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        source_filename TEXT,
        bpm REAL,
        hrv REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    conn.commit()
    conn.close()

# ---------------- Users ----------------
def register_user(username, password, edad=None, deporte=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password, edad, deporte) VALUES (?, ?, ?, ?)",
                  (username, password, edad, deporte))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def authenticate_user(username, password):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE username=? AND password=?", (username, password))
    row = c.fetchone()
    conn.close()
    return int(row[0]) if row else None

def get_user_by_id(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, username, edad, deporte FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row

# ---------------- Questionnaires ----------------
def save_questionnaire(user_id, questionnaire_id, responses):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO questionnaires (user_id, questionnaire_id, responses) VALUES (?, ?, ?)",
        (user_id, questionnaire_id, json.dumps(responses))
    )
    conn.commit()
    conn.close()

def get_questionnaire_history(user_id, questionnaire_id=None, days=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if questionnaire_id and days:
        since = datetime.now() - timedelta(days=days)
        c.execute(
            "SELECT id, questionnaire_id, responses, timestamp FROM questionnaires WHERE user_id=? AND questionnaire_id=? AND timestamp>=? ORDER BY timestamp",
            (user_id, questionnaire_id, since)
        )
    elif questionnaire_id:
        c.execute(
            "SELECT id, questionnaire_id, responses, timestamp FROM questionnaires WHERE user_id=? AND questionnaire_id=? ORDER BY timestamp",
            (user_id, questionnaire_id)
        )
    else:
        c.execute(
            "SELECT id, questionnaire_id, responses, timestamp FROM questionnaires WHERE user_id=? ORDER BY timestamp",
            (user_id,)
        )
    rows = c.fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id": r[0],
            "questionnaire_id": r[1],
            "responses": json.loads(r[2]),
            "timestamp": r[3]
        })
    return result

# ---------------- Sensors ----------------
def save_sensor_data(user_id, source_filename, bpm, hrv):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO sensors_data (user_id, source_filename, bpm, hrv) VALUES (?, ?, ?, ?)",
        (user_id, source_filename, bpm, hrv)
    )
    conn.commit()
    conn.close()

def get_sensor_history(user_id, days=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if days:
        since = datetime.now() - timedelta(days=days)
        c.execute("SELECT id, source_filename, bpm, hrv, timestamp FROM sensors_data WHERE user_id=? AND timestamp>=? ORDER BY timestamp",
                  (user_id, since))
    else:
        c.execute("SELECT id, source_filename, bpm, hrv, timestamp FROM sensors_data WHERE user_id=? ORDER BY timestamp", (user_id,))
    rows = c.fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id": r[0],
            "source_filename": r[1],
            "bpm": r[2],
            "hrv": r[3],
            "timestamp": r[4]
        })
    return result

# ---------------- Training load & ACWR ----------------
def compute_session_load_from_responses(responses):
    """
    inputs: responses dict (must include keys 'rpe' and 'duracion' ideally)
    returns: load (numeric) or None
    """
    try:
        rpe = responses.get("rpe")
        duracion = responses.get("duracion")
        if rpe is None or duracion is None:
            return None
        # ensure numeric
        rpe_v = float(rpe)
        dur_v = float(duracion)
        return rpe_v * dur_v
    except Exception:
        return None

def get_training_load_history(user_id, days=None):
    """
    Returns list of dicts {timestamp, load} for 'general' questionnaire entries
    """
    history = get_questionnaire_history(user_id, questionnaire_id="general", days=days)
    loads = []
    for h in history:
        load = compute_session_load_from_responses(h["responses"])
        loads.append({"timestamp": h["timestamp"], "load": load})
    return loads

def compute_acwr(user_id, acute_days=7, chronic_days=28):
    """
    ACWR = mean(acute window) / mean(chronic window)
    If chronic mean is zero or insufficient data -> return None
    """
    acute = get_training_load_history(user_id, days=acute_days)
    chronic = get_training_load_history(user_id, days=chronic_days)

    acute_vals = [d["load"] for d in acute if d["load"] is not None]
    chronic_vals = [d["load"] for d in chronic if d["load"] is not None]

    if not acute_vals:
        return None
    acute_mean = sum(acute_vals) / len(acute_vals)

    if not chronic_vals:
        return None
    chronic_mean = sum(chronic_vals) / len(chronic_vals)

    try:
        acwr = acute_mean / chronic_mean if chronic_mean != 0 else None
    except Exception:
        acwr = None
    return {"acute_mean": acute_mean, "chronic_mean": chronic_mean, "acwr": acwr}

# ---------------- Export CSV ----------------
def export_user_data_csv(user_id, output_filepath=None):
    if output_filepath is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filepath = f"data/export_user_{user_id}_{ts}.csv"

    # Collect questionnaires and sensors
    q = get_questionnaire_history(user_id)
    s = get_sensor_history(user_id)

    records = []
    for item in q:
        base = {
            "type": "questionnaire",
            "record_id": item["id"],
            "questionnaire_id": item["questionnaire_id"],
            "timestamp": item["timestamp"]
        }
        for k, v in item["responses"].items():
            base[k] = v
        records.append(base)

    for item in s:
        base = {
            "type": "sensor",
            "record_id": item["id"],
            "source_filename": item["source_filename"],
            "bpm": item["bpm"],
            "hrv": item["hrv"],
            "timestamp": item["timestamp"]
        }
        records.append(base)

    if len(records) == 0:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame.from_records(records)
    df.to_csv(output_filepath, index=False)
    return output_filepath
