# db.py - Improved version with security, error handling, and context managers
import sqlite3
import os
import json
import pandas as pd
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager
from werkzeug.security import generate_password_hash, check_password_hash

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "data/users.db"

# ============ CONTEXT MANAGER FOR DATABASE ============
@contextmanager
def get_db_connection():
    """Context manager for database connections with automatic cleanup."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    except Exception as e:
        logger.error(f"Database error: {e}", exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()

# ============ INITIALIZATION ============
def init_db():
    """Initialize database tables if they don't exist."""
    try:
        os.makedirs("data", exist_ok=True)
        with get_db_connection() as conn:
            c = conn.cursor()
            
            # Tabla usuarios
            c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                edad INTEGER,
                deporte TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME
            )"""
            )

            # Tabla cuestionarios (respuestas en JSON)
            c.execute("""
            CREATE TABLE IF NOT EXISTS questionnaires (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                questionnaire_id TEXT NOT NULL,
                responses TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )"""
            )

            # Tabla sensores (BPM, HRV)
            c.execute("""
            CREATE TABLE IF NOT EXISTS sensors_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                source_filename TEXT,
                bpm REAL,
                hrv REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )"""
            )
            
            conn.commit()
            logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        raise

# ============ PASSWORD MANAGEMENT ============
def hash_password(password):
    """Hash a password using werkzeug security."""
    if not password or len(password) < 6:
        raise ValueError("Password must be at least 6 characters long")
    return generate_password_hash(password)

def verify_password(password, hashed):
    """Verify a password against its hash."""
    try:
        return check_password_hash(hashed, password)
    except Exception as e:
        logger.error(f"Error verifying password: {e}")
        return False

# ============ USERS ============
def register_user(username, password, edad=None, deporte=None):
    """Register a new user with hashed password."""
    try:
        # Validate inputs
        if not username or len(username) < 3:
            logger.warning(f"Invalid username attempt: {username}")
            return False
        
        hashed_pwd = hash_password(password)
        
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO users (username, password, edad, deporte) 
                VALUES (?, ?, ?, ?)
            """, (username, hashed_pwd, edad, deporte))
            conn.commit()
            logger.info(f"User registered: {username}")
            return True
            
    except sqlite3.IntegrityError:
        logger.warning(f"User already exists: {username}")
        return False
    except Exception as e:
        logger.error(f"Error registering user {username}: {e}", exc_info=True)
        return False

def authenticate_user(username, password):
    """Authenticate user and return user_id if successful."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT id, password FROM users WHERE username=?
            """, (username,))
            row = c.fetchone()
            
            if row and verify_password(password, row['password']):
                user_id = row['id']
                # Update last_login
                c.execute("UPDATE users SET last_login=? WHERE id=?", 
                         (datetime.now(), user_id))
                conn.commit()
                logger.info(f"User authenticated: {username}")
                return user_id
            
            logger.warning(f"Failed authentication attempt for: {username}")
            return None
            
    except Exception as e:
        logger.error(f"Error authenticating user {username}: {e}", exc_info=True)
        return None

def get_user_by_id(user_id):
    """Get user information by ID."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT id, username, edad, deporte, created_at, last_login 
                FROM users WHERE id=?
            """, (user_id,))
            row = c.fetchone()
            return dict(row) if row else None
            
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}", exc_info=True)
        return None

# ============ QUESTIONNAIRES - VALIDATION ============
def validate_questionnaire_response(questionnaire_id, responses, questionnaire_defs):
    """Validate questionnaire responses against defined fields."""
    try:
        if questionnaire_id not in questionnaire_defs:
            return False, f"Unknown questionnaire: {questionnaire_id}"
        
        q_def = questionnaire_defs[questionnaire_id]
        
        for field in q_def.get("fields", []):
            key = field["key"]
            value = responses.get(key)
            
            # Check if required field is present
            if value is None:
                return False, f"Missing required field: {key}"
            
            # Validate field type and range
            try:
                val_num = float(value)
                min_val = float(field.get("min", float('-inf')))
                max_val = float(field.get("max", float('inf')))
                
                if not (min_val <= val_num <= max_val):
                    return False, f"Field '{key}' out of range [{min_val}, {max_val}]"
            except (ValueError, TypeError):
                return False, f"Field '{key}' must be numeric"
        
        return True, "Valid"
        
    except Exception as e:
        logger.error(f"Error validating questionnaire {questionnaire_id}: {e}")
        return False, "Validation error"

def save_questionnaire(user_id, questionnaire_id, responses):
    """Save questionnaire responses to database."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO questionnaires (user_id, questionnaire_id, responses) 
                VALUES (?, ?, ?)
            """, (user_id, questionnaire_id, json.dumps(responses)))
            conn.commit()
            logger.info(f"Questionnaire saved for user {user_id}: {questionnaire_id}")
            return True
            
    except Exception as e:
        logger.error(f"Error saving questionnaire for user {user_id}: {e}", exc_info=True)
        return False

def get_questionnaire_history(user_id, questionnaire_id=None, days=None):
    """Get questionnaire history with optional filtering."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            
            if questionnaire_id and days:
                since = datetime.now() - timedelta(days=days)
                c.execute("""
                    SELECT id, questionnaire_id, responses, timestamp 
                    FROM questionnaires 
                    WHERE user_id=? AND questionnaire_id=? AND timestamp>=? 
                    ORDER BY timestamp
                """, (user_id, questionnaire_id, since))
                
            elif questionnaire_id:
                c.execute("""
                    SELECT id, questionnaire_id, responses, timestamp 
                    FROM questionnaires 
                    WHERE user_id=? AND questionnaire_id=? 
                    ORDER BY timestamp
                """, (user_id, questionnaire_id))
                
            else:
                c.execute("""
                    SELECT id, questionnaire_id, responses, timestamp 
                    FROM questionnaires 
                    WHERE user_id=? 
                    ORDER BY timestamp
                """, (user_id,))
            
            rows = c.fetchall()
            result = []
            for r in rows:
                result.append({
                    "id": r['id'],
                    "questionnaire_id": r['questionnaire_id'],
                    "responses": json.loads(r['responses']),
                    "timestamp": r['timestamp']
                })
            return result
            
    except Exception as e:
        logger.error(f"Error getting questionnaire history for user {user_id}: {e}", exc_info=True)
        return []

# ============ SENSORS ============
def save_sensor_data(user_id, source_filename, bpm, hrv):
    """Save sensor data to database."""
    try:
        # Validate sensor values
        if bpm is not None:
            bpm = float(bpm)
            if not (30 <= bpm <= 200):
                logger.warning(f"BPM out of normal range: {bpm}")
        
        if hrv is not None:
            hrv = float(hrv)
            if hrv < 0:
                logger.warning(f"HRV negative: {hrv}")
        
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO sensors_data (user_id, source_filename, bpm, hrv) 
                VALUES (?, ?, ?, ?)
            """, (user_id, source_filename, bpm, hrv))
            conn.commit()
            logger.info(f"Sensor data saved for user {user_id}: BPM={bpm}, HRV={hrv}")
            return True
            
    except Exception as e:
        logger.error(f"Error saving sensor data for user {user_id}: {e}", exc_info=True)
        return False

def get_sensor_history(user_id, days=None):
    """Get sensor history data."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            
            if days:
                since = datetime.now() - timedelta(days=days)
                c.execute("""
                    SELECT id, source_filename, bpm, hrv, timestamp 
                    FROM sensors_data 
                    WHERE user_id=? AND timestamp>=? 
                    ORDER BY timestamp
                """, (user_id, since))
            else:
                c.execute("""
                    SELECT id, source_filename, bpm, hrv, timestamp 
                    FROM sensors_data 
                    WHERE user_id=? 
                    ORDER BY timestamp
                """, (user_id,))
            
            rows = c.fetchall()
            result = []
            for r in rows:
                result.append({
                    "id": r['id'],
                    "source_filename": r['source_filename'],
                    "bpm": r['bpm'],
                    "hrv": r['hrv'],
                    "timestamp": r['timestamp']
                })
            return result
            
    except Exception as e:
        logger.error(f"Error getting sensor history for user {user_id}: {e}", exc_info=True)
        return []

# ============ TRAINING LOAD & ACWR ============
def compute_session_load_from_responses(responses):
    """Compute training load (RPE × duration)."""
    try:
        rpe = responses.get("rpe")
        duracion = responses.get("duracion")
        
        if rpe is None or duracion is None:
            return None
        
        rpe_v = float(rpe)
        dur_v = float(duracion)
        
        # Validate ranges
        if not (0 <= rpe_v <= 10):
            logger.warning(f"RPE out of range: {rpe_v}")
            return None
        if dur_v < 0:
            logger.warning(f"Duration negative: {dur_v}")
            return None
        
        return rpe_v * dur_v
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error computing session load: {e}")
        return None

def get_training_load_history(user_id, days=None):
    """Get training load history."""
    try:
        history = get_questionnaire_history(user_id, questionnaire_id="general", days=days)
        loads = []
        
        for h in history:
            load = compute_session_load_from_responses(h["responses"])
            loads.append({"timestamp": h["timestamp"], "load": load})
        
        return loads
        
    except Exception as e:
        logger.error(f"Error getting training load history for user {user_id}: {e}")
        return []

def compute_acwr(user_id, acute_days=7, chronic_days=28):
    """
    Compute Acute:Chronic Workload Ratio (ACWR).
    ACWR = mean(acute window) / mean(chronic window)
    """
    try:
        acute = get_training_load_history(user_id, days=acute_days)
        chronic = get_training_load_history(user_id, days=chronic_days)

        acute_vals = [d["load"] for d in acute if d["load"] is not None]
        chronic_vals = [d["load"] for d in chronic if d["load"] is not None]

        if not acute_vals:
            logger.info(f"No acute data for user {user_id}")
            return None
        
        acute_mean = sum(acute_vals) / len(acute_vals)

        if not chronic_vals:
            logger.info(f"No chronic data for user {user_id}")
            return None
        
        chronic_mean = sum(chronic_vals) / len(chronic_vals)

        if chronic_mean == 0:
            logger.warning(f"Zero chronic mean for user {user_id}")
            return None
        
        acwr = acute_mean / chronic_mean
        
        logger.info(f"ACWR computed for user {user_id}: {acwr:.2f}")
        return {
            "acute_mean": acute_mean,
            "chronic_mean": chronic_mean,
            "acwr": acwr
        }
        
    except Exception as e:
        logger.error(f"Error computing ACWR for user {user_id}: {e}")
        return None

# ============ EXPORT ============
def export_user_data_csv(user_id, output_filepath=None):
    """Export user data (questionnaires + sensors) to CSV."""
    try:
        if output_filepath is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filepath = f"data/export_user_{user_id}_{ts}.csv"

        q = get_questionnaire_history(user_id)
        s = get_sensor_history(user_id)

        records = []
        
        # Add questionnaire records
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

        # Add sensor records
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

        # Create DataFrame and export
        if len(records) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame.from_records(records)
        
        df.to_csv(output_filepath, index=False)
        logger.info(f"Data exported for user {user_id} to {output_filepath}")
        return output_filepath
        
    except Exception as e:
        logger.error(f"Error exporting data for user {user_id}: {e}", exc_info=True)
        return None