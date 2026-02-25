# sensors.py
import io
import base64
import pandas as pd
import numpy as np

# Intentamos importar funciones avanzadas. Si no están, usamos aproximaciones simples.
try:
    from scipy.signal import find_peaks, detrend
    SCIPY_AVAILABLE = True
except Exception:
    SCIPY_AVAILABLE = False

def parse_csv_contents(contents, filename):
    """
    Recibe contents base64 y filename (como dash dcc.Upload entrega).
    Devuelve pandas.DataFrame (si puede) o None.
    """
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        s = io.StringIO(decoded.decode('utf-8', errors='ignore'))
        df = pd.read_csv(s)
        return df
    except Exception:
        # intento con separador punto y coma
        try:
            s = io.StringIO(decoded.decode('utf-8', errors='ignore'))
            df = pd.read_csv(s, sep=';')
            return df
        except Exception:
            return None

def load_ecg_and_compute_bpm(df, signal_column_guess=None, fs=250):
    """
    Entrada: df pandas con columna de señal ECG (o columna única).
    Retorna: (bpm, hrv) como floats.
    Si scipy está disponible, hace un análisis simple:
      - detecta picos R y calcula inter-beat intervals -> BPM y SDNN (HRV)
    Parámetros:
      - signal_column_guess: nombre de columna si se quiere forzar
      - fs: frecuencia de muestreo (Hz), por defecto 250
    """
    if df is None or df.shape[0] < 10:
        return None, None

    # Seleccionar columna de señal
    if signal_column_guess and signal_column_guess in df.columns:
        signal = df[signal_column_guess].astype(float).values
    else:
        # coger la primera columna numérica
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            # intentar la primera columna
            signal = df.iloc[:, 0].astype(float).values
        else:
            signal = df[numeric_cols[0]].astype(float).values

    if SCIPY_AVAILABLE:
        # detrend y normalizar
        sig = detrend(signal - np.mean(signal))
        # detectar picos (ajustable)
        distance = int(0.4 * fs)  # al menos 0.4s entre latidos
        peaks, _ = find_peaks(sig, distance=distance, height=np.std(sig) * 0.5)
        if len(peaks) < 2:
            return None, None
        # tiempos de picos en segundos
        t_peaks = peaks / float(fs)
        ibis = np.diff(t_peaks)
        if len(ibis) == 0:
            return None, None
        mean_hr = 60.0 / np.mean(ibis)
        sdnn = float(np.std(ibis) * 1000.0)  # ms
        return float(mean_hr), float(sdnn)
    else:
        # fallback: aproximación muy simple:
        # calcular "pseudo-BPM" por energía y duración
        length_sec = len(signal) / float(fs)
        if length_sec <= 0:
            return None, None
        # contar picos simples por umbral
        thr = np.mean(signal) + np.std(signal)
        peaks = np.where(signal > thr)[0]
        # dedupe based on distance (~0.4s)
        if len(peaks) == 0:
            return None, None
        dedup = [peaks[0]]
        min_dist = int(0.4 * fs)
        for p in peaks[1:]:
            if p - dedup[-1] > min_dist:
                dedup.append(p)
        nbeats = len(dedup)
        bpm = (nbeats / length_sec) * 60.0
        # hrv fallback: std of beat intervals approximated
        if len(dedup) >= 2:
            ibis = np.diff(np.array(dedup) / float(fs))
            sdnn = float(np.std(ibis) * 1000.0)
        else:
            sdnn = None
        return float(bpm), sdnn
