# app.py - completa con sensores, carga, alertas y correlaciones
import dash
from dash import dcc, html, Input, Output, State, ALL, no_update
import dash_bootstrap_components as dbc
import os
import base64
import io
import pandas as pd
import plotly.graph_objects as go
from flask import send_from_directory

from db import (
    init_db,
    register_user,
    authenticate_user,
    save_questionnaire,
    get_questionnaire_history,
    export_user_data_csv,
    get_user_by_id,
    save_sensor_data,
    get_sensor_history,
    compute_acwr,
    compute_session_load_from_responses,
    get_training_load_history,
)
from questionnaires import get_questionnaire_list, render_questionnaire_form, QUESTIONNAIRES
import sensors

# Inicialización
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.CYBORG])
server = app.server
init_db()

# ruta para descargas
@app.server.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory("data", filename, as_attachment=True)

# ---------- LAYOUTS ----------
def login_layout():
    return dbc.Container([
        html.H1("🏋️ Monitor Deportivo", className="text-center text-info mt-4"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("🔑 Iniciar sesión"),
                    dbc.CardBody([
                        dbc.Input(id="login-username", placeholder="Usuario", type="text", className="mb-3"),
                        dbc.Input(id="login-password", placeholder="Contraseña", type="password", className="mb-3"),
                        dbc.Button("Entrar", id="login-btn", color="primary", className="w-100"),
                        html.Div(id="login-message", className="mt-2")
                    ])
                ])
            ], md=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("📝 Registro"),
                    dbc.CardBody([
                        dbc.Input(id="reg-username", placeholder="Usuario", type="text", className="mb-3"),
                        dbc.Input(id="reg-password", placeholder="Contraseña", type="password", className="mb-3"),
                        dbc.Button("Registrar", id="reg-btn", color="success", className="w-100"),
                        html.Div(id="reg-message", className="mt-2")
                    ])
                ])
            ], md=6)
        ])
    ], fluid=True)

def monitor_tab(username):
    # card bienestar y alertas en la parte superior
    summary_card = dbc.Card([
        dbc.CardHeader(html.H5("📋 Resumen y alertas")),
        dbc.CardBody([
            html.Div(id="wellness-summary"),  # rellenado por callback
            html.Div(id="alerts-area", className="mt-2")
        ])
    ], className="mb-3 shadow")

    # cuestionarios seleccionables
    questionnaires_card = dbc.Card([
        dbc.CardHeader(html.H5("🧩 Cuestionarios")),
        dbc.CardBody([
            dcc.Checklist(
                id="questionnaire-checklist",
                options=[{"label": q["title"], "value": q["id"]} for q in get_questionnaire_list()],
                value=["general"],
            ),
            html.Div(id="selected-questionnaires-forms", className="mt-3"),
            dbc.Button("Guardar respuestas", id="submit-selected-btn", color="primary", className="mt-3 w-100"),
            html.Div(id="questionnaire-feedback", className="mt-2")
        ])
    ], className="mb-3")

    # upload sensores
    sensors_card = dbc.Card([
        dbc.CardHeader(html.H5("📡 Sensores (subir CSV)")),
        dbc.CardBody([
            dcc.Upload(id="upload-data", children=html.Div(["Arrastra o selecciona archivos CSV aquí."]), multiple=True,
                       style={"border":"2px dashed #666","padding":"12px","textAlign":"center","borderRadius":"6px","background":"#111"}),
            html.Div(id="upload-output", className="mt-2")
        ])
    ], className="mb-3")

    return dbc.Container([
        summary_card,
        dbc.Row([
            dbc.Col(questionnaires_card, md=6),
            dbc.Col(sensors_card, md=6)
        ])
    ], fluid=True)

def history_tab(username):
    # controls for correlation scatter
    corr_controls = dbc.Card([
        dbc.CardHeader(html.H5("🔗 Correlación")),
        dbc.CardBody([
            dbc.RadioItems(
                id="corr-choice",
                options=[
                    {"label": "Carga vs Dolor", "value": "dolor"},
                    {"label": "Carga vs Horas de sueño", "value": "suenio"}
                ],
                value="dolor",
                inline=True
            ),
            dcc.Graph(id="corr-graph", config={"displayModeBar": False})
        ])
    ], className="mb-3")

    # sensor historical graphs
    sensor_card = dbc.Card([
        dbc.CardHeader(html.H5("📡 Métricas de sensores")),
        dbc.CardBody([dcc.Graph(id="sensors-graph", config={"displayModeBar": False})])
    ], className="mb-3")

    return dbc.Container([
        dbc.Row([
            dbc.Col(corr_controls, md=6),
            dbc.Col(sensor_card, md=6)
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(id="history-graph", config={"displayModeBar": False}), md=12)
        ])
    ], fluid=True)

def dashboard_layout(username_display=""):
    return dbc.Container([
        dbc.NavbarSimple(
            [
                dbc.NavItem(html.Span(f"👋 {username_display}", className="text-white me-3")),
                dbc.Button("Cerrar sesión", id="logout-btn", color="danger")
            ],
            brand="Monitor Deportivo | Dashboard",
            color="dark", dark=True, className="mb-3"
        ),
        dbc.Tabs([
            dbc.Tab(monitor_tab(username_display), label="📝 Monitor", tab_id="tab-monitor"),
            dbc.Tab(history_tab(username_display), label="📈 Histórico", tab_id="tab-history"),
            dbc.Tab(html.Div("Sensores: sube un CSV en Monitor → Sensores procesados aparecerán en Histórico."),
                    label="📡 Sensores", tab_id="tab-sensors")
        ], id="main-tabs", active_tab="tab-monitor")
    ], fluid=True)

# Root layout
app.layout = dbc.Container([
    dcc.Store(id="session-store", storage_type="local"),
    html.Div(id="page-content")
], fluid=True)

# ---------- Callbacks: Auth ----------
@app.callback(
    Output("login-message", "children"),
    Output("session-store", "data", allow_duplicate=True),
    Input("login-btn", "n_clicks"),
    State("login-username", "value"),
    State("login-password", "value"),
    prevent_initial_call=True
)
def handle_login(n, username, password):
    if not username or not password:
        return dbc.Alert("Introduce usuario y contraseña", color="warning"), no_update
    user_id = authenticate_user(username, password)
    if user_id:
        payload = {"user_id": user_id, "username": username}
        return "", payload
    return dbc.Alert("Usuario o contraseña incorrectos", color="danger"), no_update

@app.callback(
    Output("session-store", "data", allow_duplicate=True),
    Input("logout-btn", "n_clicks"),
    prevent_initial_call=True
)
def logout_user(n):
    if not n:
        raise dash.exceptions.PreventUpdate
    return {}

@app.callback(
    Output("reg-message", "children"),
    Input("reg-btn", "n_clicks"),
    State("reg-username", "value"),
    State("reg-password", "value"),
    prevent_initial_call=True
)
def handle_register(n, username, password):
    if not username or not password:
        return dbc.Alert("Rellena usuario y contraseña", color="warning")
    ok = register_user(username, password, 25, "test")
    if ok:
        return dbc.Alert("✅ Usuario registrado. Ahora inicia sesión.", color="success")
    return dbc.Alert("⚠️ Ese usuario ya existe.", color="danger")

# switch page
@app.callback(
    Output("page-content", "children"),
    Input("session-store", "data")
)
def display_page(session_data):
    if session_data and "user_id" in session_data:
        u = get_user_by_id(session_data["user_id"])
        username = u[1] if u else ""
        return dashboard_layout(username_display=username)
    return login_layout()

# ---------- Callbacks: Monitor / Questionnaires ----------
@app.callback(
    Output("selected-questionnaires-forms", "children"),
    Input("questionnaire-checklist", "value")
)
def render_selected_forms(selected):
    if not selected:
        return dbc.Alert("Selecciona al menos un cuestionario", color="warning")
    return [render_questionnaire_form(qid) for qid in selected]

@app.callback(
    Output("questionnaire-feedback", "children"),
    Output("wellness-summary", "children"),
    Output("alerts-area", "children"),
    Input("submit-selected-btn", "n_clicks"),
    State("questionnaire-checklist", "value"),
    State({"type": "q-field", "qid": ALL, "key": ALL}, "value"),
    State("session-store", "data"),
    prevent_initial_call=True
)
def submit_selected(n_clicks, checklist_values, all_values, session_data):
    # guardado
    if not session_data or not session_data.get("user_id"):
        return (dbc.Alert("⚠️ No se ha detectado sesión activa. Recarga e inicia sesión de nuevo.", color="danger"),
                "", "")
    user_id = session_data.get("user_id")

    values_iter = iter(all_values)
    saved = []
    for qid in checklist_values:
        q_def = QUESTIONNAIRES.get(qid)
        if not q_def:
            continue
        responses = {}
        for f in q_def["fields"]:
            responses[f["key"]] = next(values_iter, None)
        save_questionnaire(user_id, qid, responses)
        saved.append(qid)

    # construir resumen y alertas con la última entrada 'general'
    history_general = get_questionnaire_history(user_id, questionnaire_id="general")
    latest = history_general[-1] if history_general else None

    # resumen card content
    if latest:
        lr = latest["responses"]
        last_rpe = lr.get("rpe")
        last_sleep = lr.get("suenio") or lr.get("horas")
        load_today = compute_session_load_from_responses(lr) or "N/A"
        summary = dbc.ListGroup([
            dbc.ListGroupItem(f"Último RPE: {last_rpe}"),
            dbc.ListGroupItem(f"Horas sueño: {last_sleep}"),
            dbc.ListGroupItem(f"Carga sesión (RPE×min): {load_today}")
        ])
    else:
        summary = html.Div("No hay entradas aún.", className="text-muted")

    # alertas basadas en reglas
    alerts = []
    # check fatigue/dolor in latest responses (if present)
    if latest:
        fatiga = float(latest["responses"].get("fatiga") or 0)
        dolor = float(latest["responses"].get("dolor") or 0)
        acwr_data = compute_acwr(user_id)
        acwr_val = acwr_data["acwr"] if acwr_data else None

        if dolor > 6 or fatiga > 8:
            alerts.append(dbc.Alert("⚠️ ALTO RIESGO: Fatiga o dolor elevados", color="danger"))
        elif acwr_val and acwr_val > 1.4:
            alerts.append(dbc.Alert(f"⚠️ Precaución: ACWR alto ({acwr_val:.2f})", color="warning"))
        elif acwr_val and 1.0 <= acwr_val <= 1.25:
            alerts.append(dbc.Alert("✅ Estado óptimo", color="success"))
        else:
            # default neutral
            alerts.append(dbc.Alert("ℹ️ Monitorizando - muestra valores en tiempo real", color="info"))
    else:
        alerts.append(dbc.Alert("No hay datos para evaluar.", color="secondary"))

    msg = dbc.Alert(f"✅ Cuestionarios guardados: {', '.join(saved)}", color="success") if saved else dbc.Alert("⚠️ No se guardaron cuestionarios.", color="warning")
    return msg, summary, html.Div(alerts)

# ---------- Callbacks: Upload sensores ----------
def parse_upload_and_process(contents, filename):
    df = sensors.parse_csv_contents(contents, filename)
    if df is None:
        return None, "No se ha podido leer el CSV"
    bpm, hrv = sensors.load_ecg_and_compute_bpm(df)
    return {"bpm": bpm, "hrv": hrv, "filename": filename}, None

@app.callback(
    Output("upload-output", "children"),
    Output("sensors-graph", "figure"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
    State("session-store", "data"),
    prevent_initial_call=True
)
def handle_upload(list_of_contents, list_of_names, session_data):
    # inicial figura vacía
    empty_fig = {"data": [], "layout": {"template": "plotly_dark", "title": "Sensores"}}
    if not session_data or not session_data.get("user_id"):
        return dbc.Alert("⚠️ Inicia sesión para subir archivos.", color="danger"), empty_fig

    user_id = session_data.get("user_id")
    outputs = []
    # guardar cada archivo procesado y luego graficar histórico
    for contents, name in zip(list_of_contents, list_of_names):
        result, err = parse_upload_and_process(contents, name)
        if err:
            outputs.append(dbc.Alert(f"Error con {name}: {err}", color="danger"))
            continue
        bpm = result["bpm"]
        hrv = result["hrv"]
        fname = result["filename"]
        # guardar en DB
        save_sensor_data(user_id, fname, bpm, hrv)
        outputs.append(dbc.Alert(f"{fname}: BPM={bpm}, HRV={hrv}", color="success"))

    # construir figura con histórico
    hist = get_sensor_history(user_id)
    if not hist:
        return html.Div(outputs), empty_fig
    df = pd.DataFrame(hist)
    fig = go.Figure()
    if df["bpm"].notnull().any():
        fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bpm"], mode="lines+markers", name="BPM"))
    if df["hrv"].notnull().any():
        fig.add_trace(go.Scatter(x=df["timestamp"], y=df["hrv"], mode="lines+markers", name="HRV"))
    fig.update_layout(template="plotly_dark", title="Histórico sensores", xaxis_title="Fecha", yaxis_title="Valor")
    return html.Div(outputs), fig

# ---------- Callbacks: Histórico / Correlaciones ----------
@app.callback(
    Output("history-graph", "figure"),
    Input("history-graph", "id"),  # trigger once (or you can use interval)
    State("session-store", "data")
)
def update_history_graph(_, session_data):
    if not session_data or not session_data.get("user_id"):
        return {"data": [], "layout": {"template": "plotly_dark", "title": "Inicia sesión para ver histórico"}}
    user_id = session_data.get("user_id")
    # crear gráfica de cargas en el tiempo (general)
    loads = get_training_load_history(user_id)
    df = pd.DataFrame([d for d in loads if d["load"] is not None])
    if df.empty:
        return {"data": [], "layout": {"template": "plotly_dark", "title": "No hay datos de carga"}}
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["load"], mode="lines+markers", name="Carga"))
    fig.update_layout(template="plotly_dark", title="Evolución de Carga de Entrenamiento", xaxis_title="Fecha", yaxis_title="Carga (RPE×min)")
    return fig

@app.callback(
    Output("corr-graph", "figure"),
    Input("corr-choice", "value"),
    State("session-store", "data")
)
def update_correlation(choice, session_data):
    if not session_data or not session_data.get("user_id"):
        return {"data": [], "layout": {"template": "plotly_dark", "title": "Inicia sesión"}}
    user_id = session_data.get("user_id")
    # build dataframe with load and subjective metrics from 'general' or 'bienestar' entries
    q_all = get_questionnaire_history(user_id)
    rows = []
    for r in q_all:
        if r["questionnaire_id"] in ("general", "bienestar", "sueno"):
            load = compute_session_load_from_responses(r["responses"])
            rows.append({
                "timestamp": r["timestamp"],
                "load": load,
                "dolor": r["responses"].get("dolor"),
                "suenio": r["responses"].get("suenio") or r["responses"].get("horas"),
            })
    df = pd.DataFrame(rows).dropna(subset=["load"])
    if df.empty:
        return {"data": [], "layout": {"template": "plotly_dark", "title": "No hay datos suficientes"}}
    if choice == "dolor":
        y = pd.to_numeric(df["dolor"], errors="coerce")
        title = "Carga vs Dolor"
    else:
        y = pd.to_numeric(df["suenio"], errors="coerce")
        title = "Carga vs Horas de sueño"
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["load"], y=y, mode="markers", marker=dict(size=8), name=title))
    fig.update_layout(template="plotly_dark", xaxis_title="Carga (RPE×min)", yaxis_title=choice.capitalize(), title=title)
    return fig

# ---------- Export ----------
@app.callback(
    Output("export-link", "children"),
    Input("export-btn", "n_clicks"),
    State("session-store", "data"),
    prevent_initial_call=True
)
def handle_export(n, session_data):
    if not session_data or not session_data.get("user_id"):
        return dbc.Alert("Inicia sesión para exportar.", color="danger")
    user_id = session_data.get("user_id")
    outpath = export_user_data_csv(user_id)
    filename = os.path.basename(outpath)
    return dbc.Alert([html.Span("✅ Archivo listo. "), html.A("Descargar CSV", href=f"/download/{filename}", target="_blank")], color="success")

# Run
if __name__ == "__main__":
    app.run(debug=True)
