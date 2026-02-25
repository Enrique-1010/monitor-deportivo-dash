# questionnaires.py
from dash import dcc, html
import dash_bootstrap_components as dbc

QUESTIONNAIRES = {
    "general": {
        "title": "Autopercepción general",
        "fields": [
            {"key": "fatiga", "label": "Nivel de fatiga (1-10)", "type": "slider", "min": 1, "max": 10, "value": 5},
            {"key": "suenio", "label": "Horas de sueño", "type": "number", "min": 0, "max": 12, "value": 8},
            {"key": "rpe", "label": "Esfuerzo percibido (1-10)", "type": "slider", "min": 1, "max": 10, "value": 5},
            {"key": "duracion", "label": "Duración del entrenamiento (min)", "type": "number", "min": 0, "max": 600, "value": 60}
        ]
    },
    "bienestar": {
        "title": "Bienestar físico",
        "fields": [
            {"key": "dolor", "label": "Dolor muscular (0-10)", "type": "slider", "min": 0, "max": 10, "value": 2},
            {"key": "energia", "label": "Energía actual (1-10)", "type": "slider", "min": 1, "max": 10, "value": 7}
        ]
    },
    "sueno": {
        "title": "Sueño y recuperación",
        "fields": [
            {"key": "horas", "label": "Horas dormidas", "type": "number", "min": 0, "max": 12, "value": 7},
            {"key": "calidad", "label": "Calidad del sueño (1-5)", "type": "slider", "min": 1, "max": 5, "value": 4}
        ]
    }
}

def get_questionnaire_list():
    return [{"id": k, "title": QUESTIONNAIRES[k]["title"]} for k in QUESTIONNAIRES]

def render_questionnaire_form(qid):
    if qid not in QUESTIONNAIRES:
        return html.Div()
    q = QUESTIONNAIRES[qid]

    header = dbc.CardHeader(html.H5(q["title"], className="mb-0 fw-bold text-primary"))
    form_elements = []

    for f in q["fields"]:
        comp_id = {"type": "q-field", "qid": qid, "key": f["key"]}

        if f["type"] == "slider":
            form_elements.append(
                dbc.Row([
                    dbc.Col(html.Label(f["label"], className="fw-bold text-white"), width=12),
                    dbc.Col(
                        dcc.Slider(
                            f["min"], f["max"], 1, value=f["value"], id=comp_id,
                            marks={i: {"label": str(i), "style": {"color": "white"}} for i in range(int(f["min"]), int(f["max"]) + 1)}
                        ),
                        width=12
                    )
                ], className="mb-3")
            )
        elif f["type"] == "number":
            form_elements.append(
                dbc.FormFloating(
                    [
                        dbc.Input(
                            id=comp_id,
                            type="number",
                            min=f.get("min", 0),
                            max=f.get("max", 1000),
                            value=f.get("value", ""),
                            className="bg-dark text-light border-secondary"
                        ),
                        html.Label(f["label"], style={"color": "#bfbfbf"})
                    ],
                    className="mb-3"
                )
            )

    return dbc.Card([header, dbc.CardBody(form_elements)],
                    id={"type": "q-container", "qid": qid},
                    className="mb-3 shadow-sm border-secondary")
