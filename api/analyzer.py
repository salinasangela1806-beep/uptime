import os
import json
import math
import urllib.request
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler
from supabase import create_client

spbs = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_URL     = 'https://api.groq.com/openai/v1/chat/completions'


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self._procesar()

    def do_POST(self):
        self._procesar()

    def _procesar(self):
        resultados = []
        plantas = spbs.table('plantas')\
            .select('id, nombre, tipo')\
            .eq('activa', True).execute()

        for planta in (plantas.data or []):
            r = analizar_planta(planta['id'], planta['nombre'])
            resultados.append(r)

        self._send(200, {
            'status':             'ok',
            'plantas_analizadas': len(resultados),
            'alertas_generadas':  sum(r['alertas'] for r in resultados),
            'timestamp':          datetime.now(timezone.utc).isoformat()
        })

    def _send(self, status: int, data: dict):
        body = json.dumps(data, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


# ═══════════════════════════════════════════════════════════════════
# ANÁLISIS POR PLANTA
# ═══════════════════════════════════════════════════════════════════

def analizar_planta(planta_id: str, planta_nombre: str) -> dict:
    alertas_generadas = 0

    dispositivos = spbs.table('dispositivos_industriales')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .eq('es_sensor', True).execute()

    for dispositivo in (dispositivos.data or []):
        anomalias = analizar_dispositivo(dispositivo, planta_id)

        for anomalia in anomalias:
            if anomalia['severidad'] in ('critico', 'alerta'):
                registrar_alarma(dispositivo, planta_id, anomalia)
                alertas_generadas += 1

            if anomalia.get('requiere_ia'):
                razonamiento = razonar_con_llm(dispositivo, anomalia)
                if razonamiento:
                    encolar_alerta_whatsapp(planta_id, dispositivo, razonamiento)

    return {'planta_id': planta_id, 'alertas': alertas_generadas}


# ═══════════════════════════════════════════════════════════════════
# ANÁLISIS DE UN DISPOSITIVO — 5 CAPAS
# ═══════════════════════════════════════════════════════════════════

def analizar_dispositivo(dispositivo: dict, planta_id: str) -> list:
    anomalias  = []
    sensor_id  = dispositivo['id']

    sin_comm = verificar_comunicacion(dispositivo)
    if sin_comm:
        return [sin_comm]

    hist_1h  = obtener_historial(sensor_id, horas=1)
    hist_24h = obtener_historial(sensor_id, horas=24)
    hist_7d  = obtener_historial(sensor_id, horas=168)

    if not hist_1h:
        return []

    valor_actual = hist_1h[-1]['valor']

    umbral = verificar_umbrales(dispositivo, valor_actual)
    if umbral:
        anomalias.append(umbral)

    if len(hist_24h) >= 10:
        stat = detectar_anomalia_estadistica(dispositivo, valor_actual, hist_24h)
        if stat:
            anomalias.append(stat)

    if len(hist_7d) >= 24:
        tend = detectar_tendencia_sostenida(dispositivo, hist_7d, hist_1h)
        if tend:
            anomalias.append(tend)

    corr = verificar_correlaciones(dispositivo, planta_id, valor_actual)
    if corr:
        anomalias.append(corr)

    return anomalias


# ═══════════════════════════════════════════════════════════════════
# CAPA 1: COMUNICACIÓN
# ═══════════════════════════════════════════════════════════════════

def verificar_comunicacion(dispositivo: dict) -> dict | None:
    ultima = dispositivo.get('ultima_lectura')
    if not ultima:
        return None

    ultima_dt     = datetime.fromisoformat(ultima.replace('Z', '+00:00'))
    ahora         = datetime.now(timezone.utc)
    minutos_sin   = (ahora - ultima_dt).total_seconds() / 60

    UMBRALES = {'critico': 5, 'proceso': 15, 'ambiental': 60, 'default': 30}
    cat       = clasificar_sensor(dispositivo['tipo'])
    umbral    = UMBRALES.get(cat, UMBRALES['default'])

    if minutos_sin > umbral:
        return {
            'tipo':       'sin_comunicacion',
            'severidad':  'critico' if cat == 'critico' else 'alerta',
            'mensaje':    f"Sin señal hace {int(minutos_sin)} min (umbral: {umbral} min)",
            'valor':      None,
            'requiere_ia': False
        }
    return None


# ═══════════════════════════════════════════════════════════════════
# CAPA 2: UMBRALES ESTÁTICOS
# ═══════════════════════════════════════════════════════════════════

def verificar_umbrales(dispositivo: dict, valor: float) -> dict | None:
    c_max  = dispositivo.get('umbral_critico_max')
    c_min  = dispositivo.get('umbral_critico_min')
    a_max  = dispositivo.get('umbral_alerta_max')
    a_min  = dispositivo.get('umbral_alerta_min')
    unidad = dispositivo.get('unidad', '')

    if c_max is not None and valor > c_max:
        return {'tipo':'umbral_critico_alto','severidad':'critico',
                'mensaje':f"CRÍTICO ALTO: {valor:.2f}{unidad} supera {c_max}{unidad}",
                'valor':valor,'umbral':c_max,'requiere_ia':False}
    if c_min is not None and valor < c_min:
        return {'tipo':'umbral_critico_bajo','severidad':'critico',
                'mensaje':f"CRÍTICO BAJO: {valor:.2f}{unidad} bajo {c_min}{unidad}",
                'valor':valor,'umbral':c_min,'requiere_ia':False}
    if a_max is not None and valor > a_max:
        return {'tipo':'umbral_alerta_alto','severidad':'alerta',
                'mensaje':f"Alto: {valor:.2f}{unidad} supera alerta {a_max}{unidad}",
                'valor':valor,'umbral':a_max,'requiere_ia':True}
    if a_min is not None and valor < a_min:
        return {'tipo':'umbral_alerta_bajo','severidad':'alerta',
                'mensaje':f"Bajo: {valor:.2f}{unidad} bajo alerta {a_min}{unidad}",
                'valor':valor,'umbral':a_min,'requiere_ia':True}
    return None


# ═══════════════════════════════════════════════════════════════════
# CAPA 3: ANOMALÍA ESTADÍSTICA
# ═══════════════════════════════════════════════════════════════════

def detectar_anomalia_estadistica(dispositivo, valor_actual, historial) -> dict | None:
    valores = [r['valor'] for r in historial if r.get('valor') is not None]
    if len(valores) < 10:
        return None

    promedio  = sum(valores) / len(valores)
    varianza  = sum((v - promedio) ** 2 for v in valores) / len(valores)
    desviacion = math.sqrt(varianza)

    if desviacion < 0.001:
        return None

    z_score = abs(valor_actual - promedio) / desviacion
    if z_score <= 3.0:
        return None

    pct = abs(valor_actual - promedio) / promedio * 100 if promedio else 0
    return {
        'tipo':        'anomalia_estadistica',
        'severidad':   'alerta',
        'mensaje':     f"Valor anómalo: {valor_actual:.2f} ({pct:.1f}% fuera del promedio 24h de {promedio:.2f}{dispositivo.get('unidad','')})",
        'valor':       valor_actual,
        'promedio_24h': round(promedio, 3),
        'z_score':     round(z_score, 2),
        'requiere_ia': True
    }


# ═══════════════════════════════════════════════════════════════════
# CAPA 4: TENDENCIA SOSTENIDA (mantenimiento predictivo)
# ═══════════════════════════════════════════════════════════════════

def detectar_tendencia_sostenida(dispositivo, hist_7d, hist_1h) -> dict | None:
    valores = [r['valor'] for r in hist_7d if r.get('valor') is not None]
    if len(valores) < 24:
        return None

    n       = len(valores)
    idx     = list(range(n))
    media_x = sum(idx) / n
    media_y = sum(valores) / n

    num = sum((idx[i] - media_x) * (valores[i] - media_y) for i in range(n))
    den = sum((idx[i] - media_x) ** 2 for i in range(n))
    if den == 0:
        return None

    pendiente    = num / den
    valor_inicio = valores[0]
    if valor_inicio == 0:
        return None

    cambio_pct = abs(pendiente * n / valor_inicio) * 100
    if cambio_pct < 15:
        return None

    valor_actual = hist_1h[-1]['valor']
    unidad       = dispositivo.get('unidad', '')
    direccion    = 'creciente' if pendiente > 0 else 'decreciente'

    proyeccion = ''
    umbral_c   = dispositivo.get('umbral_critico_max') if pendiente > 0 else dispositivo.get('umbral_critico_min')
    if umbral_c is not None and pendiente != 0:
        lecturas_hasta = (umbral_c - valor_actual) / pendiente
        if 0 < lecturas_hasta < n * 2:
            horas = lecturas_hasta / 60
            if horas < 48:
                proyeccion = f" Proyección: umbral crítico en ~{horas:.0f}h."

    return {
        'tipo':        'tendencia_sostenida',
        'severidad':   'alerta',
        'mensaje':     f"Tendencia {direccion} del {cambio_pct:.1f}% en 7 días. Actual: {valor_actual:.2f}{unidad}.{proyeccion}",
        'valor':       valor_actual,
        'pendiente':   round(pendiente, 4),
        'cambio_pct':  round(cambio_pct, 1),
        'direccion':   direccion,
        'requiere_ia': True
    }


# ═══════════════════════════════════════════════════════════════════
# CAPA 5: CORRELACIONES
# ═══════════════════════════════════════════════════════════════════

def verificar_correlaciones(dispositivo, planta_id, valor_actual) -> dict | None:
    tipo   = dispositivo.get('tipo', '')
    sector = dispositivo.get('sector', '')

    if tipo == 'motor_arranque' and dispositivo.get('estado_actual') == 'on':
        caudal = spbs.table('dispositivos_industriales')\
            .select('id, ultimo_valor, unidad')\
            .eq('planta_id', planta_id)\
            .eq('sector', sector)\
            .eq('tipo', 'caudal').execute()

        if caudal.data:
            val_c = caudal.data[0].get('ultimo_valor', 0)
            if val_c is not None and val_c < 0.5:
                return {
                    'tipo':      'correlacion_bomba_sin_caudal',
                    'severidad': 'critico',
                    'mensaje':   f"Bomba ON pero caudal={val_c:.2f} {caudal.data[0].get('unidad','')} en {sector}. Posible cavitación u obstrucción.",
                    'valor':     val_c,
                    'requiere_ia': True
                }
    return None


# ═══════════════════════════════════════════════════════════════════
# LLM — RAZONAMIENTO GROQ
# ═══════════════════════════════════════════════════════════════════

def razonar_con_llm(dispositivo: dict, anomalia: dict) -> str | None:
    if not GROQ_API_KEY:
        return None

    hist_7d  = obtener_historial(dispositivo['id'], horas=168)
    valores  = [r['valor'] for r in hist_7d if r.get('valor') is not None]

    resumen = 'Sin historial'
    if valores:
        dia  = max(1, len(valores) // 7)
        dias = [round(sum(valores[i:i+dia])/len(valores[i:i+dia]), 2) for i in range(0, len(valores), dia)]
        resumen = (
            f"Min:{min(valores):.2f} Max:{max(valores):.2f} "
            f"Prom:{sum(valores)/len(valores):.2f}\n"
            f"Tendencia diaria: {' → '.join(str(v) for v in dias[:7])}"
        )

    prompt = (
        f"Eres UPTIME, agente IA industrial de AIRBOOK IoT.\n"
        f"Analiza esta anomalía y genera alerta para el supervisor.\n\n"
        f"DISPOSITIVO: {dispositivo.get('nombre')} · {dispositivo.get('tipo')} · {dispositivo.get('sector')}\n"
        f"Rango normal: {dispositivo.get('rango_normal_min')} a {dispositivo.get('rango_normal_max')} {dispositivo.get('unidad','')}\n\n"
        f"ANOMALÍA: {anomalia['tipo']} · {anomalia['severidad']}\n"
        f"Detalle: {anomalia['mensaje']}\n\n"
        f"HISTORIAL 7 DÍAS:\n{resumen}\n\n"
        f"Responde en máximo 150 palabras:\n"
        f"1. Qué está pasando\n"
        f"2. Causas probables (ordenadas)\n"
        f"3. Acción recomendada con urgencia\n"
        f"4. Si es tendencia, tiempo estimado hasta falla\n"
        f"Español. Solo: ✅ ⚠️ 🔴"
    )

    payload = json.dumps({
        'model':       'llama3-8b-8192',
        'messages':    [{'role': 'user', 'content': prompt}],
        'max_tokens':  300,
        'temperature': 0.3
    }).encode()

    req = urllib.request.Request(
        GROQ_URL, data=payload,
        headers={'Content-Type': 'application/json',
                 'Authorization': f'Bearer {GROQ_API_KEY}'}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())['choices'][0]['message']['content']
    except Exception as e:
        print(f"Error LLM: {e}")
        return anomalia['mensaje']


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def obtener_historial(sensor_id: str, horas: int) -> list:
    desde = (datetime.now(timezone.utc) - timedelta(hours=horas)).isoformat()
    r = spbs.table('lecturas')\
        .select('valor, timestamp')\
        .eq('dispositivo_id', sensor_id)\
        .gte('timestamp', desde)\
        .order('timestamp', desc=False)\
        .limit(2000).execute()
    return r.data or []


def clasificar_sensor(tipo: str) -> str:
    CRITICOS    = {'motor_arranque','valvula_solenoide','presion_bar',
                   'nivel_ultrasonico','corriente_ac','electrovalvula_riego'}
    AMBIENTALES = {'temperatura_ambiente','humedad_ambiente',
                   'radiacion_solar','velocidad_viento','precipitacion'}
    if tipo in CRITICOS:    return 'critico'
    if tipo in AMBIENTALES: return 'ambiental'
    return 'proceso'


def registrar_alarma(dispositivo: dict, planta_id: str, anomalia: dict):
    existente = spbs.table('alarmas').select('id')\
        .eq('dispositivo_id', dispositivo['id'])\
        .eq('tipo', anomalia['tipo'])\
        .eq('estado', 'activa').execute()
    if existente.data:
        return

    spbs.table('alarmas').insert({
        'planta_id':          planta_id,
        'dispositivo_id':     dispositivo['id'],
        'tipo':               anomalia['tipo'],
        'severidad':          anomalia['severidad'],
        'valor_disparador':   anomalia.get('valor'),
        'mensaje':            anomalia['mensaje'],
        'estado':             'activa',
        'nivel_escalamiento': 1,
        'notificados':        [],
        'proxima_escalada':   (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    }).execute()


def encolar_alerta_whatsapp(planta_id: str, dispositivo: dict, mensaje: str):
    operadores = spbs.table('operadores')\
        .select('whatsapp')\
        .eq('planta_id', planta_id)\
        .eq('recibe_alertas', True)\
        .eq('activo', True).execute()

    for op in (operadores.data or []):
        spbs.table('cola_mensajes_whatsapp').insert({
            'planta_id':    planta_id,
            'destinatario': op['whatsapp'],
            'mensaje':      f"⚠️ UPTIME IA — {dispositivo.get('nombre','Sensor')}\n\n{mensaje}",
            'tipo':         'alerta_automatica',
            'prioridad':    'normal',
            'estado':       'pendiente',
            'intentos':     0,
            'created_at':   datetime.now(timezone.utc).isoformat()
        }).execute()
