import os
import json
import hashlib
import urllib.parse
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler
from supabase import create_client

spbs = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

RANGOS_FISICOS = {
    'corriente_ac':         (0,    3000),
    'voltaje':              (0,    1000),
    'potencia':             (0,  500000),
    'temperatura_pt100':    (-200,  850),
    'temperatura_termopar': (-200, 1300),
    'presion_bar':          (0,     600),
    'nivel_ultrasonico':    (0,     100),
    'nivel_flotador':       (0,     100),
    'caudal':               (0,   10000),
    'ph':                   (0,      14),
    'conductividad':        (0,   20000),
    'turbidez':             (0,    4000),
    'humedad_suelo':        (0,     100),
    'co2':                  (0,   10000),
    'oxigeno_disuelto':     (0,      20),
    'radiacion_solar':      (0,    2000),
    'velocidad_viento':     (0,     200),
    'precipitacion':        (0,     500),
    'contador_pulsos':      (0, 9999999),
}


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        largo  = int(self.headers.get('Content-Length', 0))
        body   = self.rfile.read(largo)
        path   = urllib.parse.urlparse(self.path).path.rstrip('/')

        try:
            data = json.loads(body)
        except Exception:
            self._send(400, {'error': 'JSON inválido'})
            return

        if path.endswith('/boot'):
            self._send(200, handle_boot(data))
        elif path.endswith('/ack'):
            self._send(200, handle_ack(data))
        else:
            self._send(200, handle_lecturas(data))

    def do_GET(self):
        params = dict(urllib.parse.parse_qsl(
            urllib.parse.urlparse(self.path).query
        ))
        self._send(200, handle_comandos(params))

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
# POST /api/gateway — LECTURAS DEL ESP32
# ═══════════════════════════════════════════════════════════════════

def handle_lecturas(data: dict) -> dict:
    gateway_id     = data.get('gateway_id', '').strip()
    gateway_secret = data.get('gateway_secret', '').strip()
    lecturas_raw   = data.get('lecturas', [])

    if not gateway_id or not gateway_secret:
        return {'error': 'Credenciales requeridas', 'status': 401}

    gateway = autenticar_gateway(gateway_id, gateway_secret)
    if not gateway:
        return {'error': 'Gateway no autorizado', 'status': 401}

    planta_id   = gateway['planta_id']
    gateway_uid = gateway['id']

    lecturas_raw    = lecturas_raw[:100]
    sensores_validos = obtener_sensores_planta(planta_id)

    filas_ok     = []
    errores_comm = []
    alertas      = []
    ahora        = datetime.now(timezone.utc).isoformat()

    for l in lecturas_raw:
        sensor_id = l.get('sensor_id', '').strip()
        valor     = l.get('valor')
        unidad    = l.get('unidad', '').strip()

        if sensor_id not in sensores_validos:
            continue

        sensor_cfg = sensores_validos[sensor_id]

        if valor in (-9999, -9998, None):
            errores_comm.append({'sensor_id': sensor_id, 'sensor_cfg': sensor_cfg,
                                  'planta_id': planta_id})
            continue

        valor_san = sanitizar_valor(valor, sensor_cfg)
        if valor_san is None:
            continue

        filas_ok.append({
            'dispositivo_id': sensor_id,
            'planta_id':      planta_id,
            'valor':          valor_san,
            'unidad':         unidad or sensor_cfg.get('unidad', ''),
            'calidad':        evaluar_calidad(valor_san, sensor_cfg),
            'timestamp':      ahora,
            'fuente':         'esp32_gateway'
        })

        alerta = verificar_umbral_critico(valor_san, sensor_cfg, planta_id)
        if alerta:
            alertas.append(alerta)

    insertadas = 0
    if filas_ok:
        try:
            spbs.table('lecturas').insert(filas_ok).execute()
            insertadas = len(filas_ok)
            actualizar_ultimo_valor(filas_ok, ahora)
        except Exception as e:
            print(f"Error inserción: {e}")
            return {'error': 'Error al guardar', 'status': 500}

    for err in errores_comm:
        registrar_error_comunicacion(err)

    for alerta in alertas:
        registrar_alerta_umbral(alerta, planta_id)

    actualizar_heartbeat(gateway_uid, insertadas, len(errores_comm))

    return {
        'status':         'ok',
        'recibidas':      insertadas,
        'errores_sensor': len(errores_comm),
        'alertas':        len(alertas),
        'timestamp':      ahora
    }


# ═══════════════════════════════════════════════════════════════════
# GET /api/gateway — COMANDOS PENDIENTES
# ═══════════════════════════════════════════════════════════════════

def handle_comandos(params: dict) -> dict:
    gateway_id     = params.get('gateway_id', '').strip()
    gateway_secret = params.get('secret', '').strip()

    if not gateway_id or not gateway_secret:
        return {'comandos': []}

    gateway = autenticar_gateway(gateway_id, gateway_secret)
    if not gateway:
        return {'error': 'No autorizado', 'status': 401}

    hace_2_min = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()

    comandos = spbs.table('comandos_gateway')\
        .select('id, tipo, pin, estado_destino, dispositivo_id, parametros')\
        .eq('gateway_id', gateway['id'])\
        .eq('estado', 'pendiente')\
        .gte('created_at', hace_2_min)\
        .order('created_at', desc=False)\
        .limit(10)\
        .execute()

    if comandos.data:
        ids = [c['id'] for c in comandos.data]
        spbs.table('comandos_gateway').update({
            'estado':     'enviado',
            'enviado_at': datetime.now(timezone.utc).isoformat()
        }).in_('id', ids).execute()

    return {'status': 'ok', 'comandos': comandos.data or []}


# ═══════════════════════════════════════════════════════════════════
# POST /api/gateway/ack — CONFIRMACIÓN DE COMANDO
# ═══════════════════════════════════════════════════════════════════

def handle_ack(data: dict) -> dict:
    gateway_id     = data.get('gateway_id', '').strip()
    gateway_secret = data.get('gateway_secret', '').strip()
    comando_id     = data.get('comando_id', '').strip()
    ejecutado      = data.get('ejecutado', False)
    error_msg      = data.get('error', '')

    gateway = autenticar_gateway(gateway_id, gateway_secret)
    if not gateway:
        return {'error': 'No autorizado', 'status': 401}

    if not comando_id:
        return {'error': 'comando_id requerido', 'status': 400}

    spbs.table('comandos_gateway').update({
        'estado':       'ejecutado' if ejecutado else 'fallido',
        'ejecutado_at': datetime.now(timezone.utc).isoformat(),
        'error_msg':    error_msg or None
    }).eq('id', comando_id).eq('gateway_id', gateway['id']).execute()

    if ejecutado:
        cmd = spbs.table('comandos_gateway')\
            .select('dispositivo_id, estado_destino')\
            .eq('id', comando_id).single().execute()
        if cmd.data and cmd.data.get('dispositivo_id'):
            spbs.table('dispositivos_industriales').update({
                'estado_actual':  cmd.data['estado_destino'],
                'ultima_lectura': datetime.now(timezone.utc).isoformat()
            }).eq('id', cmd.data['dispositivo_id']).execute()

    return {'status': 'ok', 'comando_id': comando_id}


# ═══════════════════════════════════════════════════════════════════
# POST /api/gateway/boot — REGISTRO DE ARRANQUE
# ═══════════════════════════════════════════════════════════════════

def handle_boot(data: dict) -> dict:
    gateway_id     = data.get('gateway_id', '').strip()
    gateway_secret = data.get('gateway_secret', '').strip()

    gateway = autenticar_gateway(gateway_id, gateway_secret)
    if not gateway:
        return {'error': 'No autorizado', 'status': 401}

    spbs.table('gateways').update({
        'ultima_conexion':  datetime.now(timezone.utc).isoformat(),
        'firmware_version': data.get('firmware_version', ''),
        'wifi_rssi':        data.get('wifi_rssi'),
        'free_heap':        data.get('free_heap'),
        'estado':           'online',
        'boots_totales':    (gateway.get('boots_totales') or 0) + 1
    }).eq('id', gateway['id']).execute()

    sensores = spbs.table('dispositivos_industriales')\
        .select('id, nombre, tipo, sector, unidad, protocolo, '
                'rango_normal_min, rango_normal_max, '
                'umbral_critico_min, umbral_critico_max, '
                'intervalo_lectura_seg, es_actuador, es_sensor')\
        .eq('planta_id', gateway['planta_id'])\
        .eq('activo', True)\
        .execute()

    return {
        'status':              'ok',
        'planta_id':           gateway['planta_id'],
        'intervalo_envio_seg': 30,
        'intervalo_cmd_seg':   10,
        'sensores':            sensores.data or [],
        'server_time':         datetime.now(timezone.utc).isoformat()
    }


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def autenticar_gateway(gateway_id: str, secret: str) -> dict | None:
    secret_hash = hashlib.sha256(secret.encode()).hexdigest()
    resultado   = spbs.table('gateways')\
        .select('id, planta_id, activo, boots_totales')\
        .eq('codigo', gateway_id)\
        .eq('secret_hash', secret_hash)\
        .single().execute()

    if not resultado.data or not resultado.data.get('activo'):
        return None
    return resultado.data


def obtener_sensores_planta(planta_id: str) -> dict:
    resultado = spbs.table('dispositivos_industriales')\
        .select('id, nombre, tipo, unidad, '
                'rango_normal_min, rango_normal_max, '
                'umbral_alerta_min, umbral_alerta_max, '
                'umbral_critico_min, umbral_critico_max')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .execute()
    return {s['id']: s for s in (resultado.data or [])}


def sanitizar_valor(valor: float, sensor_cfg: dict) -> float | None:
    tipo  = sensor_cfg.get('tipo', '')
    rango = RANGOS_FISICOS.get(tipo)
    if rango is None:
        return round(float(valor), 4) if -999999 < valor < 999999 else None
    min_f, max_f = rango
    return round(float(valor), 4) if min_f <= valor <= max_f else None


def evaluar_calidad(valor: float, sensor_cfg: dict) -> str:
    r_min = sensor_cfg.get('rango_normal_min')
    r_max = sensor_cfg.get('rango_normal_max')
    if r_min is None or r_max is None:
        return 'buena'
    if r_min <= valor <= r_max:
        return 'buena'
    a_min = sensor_cfg.get('umbral_alerta_min', r_min * 0.8)
    a_max = sensor_cfg.get('umbral_alerta_max', r_max * 1.2)
    return 'dudosa' if a_min <= valor <= a_max else 'mala'


def verificar_umbral_critico(valor: float, sensor_cfg: dict, planta_id: str) -> dict | None:
    c_max  = sensor_cfg.get('umbral_critico_max')
    c_min  = sensor_cfg.get('umbral_critico_min')
    unidad = sensor_cfg.get('unidad', '')
    nombre = sensor_cfg.get('nombre', 'Sensor')

    if c_max is not None and valor > c_max:
        return {'sensor_id': sensor_cfg['id'], 'tipo': 'umbral_critico_alto',
                'severidad': 'critico', 'valor': valor, 'umbral': c_max,
                'mensaje': f"CRÍTICO: {nombre} = {valor}{unidad} supera {c_max}{unidad}"}
    if c_min is not None and valor < c_min:
        return {'sensor_id': sensor_cfg['id'], 'tipo': 'umbral_critico_bajo',
                'severidad': 'critico', 'valor': valor, 'umbral': c_min,
                'mensaje': f"CRÍTICO: {nombre} = {valor}{unidad} bajo {c_min}{unidad}"}
    return None


def registrar_alerta_umbral(alerta: dict, planta_id: str):
    existente = spbs.table('alarmas').select('id')\
        .eq('dispositivo_id', alerta['sensor_id'])\
        .eq('tipo', alerta['tipo'])\
        .eq('estado', 'activa').execute()
    if existente.data:
        return
    spbs.table('alarmas').insert({
        'planta_id':          planta_id,
        'dispositivo_id':     alerta['sensor_id'],
        'tipo':               alerta['tipo'],
        'severidad':          alerta['severidad'],
        'valor_disparador':   alerta['valor'],
        'mensaje':            alerta['mensaje'],
        'estado':             'activa',
        'nivel_escalamiento': 1,
        'notificados':        [],
        'proxima_escalada':   (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    }).execute()


def registrar_error_comunicacion(err: dict):
    spbs.table('dispositivos_industriales').update({
        'estado_actual':  'error_comm',
        'ultima_lectura': datetime.now(timezone.utc).isoformat()
    }).eq('id', err['sensor_id']).execute()

    spbs.table('errores_comunicacion').insert({
        'dispositivo_id': err['sensor_id'],
        'planta_id':      err['planta_id'],
        'tipo_error':     err.get('tipo_error', 'sin_señal'),
        'timestamp':      datetime.now(timezone.utc).isoformat()
    }).execute()


def actualizar_ultimo_valor(filas: list, ahora: str):
    ultimo_por_sensor = {}
    for fila in filas:
        ultimo_por_sensor[fila['dispositivo_id']] = fila['valor']
    for sensor_id, valor in ultimo_por_sensor.items():
        spbs.table('dispositivos_industriales').update({
            'ultimo_valor':   valor,
            'ultima_lectura': ahora,
            'estado_actual':  'online'
        }).eq('id', sensor_id).execute()


def actualizar_heartbeat(gateway_uid: str, lecturas_ok: int = 0, errores: int = 0):
    spbs.table('gateways').update({
        'ultima_conexion':     datetime.now(timezone.utc).isoformat(),
        'estado':              'online',
        'lecturas_ultimo_env': lecturas_ok,
        'errores_ultimo_env':  errores
    }).eq('id', gateway_uid).execute()
