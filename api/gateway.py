import os
import json
import hashlib
import hmac
from datetime import datetime, timezone, timedelta
from supabase import create_client

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)


# ═══════════════════════════════════════════════════════════════════
# HANDLER PRINCIPAL
#
# Métodos soportados:
#   POST /api/gateway        → ESP32 envía lecturas
#   GET  /api/gateway        → ESP32 pide comandos pendientes
#   POST /api/gateway/ack    → ESP32 confirma ejecución de comando
#   POST /api/gateway/boot   → ESP32 se registra al encender
# ═══════════════════════════════════════════════════════════════════

def handler(request):
    path = request.path.rstrip('/')

    if path.endswith('/boot'):
        return handle_boot(request)

    if path.endswith('/ack'):
        return handle_ack(request)

    if request.method == 'POST':
        return handle_lecturas(request)

    if request.method == 'GET':
        return handle_comandos(request)

    return response_json({'error': 'Método no soportado'}, 405)


# ═══════════════════════════════════════════════════════════════════
# POST /api/gateway — RECIBIR LECTURAS DEL ESP32
# ═══════════════════════════════════════════════════════════════════

def handle_lecturas(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return response_json({'error': 'JSON inválido'}, 400)

    # --- Autenticar gateway ---
    gateway_id     = data.get('gateway_id', '').strip()
    gateway_secret = data.get('gateway_secret', '').strip()
    lecturas_raw   = data.get('lecturas', [])

    if not gateway_id or not gateway_secret:
        return response_json({'error': 'Credenciales requeridas'}, 401)

    gateway = autenticar_gateway(gateway_id, gateway_secret)
    if not gateway:
        return response_json({'error': 'Gateway no autorizado'}, 401)

    planta_id   = gateway['planta_id']
    gateway_uid = gateway['id']

    # --- Validar estructura de lecturas ---
    if not isinstance(lecturas_raw, list) or len(lecturas_raw) == 0:
        actualizar_heartbeat(gateway_uid)
        return response_json({'status': 'ok', 'recibidas': 0, 'errores': 0})

    # Máximo 100 lecturas por request (protección contra abuso)
    lecturas_raw = lecturas_raw[:100]

    # --- Obtener mapa de sensores válidos para esta planta ---
    sensores_validos = obtener_sensores_planta(planta_id)

    # --- Procesar y clasificar lecturas ---
    filas_ok      = []
    errores_comm  = []
    alertas_nivel = []

    for l in lecturas_raw:
        sensor_id = l.get('sensor_id', '').strip()
        valor     = l.get('valor')
        unidad    = l.get('unidad', '').strip()
        ts_raw    = l.get('timestamp')

        # Validar que el sensor pertenece a esta planta
        if sensor_id not in sensores_validos:
            continue

        sensor_cfg = sensores_validos[sensor_id]

        # Normalizar timestamp
        # El ESP32 envía millis() — tiempo desde boot, no Unix time
        # Usamos el tiempo del servidor como fuente de verdad
        timestamp = datetime.now(timezone.utc).isoformat()

        # Valor -9999 = error de comunicación con el sensor físico
        if valor == -9999 or valor is None:
            errores_comm.append({
                'sensor_id':  sensor_id,
                'sensor_cfg': sensor_cfg,
                'planta_id':  planta_id
            })
            continue

        # Valor -9998 = error de CRC en Modbus
        if valor == -9998:
            errores_comm.append({
                'sensor_id':  sensor_id,
                'sensor_cfg': sensor_cfg,
                'planta_id':  planta_id,
                'tipo_error': 'crc_modbus'
            })
            continue

        # Validar rango físico posible (descarta lecturas absurdas)
        valor_sanitizado = sanitizar_valor(valor, sensor_cfg)
        if valor_sanitizado is None:
            continue

        # Construir fila para inserción en lote
        filas_ok.append({
            'dispositivo_id': sensor_id,
            'planta_id':      planta_id,
            'valor':          valor_sanitizado,
            'unidad':         unidad or sensor_cfg.get('unidad', ''),
            'calidad':        evaluar_calidad(valor_sanitizado, sensor_cfg),
            'timestamp':      timestamp,
            'fuente':         'esp32_gateway'
        })

        # Verificar si supera umbral para alerta inmediata
        alerta = verificar_umbral_inmediato(
            valor_sanitizado, sensor_cfg, planta_id
        )
        if alerta:
            alertas_nivel.append(alerta)

    # --- Inserción en lote (una sola query) ---
    insertadas = 0
    if filas_ok:
        try:
            supabase.table('lecturas').insert(filas_ok).execute()
            insertadas = len(filas_ok)
        except Exception as e:
            print(f"Error inserción lecturas: {e}")
            return response_json({'error': 'Error al guardar lecturas'}, 500)

    # --- Actualizar ultimo_valor en dispositivos_industriales ---
    # Solo actualiza el último valor conocido (no inserta en lecturas)
    actualizar_ultimo_valor(filas_ok)

    # --- Registrar errores de comunicación ---
    for err in errores_comm:
        registrar_error_comunicacion(err)

    # --- Registrar alertas por umbral inmediato ---
    for alerta in alertas_nivel:
        registrar_alerta_umbral(alerta, planta_id)

    # --- Actualizar heartbeat del gateway ---
    actualizar_heartbeat(gateway_uid, len(filas_ok), len(errores_comm))

    return response_json({
        'status':          'ok',
        'recibidas':       insertadas,
        'errores_sensor':  len(errores_comm),
        'alertas':         len(alertas_nivel),
        'timestamp':       datetime.now(timezone.utc).isoformat()
    })


# ═══════════════════════════════════════════════════════════════════
# GET /api/gateway — ENTREGAR COMANDOS PENDIENTES AL ESP32
# ═══════════════════════════════════════════════════════════════════

def handle_comandos(request):
    gateway_id     = request.args.get('gateway_id', '').strip()
    gateway_secret = request.args.get('secret', '').strip()

    if not gateway_id or not gateway_secret:
        return response_json({'comandos': []})

    gateway = autenticar_gateway(gateway_id, gateway_secret)
    if not gateway:
        return response_json({'error': 'No autorizado'}, 401)

    # Obtener comandos pendientes para este gateway
    # Con timeout: comandos con más de 2 minutos sin confirmar se descartan
    hace_2_min = (
        datetime.now(timezone.utc) - timedelta(minutes=2)
    ).isoformat()

    comandos = supabase.table('comandos_gateway')\
        .select('id, tipo, pin, estado_destino, dispositivo_id, parametros')\
        .eq('gateway_id', gateway['id'])\
        .eq('estado', 'pendiente')\
        .gte('created_at', hace_2_min)\
        .order('created_at', desc=False)\
        .limit(10)\
        .execute()

    # Marcar como 'enviado' para evitar re-entrega
    if comandos.data:
        ids = [c['id'] for c in comandos.data]
        supabase.table('comandos_gateway')\
            .update({
                'estado':    'enviado',
                'enviado_at': datetime.now(timezone.utc).isoformat()
            })\
            .in_('id', ids)\
            .execute()

    return response_json({
        'status':   'ok',
        'comandos': comandos.data or []
    })


# ═══════════════════════════════════════════════════════════════════
# POST /api/gateway/ack — ESP32 CONFIRMA EJECUCIÓN DE COMANDO
# ═══════════════════════════════════════════════════════════════════

def handle_ack(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return response_json({'error': 'JSON inválido'}, 400)

    gateway_id  = data.get('gateway_id', '').strip()
    secret      = data.get('gateway_secret', '').strip()
    comando_id  = data.get('comando_id', '').strip()
    ejecutado   = data.get('ejecutado', False)
    error_msg   = data.get('error', '')

    gateway = autenticar_gateway(gateway_id, secret)
    if not gateway:
        return response_json({'error': 'No autorizado'}, 401)

    if not comando_id:
        return response_json({'error': 'comando_id requerido'}, 400)

    supabase.table('comandos_gateway')\
        .update({
            'estado':       'ejecutado' if ejecutado else 'fallido',
            'ejecutado_at': datetime.now(timezone.utc).isoformat(),
            'error_msg':    error_msg or None
        })\
        .eq('id', comando_id)\
        .eq('gateway_id', gateway['id'])\
        .execute()

    # Si el comando fue ejecutado, actualizar estado del dispositivo
    if ejecutado:
        cmd = supabase.table('comandos_gateway')\
            .select('dispositivo_id, estado_destino')\
            .eq('id', comando_id)\
            .single()\
            .execute()

        if cmd.data and cmd.data.get('dispositivo_id'):
            supabase.table('dispositivos_industriales')\
                .update({
                    'estado_actual': cmd.data['estado_destino'],
                    'ultima_lectura': datetime.now(timezone.utc).isoformat()
                })\
                .eq('id', cmd.data['dispositivo_id'])\
                .execute()

    return response_json({'status': 'ok', 'comando_id': comando_id})


# ═══════════════════════════════════════════════════════════════════
# POST /api/gateway/boot — ESP32 SE REGISTRA AL ENCENDER
# ═══════════════════════════════════════════════════════════════════

def handle_boot(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return response_json({'error': 'JSON inválido'}, 400)

    gateway_id  = data.get('gateway_id', '').strip()
    secret      = data.get('gateway_secret', '').strip()
    firmware_v  = data.get('firmware_version', 'desconocida')
    wifi_rssi   = data.get('wifi_rssi')        # señal WiFi en dBm
    free_heap   = data.get('free_heap')        # RAM libre en bytes

    gateway = autenticar_gateway(gateway_id, secret)
    if not gateway:
        return response_json({'error': 'No autorizado'}, 401)

    # Registrar boot
    supabase.table('gateways')\
        .update({
            'ultima_conexion': datetime.now(timezone.utc).isoformat(),
            'firmware_version': firmware_v,
            'wifi_rssi':        wifi_rssi,
            'free_heap':        free_heap,
            'estado':           'online',
            'boots_totales':    gateway.get('boots_totales', 0) + 1
        })\
        .eq('id', gateway['id'])\
        .execute()

    # Devolver configuración actualizada de sensores
    # El ESP32 la usa para saber qué sensores leer y con qué parámetros
    sensores = supabase.table('dispositivos_industriales')\
        .select(
            'id, nombre, tipo, sector, '
            'tuya_device_id, protocolo, '
            'unidad, rango_normal_min, rango_normal_max, '
            'umbral_critico_min, umbral_critico_max, '
            'intervalo_lectura_seg, es_actuador, es_sensor'
        )\
        .eq('planta_id', gateway['planta_id'])\
        .eq('activo', True)\
        .execute()

    return response_json({
        'status':              'ok',
        'planta_id':           gateway['planta_id'],
        'intervalo_envio_seg': 30,
        'intervalo_cmd_seg':   10,
        'sensores':            sensores.data or [],
        'server_time':         datetime.now(timezone.utc).isoformat()
    })


# ═══════════════════════════════════════════════════════════════════
# AUTENTICACIÓN DE GATEWAY
# ═══════════════════════════════════════════════════════════════════

def autenticar_gateway(gateway_id: str, secret: str) -> dict | None:
    """
    Cada ESP32 tiene un gateway_id único (ej: AIRBOOK-G-0001)
    y un secret generado al registrar el dispositivo.
    Se almacena hasheado en la BD — nunca en texto plano.
    """
    secret_hash = hashlib.sha256(secret.encode()).hexdigest()

    resultado = supabase.table('gateways')\
        .select('id, planta_id, activo, boots_totales')\
        .eq('codigo', gateway_id)\
        .eq('secret_hash', secret_hash)\
        .single()\
        .execute()

    if not resultado.data:
        return None

    if not resultado.data.get('activo', False):
        return None

    return resultado.data


# ═══════════════════════════════════════════════════════════════════
# OBTENER MAPA DE SENSORES VÁLIDOS (cache por request)
# ═══════════════════════════════════════════════════════════════════

def obtener_sensores_planta(planta_id: str) -> dict:
    """
    Devuelve {sensor_id: config_dict} para validación rápida
    """
    resultado = supabase.table('dispositivos_industriales')\
        .select(
            'id, nombre, tipo, unidad, '
            'rango_normal_min, rango_normal_max, '
            'umbral_alerta_min, umbral_alerta_max, '
            'umbral_critico_min, umbral_critico_max'
        )\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .execute()

    return {
        s['id']: s
        for s in (resultado.data or [])
    }


# ═══════════════════════════════════════════════════════════════════
# SANITIZACIÓN DE VALORES
# ═══════════════════════════════════════════════════════════════════

# Rangos físicos absolutos por tipo de sensor
# Si un valor llega fuera de estos rangos, es un error del hardware
RANGOS_FISICOS = {
    'corriente_ac':         (0,    3000),   # A
    'voltaje':              (0,    1000),   # V
    'potencia':             (0,  500000),   # W
    'temperatura_pt100':    (-200,  850),   # °C
    'temperatura_termopar': (-200, 1300),   # °C
    'presion_bar':          (0,     600),   # bar
    'nivel_ultrasonico':    (0,     100),   # %
    'nivel_flotador':       (0,     100),   # %
    'caudal':               (0,   10000),   # m³/h o L/h
    'ph':                   (0,      14),   # pH
    'conductividad':        (0,   20000),   # µS/cm
    'turbidez':             (0,    4000),   # NTU
    'humedad_suelo':        (0,     100),   # %
    'co2':                  (0,   10000),   # ppm
    'oxigeno_disuelto':     (0,      20),   # mg/L
    'radiacion_solar':      (0,    2000),   # W/m²
    'velocidad_viento':     (0,     200),   # km/h
    'precipitacion':        (0,     500),   # mm/h
    'contador_pulsos':      (0, 9999999),   # pulsos
}

def sanitizar_valor(valor: float, sensor_cfg: dict) -> float | None:
    tipo = sensor_cfg.get('tipo', '')
    rango = RANGOS_FISICOS.get(tipo)

    if rango is None:
        # Tipo desconocido — aceptar si es número razonable
        if -999999 < valor < 999999:
            return round(float(valor), 4)
        return None

    min_f, max_f = rango
    if min_f <= valor <= max_f:
        return round(float(valor), 4)

    # Valor fuera de rango físico posible — descartar
    print(
        f"Valor descartado: sensor {sensor_cfg.get('id')} "
        f"tipo {tipo} valor {valor} fuera de ({min_f},{max_f})"
    )
    return None


# ═══════════════════════════════════════════════════════════════════
# EVALUAR CALIDAD DE LA LECTURA
# ═══════════════════════════════════════════════════════════════════

def evaluar_calidad(valor: float, sensor_cfg: dict) -> str:
    """
    'buena'  — dentro del rango normal
    'dudosa' — fuera del rango normal pero dentro de alarma
    'mala'   — superó umbral de alarma (dato válido pero preocupante)
    """
    rango_min = sensor_cfg.get('rango_normal_min')
    rango_max = sensor_cfg.get('rango_normal_max')

    if rango_min is None or rango_max is None:
        return 'buena'

    if rango_min <= valor <= rango_max:
        return 'buena'

    alerta_min = sensor_cfg.get('umbral_alerta_min', rango_min * 0.8)
    alerta_max = sensor_cfg.get('umbral_alerta_max', rango_max * 1.2)

    if alerta_min <= valor <= alerta_max:
        return 'dudosa'

    return 'mala'


# ═══════════════════════════════════════════════════════════════════
# VERIFICAR UMBRAL INMEDIATO (crítico — no espera al analyzer)
# ═══════════════════════════════════════════════════════════════════

def verificar_umbral_inmediato(
    valor: float, sensor_cfg: dict, planta_id: str
) -> dict | None:
    """
    Solo verifica umbrales CRÍTICOS aquí.
    Los umbrales de alerta los maneja el analyzer.py cada minuto.
    La idea: críticos se notifican en segundos, no en un minuto.
    """
    critico_max = sensor_cfg.get('umbral_critico_max')
    critico_min = sensor_cfg.get('umbral_critico_min')
    unidad      = sensor_cfg.get('unidad', '')

    if critico_max is not None and valor > critico_max:
        return {
            'sensor_id':   sensor_cfg['id'],
            'sensor_nombre': sensor_cfg.get('nombre', 'Sensor'),
            'tipo':        'umbral_critico_alto',
            'severidad':   'critico',
            'valor':       valor,
            'umbral':      critico_max,
            'mensaje': (
                f"CRÍTICO: {sensor_cfg.get('nombre','Sensor')} "
                f"= {valor}{unidad} supera límite {critico_max}{unidad}"
            )
        }

    if critico_min is not None and valor < critico_min:
        return {
            'sensor_id':   sensor_cfg['id'],
            'sensor_nombre': sensor_cfg.get('nombre', 'Sensor'),
            'tipo':        'umbral_critico_bajo',
            'severidad':   'critico',
            'valor':       valor,
            'umbral':      critico_min,
            'mensaje': (
                f"CRÍTICO: {sensor_cfg.get('nombre','Sensor')} "
                f"= {valor}{unidad} bajo límite {critico_min}{unidad}"
            )
        }

    return None


# ═══════════════════════════════════════════════════════════════════
# REGISTRAR ALERTA POR UMBRAL CRÍTICO INMEDIATO
# ═══════════════════════════════════════════════════════════════════

def registrar_alerta_umbral(alerta: dict, planta_id: str):
    # No duplicar si ya hay una alarma activa del mismo tipo y sensor
    existente = supabase.table('alarmas')\
        .select('id')\
        .eq('dispositivo_id', alerta['sensor_id'])\
        .eq('tipo', alerta['tipo'])\
        .eq('estado', 'activa')\
        .execute()

    if existente.data:
        return

    supabase.table('alarmas').insert({
        'planta_id':          planta_id,
        'dispositivo_id':     alerta['sensor_id'],
        'tipo':               alerta['tipo'],
        'severidad':          alerta['severidad'],
        'valor_disparador':   alerta['valor'],
        'mensaje':            alerta['mensaje'],
        'estado':             'activa',
        'nivel_escalamiento': 1,
        'notificados':        [],
        'proxima_escalada': (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat()
    }).execute()


# ═══════════════════════════════════════════════════════════════════
# REGISTRAR ERROR DE COMUNICACIÓN CON SENSOR FÍSICO
# ═══════════════════════════════════════════════════════════════════

def registrar_error_comunicacion(err: dict):
    sensor_cfg = err['sensor_cfg']
    tipo_error = err.get('tipo_error', 'sin_señal')

    # Actualizar estado del dispositivo
    supabase.table('dispositivos_industriales')\
        .update({
            'estado_actual':  'error_comm',
            'ultima_lectura': datetime.now(timezone.utc).isoformat()
        })\
        .eq('id', err['sensor_id'])\
        .execute()

    # Registrar en tabla de errores para análisis posterior
    supabase.table('errores_comunicacion').insert({
        'dispositivo_id': err['sensor_id'],
        'planta_id':      err['planta_id'],
        'tipo_error':     tipo_error,
        'timestamp':      datetime.now(timezone.utc).isoformat()
    }).execute()


# ═══════════════════════════════════════════════════════════════════
# ACTUALIZAR ÚLTIMO VALOR EN DISPOSITIVOS (sin query extra)
# ═══════════════════════════════════════════════════════════════════

def actualizar_ultimo_valor(filas: list):
    """
    Actualiza ultimo_valor y ultima_lectura en dispositivos_industriales.
    Se hace en lote para minimizar queries a Supabase.
    """
    ahora = datetime.now(timezone.utc).isoformat()

    # Agrupar por sensor_id — nos quedamos solo con el último valor
    ultimo_por_sensor = {}
    for fila in filas:
        ultimo_por_sensor[fila['dispositivo_id']] = fila['valor']

    for sensor_id, valor in ultimo_por_sensor.items():
        supabase.table('dispositivos_industriales')\
            .update({
                'ultimo_valor':   valor,
                'ultima_lectura': ahora,
                'estado_actual':  'online'
            })\
            .eq('id', sensor_id)\
            .execute()


# ═══════════════════════════════════════════════════════════════════
# ACTUALIZAR HEARTBEAT DEL GATEWAY
# ═══════════════════════════════════════════════════════════════════

def actualizar_heartbeat(
    gateway_uid: str,
    lecturas_ok: int = 0,
    errores: int = 0
):
    supabase.table('gateways')\
        .update({
            'ultima_conexion':    datetime.now(timezone.utc).isoformat(),
            'estado':             'online',
            'lecturas_ultimo_env': lecturas_ok,
            'errores_ultimo_env':  errores
        })\
        .eq('id', gateway_uid)\
        .execute()


# ═══════════════════════════════════════════════════════════════════
# HELPER: RESPUESTA JSON
# ═══════════════════════════════════════════════════════════════════

def response_json(data: dict, status: int = 200):
    body = json.dumps(data, ensure_ascii=False)
    return {
        'statusCode': status,
        'headers':    {'Content-Type': 'application/json'},
        'body':       body
    }
