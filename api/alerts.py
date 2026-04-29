import os
import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler
from supabase import create_client

spbs = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

TWILIO_SID   = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM  = os.environ.get('TWILIO_WHATSAPP_FROM', '')

TIEMPOS_ESCALADO = {
    ('emergencia', 1): 1,  ('emergencia', 2): 2,
    ('critico',    1): 2,  ('critico',    2): 5,
    ('alerta',     1): 5,  ('alerta',     2): 15,
}


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self._procesar()

    def do_POST(self):
        self._procesar()

    def _procesar(self):
        ahora = datetime.now(timezone.utc)
        resultado = {
            'status':            'ok',
            'escaladas':         escalar_alarmas_pendientes(ahora),
            'gateways_offline':  verificar_gateways_offline(ahora),
            'mensajes_enviados': procesar_cola_mensajes(),
            'timestamp':         ahora.isoformat()
        }
        self._send(200, resultado)

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
# 1. ESCALAMIENTO DE ALARMAS
# ═══════════════════════════════════════════════════════════════════

def escalar_alarmas_pendientes(ahora: datetime) -> int:
    escaladas = 0

    alarmas = spbs.table('alarmas')\
        .select('*, dispositivos_industriales(nombre, tipo, sector), plantas(nombre)')\
        .eq('estado', 'activa')\
        .lte('proxima_escalada', ahora.isoformat())\
        .lt('nivel_escalamiento', 3)\
        .execute()

    for alarma in (alarmas.data or []):
        nivel_actual  = alarma['nivel_escalamiento']
        severidad     = alarma['severidad']
        nivel_destino = nivel_actual + 1

        destinatarios = obtener_destinatarios_nivel(
            alarma['planta_id'], nivel_destino, ahora
        )

        mensaje = construir_mensaje_escalamiento(alarma, nivel_destino)

        for dest in destinatarios:
            encolar_mensaje(
                planta_id=alarma['planta_id'],
                destinatario=dest['whatsapp'],
                mensaje=mensaje,
                tipo='escalamiento',
                prioridad='alta' if severidad == 'critico' else 'normal'
            )

        minutos_prox = TIEMPOS_ESCALADO.get((severidad, nivel_destino), 30)
        proxima      = ahora + timedelta(minutes=minutos_prox)

        notificados_actuales = alarma.get('notificados') or []
        nuevos_numeros       = [d['whatsapp'] for d in destinatarios]

        spbs.table('alarmas').update({
            'nivel_escalamiento': nivel_destino,
            'notificados':        notificados_actuales + nuevos_numeros,
            'proxima_escalada':   proxima.isoformat() if nivel_destino < 3 else None
        }).eq('id', alarma['id']).execute()

        escaladas += 1

    return escaladas


def obtener_destinatarios_nivel(
    planta_id: str, nivel: int, ahora: datetime
) -> list:
    ROLES_POR_NIVEL = {
        1: ['operador', 'mantenimiento'],
        2: ['supervisor'],
        3: ['gerente', 'supervisor', 'operador']
    }
    roles       = ROLES_POR_NIVEL.get(nivel, ['gerente'])
    hora_actual = ahora.strftime('%H:%M')

    operadores = spbs.table('operadores')\
        .select('whatsapp, nombre, rol, horario_alertas')\
        .eq('planta_id', planta_id)\
        .in_('rol', roles)\
        .eq('activo', True)\
        .eq('recibe_alertas', True)\
        .execute()

    resultado = []
    for op in (operadores.data or []):
        horario = op.get('horario_alertas')
        if horario:
            h_ini = horario.get('inicio', '00:00')
            h_fin = horario.get('fin',    '23:59')
            if not (h_ini <= hora_actual <= h_fin) and nivel < 3:
                continue
        resultado.append(op)

    return resultado


def construir_mensaje_escalamiento(alarma: dict, nivel_destino: int) -> str:
    disp         = alarma.get('dispositivos_industriales') or {}
    plant        = alarma.get('plantas') or {}
    nombre_disp  = disp.get('nombre', 'Sensor desconocido')
    sector       = disp.get('sector', '')
    nombre_plant = plant.get('nombre', 'Planta')
    severidad    = alarma['severidad'].upper()

    creada_at      = datetime.fromisoformat(alarma['created_at'].replace('Z', '+00:00'))
    minutos_activa = int((datetime.now(timezone.utc) - creada_at).total_seconds() / 60)

    NIVEL_LABELS = {1: 'Operador', 2: 'Supervisor', 3: 'Gerencia'}
    nivel_label  = NIVEL_LABELS.get(nivel_destino, 'Responsable')

    if nivel_destino == 2:
        encabezado = "⚠️ ESCALAMIENTO NIVEL 2 — Sin respuesta del operador"
    elif nivel_destino == 3:
        encabezado = "🔴 ESCALAMIENTO CRÍTICO NIVEL 3 — Sin respuesta del supervisor"
    else:
        encabezado = f"📢 ALERTA NIVEL {nivel_destino}"

    return (
        f"{encabezado}\n"
        f"UPTIME IA · {nombre_plant}\n\n"
        f"Dispositivo: {nombre_disp}"
        f"{' · ' + sector if sector else ''}\n"
        f"Severidad: {severidad}\n"
        f"Detalle: {alarma['mensaje']}\n"
        f"Sin atender: {minutos_activa} minutos\n\n"
        f"Requiere atención de {nivel_label}.\n"
        f"Responde *reconocer {alarma['id'][:8]}* para confirmar."
    )


# ═══════════════════════════════════════════════════════════════════
# 2. GATEWAYS OFFLINE
# ═══════════════════════════════════════════════════════════════════

def verificar_gateways_offline(ahora: datetime) -> int:
    offline_count = 0
    umbral        = (ahora - timedelta(minutes=5)).isoformat()

    gateways = spbs.table('gateways')\
        .select('id, codigo, planta_id, estado, ultima_conexion, plantas(nombre)')\
        .eq('activo', True)\
        .lte('ultima_conexion', umbral)\
        .execute()

    for gw in (gateways.data or []):
        if gw['estado'] == 'offline':
            continue

        ultima       = datetime.fromisoformat(gw['ultima_conexion'].replace('Z', '+00:00'))
        minutos_off  = int((ahora - ultima).total_seconds() / 60)
        planta_id    = gw['planta_id']
        nombre_plant = (gw.get('plantas') or {}).get('nombre', 'Planta')

        spbs.table('gateways').update({'estado': 'offline'}).eq('id', gw['id']).execute()

        existente = spbs.table('alarmas')\
            .select('id')\
            .eq('planta_id', planta_id)\
            .eq('tipo', 'gateway_offline')\
            .eq('estado', 'activa')\
            .execute()

        if not existente.data:
            spbs.table('alarmas').insert({
                'planta_id':          planta_id,
                'dispositivo_id':     None,
                'tipo':               'gateway_offline',
                'severidad':          'critico',
                'valor_disparador':   None,
                'mensaje': (
                    f"Gateway {gw['codigo']} sin comunicación "
                    f"hace {minutos_off} min. "
                    f"Sensores de {nombre_plant} sin datos."
                ),
                'estado':             'activa',
                'nivel_escalamiento': 1,
                'notificados':        [],
                'proxima_escalada':   (ahora + timedelta(minutes=2)).isoformat()
            }).execute()

        offline_count += 1

    spbs.table('gateways')\
        .update({'estado': 'online'})\
        .eq('estado', 'offline')\
        .gt('ultima_conexion', umbral)\
        .execute()

    return offline_count


# ═══════════════════════════════════════════════════════════════════
# 3. COLA DE MENSAJES WHATSAPP
# ═══════════════════════════════════════════════════════════════════

def procesar_cola_mensajes() -> int:
    enviados = 0

    mensajes = spbs.table('cola_mensajes_whatsapp')\
        .select('*')\
        .eq('estado', 'pendiente')\
        .order('prioridad', desc=True)\
        .order('created_at', desc=False)\
        .limit(20)\
        .execute()

    for msg in (mensajes.data or []):
        exito    = enviar_whatsapp(msg['destinatario'], msg['mensaje'])
        intentos = (msg.get('intentos') or 0) + 1

        if not exito and intentos < 3:
            spbs.table('cola_mensajes_whatsapp').update({
                'intentos':     intentos,
                'ultimo_error': datetime.now(timezone.utc).isoformat()
            }).eq('id', msg['id']).execute()
            continue

        spbs.table('cola_mensajes_whatsapp').update({
            'estado':     'enviado' if exito else 'fallido',
            'enviado_at': datetime.now(timezone.utc).isoformat(),
            'intentos':   intentos
        }).eq('id', msg['id']).execute()

        if exito:
            enviados += 1

    return enviados


def enviar_whatsapp(destinatario: str, mensaje: str) -> bool:
    if not TWILIO_SID or not TWILIO_TOKEN:
        print(f"[MOCK] → {destinatario}: {mensaje[:60]}")
        return True

    numero = destinatario.replace('whatsapp:', '').strip()
    if not numero.startswith('+'):
        numero = '+' + numero

    import base64
    credentials = base64.b64encode(
        f'{TWILIO_SID}:{TWILIO_TOKEN}'.encode()
    ).decode()

    data = urllib.parse.urlencode({
        'From': TWILIO_FROM,
        'To':   f'whatsapp:{numero}',
        'Body': mensaje
    }).encode()

    req = urllib.request.Request(
        f'https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json',
        data=data,
        headers={
            'Authorization': f'Basic {credentials}',
            'Content-Type':  'application/x-www-form-urlencoded'
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 201)
    except Exception as e:
        print(f"Error Twilio: {e}")
        return False


def encolar_mensaje(
    planta_id: str, destinatario: str, mensaje: str,
    tipo: str = 'alerta', prioridad: str = 'normal'
):
    spbs.table('cola_mensajes_whatsapp').insert({
        'planta_id':    planta_id,
        'destinatario': destinatario,
        'mensaje':      mensaje,
        'tipo':         tipo,
        'prioridad':    prioridad,
        'estado':       'pendiente',
        'intentos':     0,
        'created_at':   datetime.now(timezone.utc).isoformat()
    }).execute()
