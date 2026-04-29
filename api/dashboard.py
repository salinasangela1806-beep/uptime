import os
import json
import urllib.parse
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler
from supabase import create_client

spbs = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        path   = parsed.path.rstrip('/')

        planta_id = params.get('planta_id', '').strip()

        if path.endswith('/resumen'):
            self._send(200, resumen_planta(planta_id))
        elif path.endswith('/dispositivos'):
            self._send(200, listar_dispositivos(planta_id))
        elif path.endswith('/alarmas'):
            self._send(200, listar_alarmas(planta_id))
        elif path.endswith('/historial'):
            sensor_id = params.get('sensor_id', '').strip()
            horas     = int(params.get('horas', 24))
            self._send(200, historial_sensor(sensor_id, horas))
        elif path.endswith('/turno'):
            self._send(200, turno_activo(planta_id))
        elif path.endswith('/plantas'):
            self._send(200, listar_plantas())
        else:
            self._send(200, {'status': 'ok', 'servicio': 'UPTIME IA Dashboard API'})

    def do_OPTIONS(self):
        # CORS para el dashboard web
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _send(self, status: int, data: dict):
        body = json.dumps(data, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


# ═══════════════════════════════════════════════════════════════════
# ENDPOINTS DE DATOS
# ═══════════════════════════════════════════════════════════════════

def listar_plantas() -> dict:
    plantas = spbs.table('plantas')\
        .select('id, codigo, nombre, tipo, ciudad, activa')\
        .eq('activa', True)\
        .order('nombre').execute()
    return {'plantas': plantas.data or []}


def resumen_planta(planta_id: str) -> dict:
    if not planta_id:
        return {'error': 'planta_id requerido'}

    planta = spbs.table('plantas')\
        .select('id, codigo, nombre, tipo, ciudad, departamento')\
        .eq('id', planta_id).single().execute()
    if not planta.data:
        return {'error': 'Planta no encontrada'}

    disps = spbs.table('dispositivos_industriales')\
        .select('id, estado_actual')\
        .eq('planta_id', planta_id)\
        .eq('activo', True).execute()

    alarmas = spbs.table('alarmas')\
        .select('id, severidad')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa').execute()

    gateways = spbs.table('gateways')\
        .select('id, codigo, estado, ultima_conexion')\
        .eq('planta_id', planta_id)\
        .eq('activo', True).execute()

    total_disps   = len(disps.data or [])
    online_disps  = sum(1 for d in (disps.data or []) if d.get('estado_actual') == 'online')
    error_disps   = sum(1 for d in (disps.data or []) if d.get('estado_actual') == 'error_comm')
    criticas      = sum(1 for a in (alarmas.data or []) if a.get('severidad') == 'critico')
    alertas       = sum(1 for a in (alarmas.data or []) if a.get('severidad') == 'alerta')
    gw_online     = sum(1 for g in (gateways.data or []) if g.get('estado') == 'online')

    turno = spbs.table('turnos')\
        .select('operador_nombre, inicio')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .limit(1).execute()

    return {
        'planta':         planta.data,
        'dispositivos':   {'total': total_disps, 'online': online_disps, 'error': error_disps},
        'alarmas':        {'total': len(alarmas.data or []), 'criticas': criticas, 'alertas': alertas},
        'gateways':       {'total': len(gateways.data or []), 'online': gw_online},
        'turno_activo':   turno.data[0] if turno.data else None,
        'timestamp':      datetime.now(timezone.utc).isoformat()
    }


def listar_dispositivos(planta_id: str) -> dict:
    if not planta_id:
        return {'error': 'planta_id requerido'}

    disps = spbs.table('dispositivos_industriales')\
        .select('id, codigo, nombre, tipo, sector, unidad, '
                'estado_actual, ultimo_valor, ultima_lectura, '
                'rango_normal_min, rango_normal_max, '
                'umbral_alerta_min, umbral_alerta_max, '
                'umbral_critico_min, umbral_critico_max, '
                'es_sensor, es_actuador')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .order('sector').execute()

    # Agrupar por sector
    por_sector = {}
    for d in (disps.data or []):
        s = d.get('sector') or 'General'
        por_sector.setdefault(s, []).append(d)

    return {
        'dispositivos':  disps.data or [],
        'por_sector':    por_sector,
        'total':         len(disps.data or []),
        'timestamp':     datetime.now(timezone.utc).isoformat()
    }


def listar_alarmas(planta_id: str) -> dict:
    if not planta_id:
        return {'error': 'planta_id requerido'}

    alarmas = spbs.table('alarmas')\
        .select('id, tipo, severidad, mensaje, estado, '
                'nivel_escalamiento, created_at, reconocida_at, '
                'dispositivos_industriales(nombre, sector)')\
        .eq('planta_id', planta_id)\
        .in_('estado', ['activa', 'reconocida'])\
        .order('created_at', desc=True)\
        .limit(50).execute()

    ahora = datetime.now(timezone.utc)
    for a in (alarmas.data or []):
        creada    = datetime.fromisoformat(a['created_at'].replace('Z', '+00:00'))
        a['edad_min'] = int((ahora - creada).total_seconds() / 60)

    return {
        'alarmas':   alarmas.data or [],
        'total':     len(alarmas.data or []),
        'timestamp': ahora.isoformat()
    }


def historial_sensor(sensor_id: str, horas: int = 24) -> dict:
    if not sensor_id:
        return {'error': 'sensor_id requerido'}

    desde = (datetime.now(timezone.utc) - timedelta(hours=horas)).isoformat()

    lecturas = spbs.table('lecturas')\
        .select('valor, unidad, timestamp, calidad')\
        .eq('dispositivo_id', sensor_id)\
        .gte('timestamp', desde)\
        .order('timestamp', desc=False)\
        .limit(1440).execute()  # máx 1 lectura/min × 24h

    datos = lecturas.data or []

    # Estadísticas básicas
    valores = [r['valor'] for r in datos if r.get('valor') is not None]
    stats   = {}
    if valores:
        stats = {
            'min':     round(min(valores), 3),
            'max':     round(max(valores), 3),
            'promedio': round(sum(valores)/len(valores), 3),
            'ultima':  valores[-1]
        }

    return {
        'sensor_id': sensor_id,
        'horas':     horas,
        'lecturas':  datos,
        'total':     len(datos),
        'stats':     stats,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }


def turno_activo(planta_id: str) -> dict:
    if not planta_id:
        return {'error': 'planta_id requerido'}

    turno = spbs.table('turnos')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .limit(1).execute()

    if not turno.data:
        return {'turno_activo': False}

    t        = turno.data[0]
    inicio   = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    dur_min  = int((datetime.now(timezone.utc) - inicio).total_seconds() / 60)

    return {
        'turno_activo':        True,
        'operador':            t['operador_nombre'],
        'inicio':              t['inicio'],
        'duracion_min':        dur_min,
        'alarmas_generadas':   t['alarmas_generadas'],
        'comandos_ejecutados': t['comandos_ejecutados']
    }
