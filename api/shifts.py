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
        params = dict(urllib.parse.parse_qsl(
            urllib.parse.urlparse(self.path).query
        ))
        planta_id = params.get('planta_id', '').strip()
        if not planta_id:
            self._send(400, {'error': 'planta_id requerido'})
            return
        self._send(200, obtener_turno_activo(planta_id))

    def do_POST(self):
        largo = int(self.headers.get('Content-Length', 0))
        body  = self.rfile.read(largo)
        path  = urllib.parse.urlparse(self.path).path.rstrip('/')

        try:
            data = json.loads(body)
        except Exception:
            self._send(400, {'error': 'JSON inválido'})
            return

        if path.endswith('/report'):
            self._send(200, generar_reporte_externo(data))
        else:
            resultado = gestionar_turno(data)
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
# GET — TURNO ACTIVO
# ═══════════════════════════════════════════════════════════════════

def obtener_turno_activo(planta_id: str) -> dict:
    turno = spbs.table('turnos')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .order('inicio', desc=True)\
        .limit(1).execute()

    if not turno.data:
        return {'turno_activo': False}

    t        = turno.data[0]
    inicio   = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    dur_min  = int((datetime.now(timezone.utc) - inicio).total_seconds() / 60)

    return {
        'turno_activo':        True,
        'turno_id':            t['id'],
        'operador':            t['operador_nombre'],
        'inicio':              t['inicio'],
        'duracion_min':        dur_min,
        'alarmas_generadas':   t['alarmas_generadas'],
        'comandos_ejecutados': t['comandos_ejecutados']
    }


# ═══════════════════════════════════════════════════════════════════
# POST — GESTIONAR TURNO
# ═══════════════════════════════════════════════════════════════════

def gestionar_turno(data: dict) -> dict:
    accion            = data.get('accion', '').strip()
    planta_id         = data.get('planta_id', '').strip()
    operador_whatsapp = data.get('operador_whatsapp', '').strip()

    if not all([accion, planta_id, operador_whatsapp]):
        return {'error': 'accion, planta_id y operador_whatsapp requeridos', 'status': 400}

    operador = spbs.table('operadores')\
        .select('id, nombre, rol, puede_comandar')\
        .eq('planta_id', planta_id)\
        .eq('whatsapp', operador_whatsapp)\
        .eq('activo', True)\
        .single().execute()

    if not operador.data:
        return {'error': 'Operador no registrado en esta planta', 'status': 403}

    if accion == 'iniciar':
        return iniciar_turno(planta_id, operador_whatsapp, operador.data, data)
    if accion == 'cerrar':
        return cerrar_turno(planta_id, operador_whatsapp, operador.data, data)

    return {'error': 'accion debe ser iniciar o cerrar', 'status': 400}


# ═══════════════════════════════════════════════════════════════════
# INICIAR TURNO
# ═══════════════════════════════════════════════════════════════════

def iniciar_turno(planta_id, operador_whatsapp, operador, data) -> dict:
    ahora = datetime.now(timezone.utc)

    existente = spbs.table('turnos')\
        .select('id, operador_nombre')\
        .eq('planta_id', planta_id)\
        .eq('activo', True).execute()

    if existente.data:
        t = existente.data[0]
        return {'error': f"Turno activo de {t['operador_nombre']} debe cerrarse primero.", 'status': 409}

    estado_inicio  = capturar_estado_planta(planta_id)
    alarmas_activas = spbs.table('alarmas').select('id', count='exact')\
        .eq('planta_id', planta_id).eq('estado', 'activa').execute()
    num_alarmas = alarmas_activas.count or 0

    turno = spbs.table('turnos').insert({
        'planta_id':           planta_id,
        'operador_whatsapp':   operador_whatsapp,
        'operador_nombre':     operador['nombre'],
        'inicio':              ahora.isoformat(),
        'estado_inicio':       estado_inicio,
        'alarmas_generadas':   0,
        'alarmas_resueltas':   0,
        'comandos_ejecutados': 0,
        'activo':              True
    }).execute()

    turno_id = turno.data[0]['id'] if turno.data else None
    reporte  = construir_reporte_inicio(estado_inicio, num_alarmas, operador['nombre'], ahora)

    return {
        'status':                    'ok',
        'accion':                    'turno_iniciado',
        'turno_id':                  turno_id,
        'operador':                  operador['nombre'],
        'inicio':                    ahora.isoformat(),
        'reporte_whatsapp':          reporte,
        'alarmas_activas_al_inicio': num_alarmas
    }


# ═══════════════════════════════════════════════════════════════════
# CERRAR TURNO
# ═══════════════════════════════════════════════════════════════════

def cerrar_turno(planta_id, operador_whatsapp, operador, data) -> dict:
    ahora     = datetime.now(timezone.utc)
    novedades = data.get('novedades', 'Sin novedades').strip()

    turno = spbs.table('turnos')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .eq('operador_whatsapp', operador_whatsapp)\
        .eq('activo', True)\
        .order('inicio', desc=True)\
        .limit(1).execute()

    if not turno.data:
        return {'error': 'No tienes un turno activo', 'status': 404}

    t            = turno.data[0]
    turno_id     = t['id']
    inicio_dt    = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    duracion_min = int((ahora - inicio_dt).total_seconds() / 60)

    estado_fin = capturar_estado_planta(planta_id)
    metricas   = calcular_metricas_turno(planta_id, inicio_dt, ahora)

    alarmas_pend = spbs.table('alarmas')\
        .select('id, mensaje, severidad')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa').execute()

    spbs.table('turnos').update({
        'fin':                 ahora.isoformat(),
        'estado_fin':          estado_fin,
        'alarmas_generadas':   metricas['alarmas_generadas'],
        'alarmas_resueltas':   metricas['alarmas_resueltas'],
        'comandos_ejecutados': metricas['comandos_ejecutados'],
        'novedades':           novedades,
        'activo':              False
    }).eq('id', turno_id).execute()

    reporte = construir_reporte_cierre(
        t, metricas, alarmas_pend.data or [], novedades, duracion_min, ahora
    )

    return {
        'status':               'ok',
        'accion':               'turno_cerrado',
        'turno_id':             turno_id,
        'duracion_min':         duracion_min,
        'metricas':             metricas,
        'alarmas_sin_resolver': len(alarmas_pend.data or []),
        'reporte_whatsapp':     reporte
    }


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def capturar_estado_planta(planta_id: str) -> dict:
    disps   = spbs.table('dispositivos_industriales')\
        .select('id, nombre, tipo, sector, estado_actual, ultimo_valor, unidad')\
        .eq('planta_id', planta_id).eq('activo', True).execute()
    alarmas = spbs.table('alarmas')\
        .select('id, tipo, severidad, mensaje')\
        .eq('planta_id', planta_id).eq('estado', 'activa').execute()
    return {
        'timestamp':    datetime.now(timezone.utc).isoformat(),
        'dispositivos': disps.data or [],
        'alarmas':      alarmas.data or [],
        'total_online': sum(1 for d in (disps.data or []) if d.get('estado_actual') == 'online')
    }


def calcular_metricas_turno(planta_id: str, inicio: datetime, fin: datetime) -> dict:
    def count(tabla, filtros):
        q = spbs.table(tabla).select('id', count='exact')
        for k, op, v in filtros:
            if op == 'eq':   q = q.eq(k, v)
            if op == 'gte':  q = q.gte(k, v)
            if op == 'lte':  q = q.lte(k, v)
        return q.execute().count or 0

    return {
        'alarmas_generadas':   count('alarmas',         [('planta_id','eq',planta_id),('created_at','gte',inicio.isoformat()),('created_at','lte',fin.isoformat())]),
        'alarmas_resueltas':   count('alarmas',         [('planta_id','eq',planta_id),('estado','eq','resuelta'),('resuelta_at','gte',inicio.isoformat()),('resuelta_at','lte',fin.isoformat())]),
        'comandos_ejecutados': count('comandos_gateway',[('estado','eq','ejecutado'),('ejecutado_at','gte',inicio.isoformat()),('ejecutado_at','lte',fin.isoformat())]),
        'lecturas_recibidas':  count('lecturas',        [('planta_id','eq',planta_id),('timestamp','gte',inicio.isoformat()),('timestamp','lte',fin.isoformat())])
    }


def construir_reporte_inicio(estado, num_alarmas, nombre_op, ahora) -> str:
    hora    = ahora.strftime('%H:%M')
    total   = len(estado.get('dispositivos', []))
    online  = estado.get('total_online', 0)
    disps   = estado.get('dispositivos', [])

    lineas = []
    for d in disps[:8]:
        val  = d.get('ultimo_valor')
        unid = d.get('unidad', '')
        est  = d.get('estado_actual', '')
        if est == 'online' and val is not None:
            lineas.append(f"✅ {d['nombre']}: {val}{unid}")
        elif est == 'error_comm':
            lineas.append(f"🔴 {d['nombre']}: Sin señal")
        else:
            lineas.append(f"⚪ {d['nombre']}: {est}")

    if len(disps) > 8:
        lineas.append(f"... y {len(disps)-8} más")

    alarmas_str = (
        f"⚠️ {num_alarmas} alarma(s) activa(s) — escribe *alarmas*"
        if num_alarmas > 0 else "✅ Sin alarmas activas"
    )

    return (
        f"✅ *Turno iniciado — {hora}*\n"
        f"Operador: {nombre_op}\n\n"
        f"📊 Estado al inicio ({online}/{total} en línea):\n"
        + '\n'.join(lineas) +
        f"\n\n{alarmas_str}\n\n"
        f"Escribe *ayuda* para ver comandos."
    )


def construir_reporte_cierre(turno, metricas, alarmas_pend, novedades, duracion_min, ahora) -> str:
    h_ini    = datetime.fromisoformat(turno['inicio'].replace('Z', '+00:00')).strftime('%H:%M')
    h_fin    = ahora.strftime('%H:%M')
    horas    = duracion_min // 60
    mins     = duracion_min % 60
    dur_str  = f"{horas}h {mins}min" if horas > 0 else f"{mins} min"
    pend     = len(alarmas_pend)
    gen      = metricas['alarmas_generadas']
    res      = metricas['alarmas_resueltas']

    if gen == 0:
        res_str = "✅ Sin alarmas durante el turno"
    elif pend == 0:
        res_str = f"✅ {gen} alarma(s) — todas resueltas"
    else:
        res_str = f"⚠️ {gen} generadas, {res} resueltas, *{pend} pendiente(s)*"

    detalle = ''
    if alarmas_pend:
        items = [
            f"  {'🔴' if a['severidad']=='critico' else '⚠️'} {a['mensaje'][:55]}"
            for a in alarmas_pend[:3]
        ]
        detalle = '\nAlarmas pendientes:\n' + '\n'.join(items)

    return (
        f"📋 *Reporte de turno*\n"
        f"{h_ini} — {h_fin} ({dur_str})\n"
        f"Operador: {turno['operador_nombre']}\n\n"
        f"RESUMEN:\n{res_str}{detalle}\n"
        f"🔧 Comandos: {metricas['comandos_ejecutados']}\n"
        f"📡 Lecturas: {metricas['lecturas_recibidas']}\n\n"
        f"NOVEDADES:\n{novedades}"
    )


def generar_reporte_externo(data: dict) -> dict:
    turno_id = data.get('turno_id', '').strip()
    if not turno_id:
        return {'error': 'turno_id requerido', 'status': 400}

    turno = spbs.table('turnos').select('*').eq('id', turno_id).single().execute()
    if not turno.data:
        return {'error': 'Turno no encontrado', 'status': 404}

    t        = turno.data
    inicio   = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    fin      = datetime.fromisoformat(t['fin'].replace('Z', '+00:00')) if t.get('fin') else datetime.now(timezone.utc)

    alarmas = spbs.table('alarmas')\
        .select('tipo, severidad, mensaje, estado, created_at, resuelta_at')\
        .eq('planta_id', t['planta_id'])\
        .gte('created_at', inicio.isoformat())\
        .lte('created_at', fin.isoformat())\
        .order('created_at').execute()

    return {
        'turno':           t,
        'alarmas_detalle': alarmas.data or [],
        'duracion_min':    int((fin - inicio).total_seconds() / 60)
    }
