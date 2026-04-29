import os
import json
from datetime import datetime, timezone, timedelta
from supabase import create_client

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)


# ═══════════════════════════════════════════════════════════════════
# HANDLER PRINCIPAL
#
# GET  /api/shifts?planta_id=X    → estado del turno activo
# POST /api/shifts                → iniciar o cerrar turno
# POST /api/shifts/report         → generar reporte de turno
# ═══════════════════════════════════════════════════════════════════

def handler(request):
    path = request.path.rstrip('/')

    if path.endswith('/report'):
        return generar_reporte_externo(request)

    if request.method == 'GET':
        return obtener_turno_activo(request)

    if request.method == 'POST':
        return gestionar_turno(request)

    return response_json({'error': 'Método no soportado'}, 405)


# ═══════════════════════════════════════════════════════════════════
# GET — ESTADO DEL TURNO ACTIVO
# ═══════════════════════════════════════════════════════════════════

def obtener_turno_activo(request):
    planta_id = request.args.get('planta_id', '').strip()
    if not planta_id:
        return response_json({'error': 'planta_id requerido'}, 400)

    turno = supabase.table('turnos')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .order('inicio', desc=True)\
        .limit(1)\
        .execute()

    if not turno.data:
        return response_json({'turno_activo': False})

    t = turno.data[0]
    inicio = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    duracion_min = int(
        (datetime.now(timezone.utc) - inicio).total_seconds() / 60
    )

    return response_json({
        'turno_activo': True,
        'turno_id':     t['id'],
        'operador':     t['operador_nombre'],
        'inicio':       t['inicio'],
        'duracion_min': duracion_min,
        'alarmas_generadas': t['alarmas_generadas'],
        'comandos_ejecutados': t['comandos_ejecutados']
    })


# ═══════════════════════════════════════════════════════════════════
# POST — INICIAR O CERRAR TURNO
# ═══════════════════════════════════════════════════════════════════

def gestionar_turno(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return response_json({'error': 'JSON inválido'}, 400)

    accion    = data.get('accion', '').strip()      # 'iniciar' | 'cerrar'
    planta_id = data.get('planta_id', '').strip()
    operador_whatsapp = data.get('operador_whatsapp', '').strip()

    if not all([accion, planta_id, operador_whatsapp]):
        return response_json({'error': 'accion, planta_id y operador_whatsapp requeridos'}, 400)

    # Verificar que el operador existe y pertenece a la planta
    operador = supabase.table('operadores')\
        .select('id, nombre, rol, puede_comandar')\
        .eq('planta_id', planta_id)\
        .eq('whatsapp', operador_whatsapp)\
        .eq('activo', True)\
        .single()\
        .execute()

    if not operador.data:
        return response_json({'error': 'Operador no registrado en esta planta'}, 403)

    if accion == 'iniciar':
        return iniciar_turno(planta_id, operador_whatsapp, operador.data, data)

    if accion == 'cerrar':
        return cerrar_turno(planta_id, operador_whatsapp, operador.data, data)

    return response_json({'error': 'accion debe ser iniciar o cerrar'}, 400)


# ═══════════════════════════════════════════════════════════════════
# INICIAR TURNO
# ═══════════════════════════════════════════════════════════════════

def iniciar_turno(
    planta_id: str,
    operador_whatsapp: str,
    operador: dict,
    data: dict
) -> dict:
    ahora = datetime.now(timezone.utc)

    # Verificar si ya hay turno activo
    turno_existente = supabase.table('turnos')\
        .select('id, operador_nombre')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .execute()

    if turno_existente.data:
        t = turno_existente.data[0]
        return response_json({
            'error': (
                f"Ya hay un turno activo iniciado por "
                f"{t['operador_nombre']}. "
                f"Debe cerrarse antes de iniciar otro."
            )
        }, 409)

    # Capturar snapshot del estado actual de todos los dispositivos
    estado_inicio = capturar_estado_planta(planta_id)

    # Contar alarmas activas al inicio
    alarmas_activas = supabase.table('alarmas')\
        .select('id', count='exact')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .execute()

    num_alarmas = alarmas_activas.count or 0

    # Crear registro de turno
    turno = supabase.table('turnos').insert({
        'planta_id':          planta_id,
        'operador_whatsapp':  operador_whatsapp,
        'operador_nombre':    operador['nombre'],
        'inicio':             ahora.isoformat(),
        'estado_inicio':      estado_inicio,
        'alarmas_generadas':  0,
        'alarmas_resueltas':  0,
        'comandos_ejecutados': 0,
        'activo':             True
    }).execute()

    turno_id = turno.data[0]['id'] if turno.data else None

    # Construir reporte de inicio para WhatsApp
    reporte = construir_reporte_inicio(
        estado_inicio, num_alarmas, operador['nombre'], ahora
    )

    return response_json({
        'status':    'ok',
        'accion':    'turno_iniciado',
        'turno_id':  turno_id,
        'operador':  operador['nombre'],
        'inicio':    ahora.isoformat(),
        'reporte_whatsapp': reporte,
        'alarmas_activas_al_inicio': num_alarmas
    })


# ═══════════════════════════════════════════════════════════════════
# CERRAR TURNO
# ═══════════════════════════════════════════════════════════════════

def cerrar_turno(
    planta_id: str,
    operador_whatsapp: str,
    operador: dict,
    data: dict
) -> dict:
    ahora    = datetime.now(timezone.utc)
    novedades = data.get('novedades', 'Sin novedades').strip()

    # Obtener turno activo del operador
    turno = supabase.table('turnos')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .eq('operador_whatsapp', operador_whatsapp)\
        .eq('activo', True)\
        .order('inicio', desc=True)\
        .limit(1)\
        .execute()

    if not turno.data:
        return response_json({
            'error': 'No tienes un turno activo en esta planta'
        }, 404)

    t         = turno.data[0]
    turno_id  = t['id']
    inicio_dt = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    duracion_min = int((ahora - inicio_dt).total_seconds() / 60)

    # Capturar estado final
    estado_fin = capturar_estado_planta(planta_id)

    # Métricas del turno
    metricas = calcular_metricas_turno(planta_id, inicio_dt, ahora)

    # Verificar alarmas sin resolver
    alarmas_pendientes = supabase.table('alarmas')\
        .select('id, mensaje, severidad')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .execute()

    # Cerrar turno
    supabase.table('turnos').update({
        'fin':                ahora.isoformat(),
        'estado_fin':         estado_fin,
        'alarmas_generadas':  metricas['alarmas_generadas'],
        'alarmas_resueltas':  metricas['alarmas_resueltas'],
        'comandos_ejecutados': metricas['comandos_ejecutados'],
        'novedades':          novedades,
        'activo':             False
    }).eq('id', turno_id).execute()

    # Construir reporte de cierre
    reporte = construir_reporte_cierre(
        t, metricas, estado_fin, novedades,
        alarmas_pendientes.data or [],
        duracion_min, ahora
    )

    return response_json({
        'status':     'ok',
        'accion':     'turno_cerrado',
        'turno_id':   turno_id,
        'duracion_min': duracion_min,
        'metricas':   metricas,
        'alarmas_sin_resolver': len(alarmas_pendientes.data or []),
        'reporte_whatsapp': reporte
    })


# ═══════════════════════════════════════════════════════════════════
# CAPTURAR ESTADO ACTUAL DE LA PLANTA
# ═══════════════════════════════════════════════════════════════════

def capturar_estado_planta(planta_id: str) -> dict:
    dispositivos = supabase.table('dispositivos_industriales')\
        .select('id, nombre, tipo, sector, estado_actual, ultimo_valor, unidad')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .execute()

    alarmas = supabase.table('alarmas')\
        .select('id, tipo, severidad, mensaje')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .execute()

    return {
        'timestamp':    datetime.now(timezone.utc).isoformat(),
        'dispositivos': dispositivos.data or [],
        'alarmas':      alarmas.data or [],
        'total_online': sum(
            1 for d in (dispositivos.data or [])
            if d.get('estado_actual') == 'online'
        )
    }


# ═══════════════════════════════════════════════════════════════════
# CALCULAR MÉTRICAS DEL TURNO
# ═══════════════════════════════════════════════════════════════════

def calcular_metricas_turno(
    planta_id: str,
    inicio: datetime,
    fin: datetime
) -> dict:
    # Alarmas generadas durante el turno
    alarmas_gen = supabase.table('alarmas')\
        .select('id', count='exact')\
        .eq('planta_id', planta_id)\
        .gte('created_at', inicio.isoformat())\
        .lte('created_at', fin.isoformat())\
        .execute()

    # Alarmas resueltas durante el turno
    alarmas_res = supabase.table('alarmas')\
        .select('id', count='exact')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'resuelta')\
        .gte('resuelta_at', inicio.isoformat())\
        .lte('resuelta_at', fin.isoformat())\
        .execute()

    # Comandos ejecutados durante el turno
    comandos = supabase.table('comandos_gateway')\
        .select('id', count='exact')\
        .eq('estado', 'ejecutado')\
        .gte('ejecutado_at', inicio.isoformat())\
        .lte('ejecutado_at', fin.isoformat())\
        .execute()

    # Lecturas recibidas (proxy de salud del sistema)
    lecturas = supabase.table('lecturas')\
        .select('id', count='exact')\
        .eq('planta_id', planta_id)\
        .gte('timestamp', inicio.isoformat())\
        .lte('timestamp', fin.isoformat())\
        .execute()

    return {
        'alarmas_generadas':   alarmas_gen.count or 0,
        'alarmas_resueltas':   alarmas_res.count or 0,
        'comandos_ejecutados': comandos.count or 0,
        'lecturas_recibidas':  lecturas.count or 0
    }


# ═══════════════════════════════════════════════════════════════════
# CONSTRUIR REPORTE DE INICIO (texto WhatsApp)
# ═══════════════════════════════════════════════════════════════════

def construir_reporte_inicio(
    estado: dict,
    num_alarmas: int,
    nombre_operador: str,
    ahora: datetime
) -> str:
    hora = ahora.strftime('%H:%M')
    total = len(estado.get('dispositivos', []))
    online = estado.get('total_online', 0)

    dispositivos = estado.get('dispositivos', [])

    # Clasificar dispositivos por estado
    con_valor = [
        d for d in dispositivos
        if d.get('ultimo_valor') is not None
    ]

    # Construir lista de estado resumida (máx 8 dispositivos)
    lineas_estado = []
    for d in dispositivos[:8]:
        val = d.get('ultimo_valor')
        unidad = d.get('unidad', '')
        estado_disp = d.get('estado_actual', 'desconocido')

        if estado_disp == 'online' and val is not None:
            icono = '✅'
            val_str = f"{val}{unidad}"
        elif estado_disp == 'error_comm':
            icono = '🔴'
            val_str = 'Sin señal'
        else:
            icono = '⚪'
            val_str = estado_disp

        lineas_estado.append(
            f"{icono} {d['nombre']}: {val_str}"
        )

    estado_str = '\n'.join(lineas_estado)
    if len(dispositivos) > 8:
        estado_str += f'\n... y {len(dispositivos)-8} dispositivos más'

    alarmas_str = (
        f"⚠️ {num_alarmas} alarma(s) activa(s) — escribe *alarmas* para ver"
        if num_alarmas > 0
        else "✅ Sin alarmas activas"
    )

    return (
        f"✅ *Turno iniciado — {hora}*\n"
        f"Operador: {nombre_operador}\n\n"
        f"📊 Estado al inicio ({online}/{total} en línea):\n"
        f"{estado_str}\n\n"
        f"{alarmas_str}\n\n"
        f"Escribe *ayuda* para ver comandos disponibles."
    )


# ═══════════════════════════════════════════════════════════════════
# CONSTRUIR REPORTE DE CIERRE (texto WhatsApp)
# ═══════════════════════════════════════════════════════════════════

def construir_reporte_cierre(
    turno: dict,
    metricas: dict,
    estado_fin: dict,
    novedades: str,
    alarmas_pendientes: list,
    duracion_min: int,
    ahora: datetime
) -> str:
    hora_inicio = datetime.fromisoformat(
        turno['inicio'].replace('Z', '+00:00')
    ).strftime('%H:%M')
    hora_fin = ahora.strftime('%H:%M')

    horas   = duracion_min // 60
    minutos = duracion_min % 60
    dur_str = f"{horas}h {minutos}min" if horas > 0 else f"{minutos} min"

    # Evaluar si el turno fue limpio
    alarmas_gen = metricas['alarmas_generadas']
    alarmas_res = metricas['alarmas_resueltas']
    pendientes  = len(alarmas_pendientes)

    if alarmas_gen == 0:
        resumen_alarmas = "✅ Sin alarmas durante el turno"
    elif pendientes == 0:
        resumen_alarmas = (
            f"✅ {alarmas_gen} alarma(s) — todas resueltas"
        )
    else:
        resumen_alarmas = (
            f"⚠️ {alarmas_gen} alarma(s) generadas, "
            f"{alarmas_res} resueltas, "
            f"*{pendientes} pendiente(s)*"
        )

    # Alarmas pendientes detalladas
    detalle_pendientes = ''
    if alarmas_pendientes:
        items = []
        for a in alarmas_pendientes[:3]:
            sev = '🔴' if a['severidad'] == 'critico' else '⚠️'
            items.append(f"  {sev} {a['mensaje'][:60]}")
        detalle_pendientes = (
            '\nAlarmas sin resolver:\n' + '\n'.join(items)
        )
        if len(alarmas_pendientes) > 3:
            detalle_pendientes += (
                f'\n  ... y {len(alarmas_pendientes)-3} más'
            )

    return (
        f"📋 *Reporte de turno*\n"
        f"{hora_inicio} — {hora_fin} ({dur_str})\n"
        f"Operador: {turno['operador_nombre']}\n\n"
        f"RESUMEN:\n"
        f"{resumen_alarmas}"
        f"{detalle_pendientes}\n"
        f"🔧 Comandos ejecutados: {metricas['comandos_ejecutados']}\n"
        f"📡 Lecturas recibidas: {metricas['lecturas_recibidas']}\n\n"
        f"NOVEDADES:\n{novedades}"
    )


# ═══════════════════════════════════════════════════════════════════
# GENERAR REPORTE EXTERNO (para dashboard o PDF)
# ═══════════════════════════════════════════════════════════════════

def generar_reporte_externo(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return response_json({'error': 'JSON inválido'}, 400)

    turno_id = data.get('turno_id', '').strip()
    if not turno_id:
        return response_json({'error': 'turno_id requerido'}, 400)

    turno = supabase.table('turnos')\
        .select('*')\
        .eq('id', turno_id)\
        .single()\
        .execute()

    if not turno.data:
        return response_json({'error': 'Turno no encontrado'}, 404)

    t         = turno.data
    inicio_dt = datetime.fromisoformat(t['inicio'].replace('Z', '+00:00'))
    fin_dt    = datetime.fromisoformat(t['fin'].replace('Z', '+00:00')) \
                if t.get('fin') else datetime.now(timezone.utc)

    # Obtener alarmas del turno
    alarmas = supabase.table('alarmas')\
        .select('tipo, severidad, mensaje, estado, created_at, resuelta_at')\
        .eq('planta_id', t['planta_id'])\
        .gte('created_at', inicio_dt.isoformat())\
        .lte('created_at', fin_dt.isoformat())\
        .order('created_at')\
        .execute()

    return response_json({
        'turno': t,
        'alarmas_detalle': alarmas.data or [],
        'duracion_min': int((fin_dt - inicio_dt).total_seconds() / 60)
    })


# ═══════════════════════════════════════════════════════════════════
# HELPER
# ═══════════════════════════════════════════════════════════════════

def response_json(data: dict, status: int = 200):
    return {
        'statusCode': status,
        'headers':    {'Content-Type': 'application/json'},
        'body':       json.dumps(data, ensure_ascii=False, default=str)
    }
