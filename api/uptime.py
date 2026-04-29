import os
import json
import re
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from supabase import create_client

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

GROQ_API_KEY  = os.environ.get('GROQ_API_KEY', '')
GROQ_URL      = 'https://api.groq.com/openai/v1/chat/completions'
TWILIO_SID    = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN  = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM   = os.environ.get('TWILIO_WHATSAPP_FROM', '')


# ═══════════════════════════════════════════════════════════════════
# HANDLER PRINCIPAL — recibe webhooks de Twilio/Meta WhatsApp
# ═══════════════════════════════════════════════════════════════════

def handler(request):
    if request.method != 'POST':
        return response_twiml('<Response></Response>')

    # Parsear form-data de Twilio
    body = request.body
    if isinstance(body, bytes):
        body = body.decode('utf-8')
    params = dict(urllib.parse.parse_qsl(body))

    numero_raw = params.get('From', '')
    mensaje    = params.get('Body', '').strip()
    media_url  = params.get('MediaUrl0', '')

    # Normalizar número (quitar prefijo whatsapp:)
    numero = numero_raw.replace('whatsapp:', '').strip()
    if not numero.startswith('+'):
        numero = '+' + numero

    if not numero or not mensaje:
        return response_twiml('<Response></Response>')

    # Transcribir audio si viene nota de voz
    if media_url and params.get('MediaContentType0', '').startswith('audio'):
        mensaje = transcribir_audio(media_url) or mensaje

    # Procesar el mensaje y obtener respuesta
    respuesta = procesar_mensaje(numero, mensaje.lower().strip())

    # Responder vía Twilio TwiML
    respuesta_safe = respuesta.replace('&', '&amp;').replace('<', '&lt;')
    return response_twiml(
        f'<Response><Message>{respuesta_safe}</Message></Response>'
    )


# ═══════════════════════════════════════════════════════════════════
# PROCESADOR CENTRAL DE MENSAJES
# ═══════════════════════════════════════════════════════════════════

def procesar_mensaje(numero: str, mensaje: str) -> str:
    # Identificar operador y planta
    operador = identificar_operador(numero)

    # Si no está registrado, flujo de registro
    if not operador:
        return flujo_registro(numero, mensaje)

    planta_id  = operador['planta_id']
    planta     = obtener_planta(planta_id)

    # Verificar palabra clave si la planta la requiere
    if planta and planta.get('palabra_clave'):
        if not verificar_sesion_activa(numero):
            if mensaje == planta['palabra_clave'].lower():
                activar_sesion(numero, planta_id)
                return (
                    f"✅ *UPTIME IA Industrial*\n"
                    f"Bienvenido, {operador['nombre']}.\n"
                    f"Planta: {planta['nombre']}\n\n"
                    f"Escribe *ayuda* para ver comandos."
                )
            else:
                return (
                    "🔒 Planta protegida.\n"
                    "Escribe la palabra clave para acceder."
                )

    # Enrutar intención
    return enrutar_intencion(mensaje, operador, planta_id, planta)


# ═══════════════════════════════════════════════════════════════════
# ENRUTADOR DE INTENCIONES
# ═══════════════════════════════════════════════════════════════════

def enrutar_intencion(
    mensaje: str,
    operador: dict,
    planta_id: str,
    planta: dict
) -> str:
    m = mensaje.lower().strip()

    # ── TURNO ──────────────────────────────────────────────────────
    if any(x in m for x in ['inicio de turno', 'iniciar turno', 'entro de turno']):
        return cmd_iniciar_turno(operador, planta_id)

    if any(x in m for x in ['fin de turno', 'finalizar turno', 'cerrar turno', 'salgo de turno']):
        return cmd_cerrar_turno(operador, planta_id, m)

    # ── ESTADO GENERAL ─────────────────────────────────────────────
    if any(x in m for x in ['estado', 'cómo está todo', 'como esta todo', 'reporte', 'status']):
        if 'turno' not in m:
            return cmd_estado_general(planta_id)

    # ── ALARMAS ────────────────────────────────────────────────────
    if m.startswith('reconocer ') or m.startswith('ack '):
        partes = m.split()
        if len(partes) >= 2:
            return cmd_reconocer_alarma(partes[-1], operador, planta_id)

    if any(x in m for x in ['alarmas', 'alertas', 'alarma']):
        return cmd_listar_alarmas(planta_id)

    # ── CONSULTA DE DISPOSITIVO ────────────────────────────────────
    if any(x in m for x in ['cómo está', 'como esta', 'estado de', 'valor de']):
        nombre_disp = extraer_nombre_dispositivo(m)
        if nombre_disp:
            return cmd_consultar_dispositivo(nombre_disp, planta_id)

    # ── COMANDOS DE CONTROL ────────────────────────────────────────
    if any(x in m for x in ['encender', 'apagar', 'abrir', 'cerrar', 'activar', 'desactivar']):
        return cmd_control_dispositivo(m, operador, planta_id)

    if any(x in m for x in ['iniciar riego', 'regar', 'riego zona']):
        return cmd_iniciar_riego(m, operador, planta_id)

    # ── DIAGNÓSTICO IA ─────────────────────────────────────────────
    if any(x in m for x in ['diagnóstico', 'diagnostico', 'analiza', 'por qué', 'por que', 'qué pasó', 'que paso']):
        return cmd_diagnostico_ia(m, planta_id)

    # ── AYUDA ──────────────────────────────────────────────────────
    if any(x in m for x in ['ayuda', 'help', 'comandos', '?']):
        return cmd_ayuda(operador)

    # ── SIN NOVEDADES (para cierre de turno) ──────────────────────
    if 'sin novedades' in m:
        return cmd_cerrar_turno(operador, planta_id, m, novedades='Sin novedades')

    # ── FALLBACK: LLM con contexto de planta ──────────────────────
    return cmd_fallback_llm(mensaje, operador, planta_id, planta)


# ═══════════════════════════════════════════════════════════════════
# COMANDOS
# ═══════════════════════════════════════════════════════════════════

def cmd_iniciar_turno(operador: dict, planta_id: str) -> str:
    import urllib.request as urlr
    payload = json.dumps({
        'accion':             'iniciar',
        'planta_id':          planta_id,
        'operador_whatsapp':  operador['whatsapp']
    }).encode()

    req = urlr.Request(
        f"{os.environ.get('VERCEL_URL','')}/api/shifts",
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    try:
        with urlr.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
            return data.get('reporte_whatsapp', '✅ Turno iniciado.')
    except Exception:
        return "❌ Error al iniciar turno. Intenta de nuevo."


def cmd_cerrar_turno(
    operador: dict,
    planta_id: str,
    mensaje: str,
    novedades: str = ''
) -> str:
    # Si no se pasó novedades, preguntar primero
    if not novedades:
        # Verificar si el mensaje incluye novedades inline
        if 'fin de turno:' in mensaje:
            novedades = mensaje.split('fin de turno:', 1)[1].strip()
        elif 'fin de turno' == mensaje.strip():
            return (
                "📝 Escribe las novedades del turno o responde:\n"
                "*sin novedades* para cerrar sin novedad."
            )

    import urllib.request as urlr
    payload = json.dumps({
        'accion':             'cerrar',
        'planta_id':          planta_id,
        'operador_whatsapp':  operador['whatsapp'],
        'novedades':          novedades or 'Sin novedades'
    }).encode()

    req = urlr.Request(
        f"{os.environ.get('VERCEL_URL','')}/api/shifts",
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    try:
        with urlr.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
            return data.get('reporte_whatsapp', '✅ Turno cerrado.')
    except Exception:
        return "❌ Error al cerrar turno. Intenta de nuevo."


def cmd_estado_general(planta_id: str) -> str:
    dispositivos = supabase.table('dispositivos_industriales')\
        .select('nombre, tipo, sector, estado_actual, ultimo_valor, unidad')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .order('sector')\
        .execute()

    alarmas = supabase.table('alarmas')\
        .select('severidad, mensaje')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .order('severidad')\
        .execute()

    if not dispositivos.data:
        return "⚪ No hay dispositivos registrados en esta planta."

    total   = len(dispositivos.data)
    online  = sum(1 for d in dispositivos.data if d.get('estado_actual') == 'online')
    errores = sum(1 for d in dispositivos.data if d.get('estado_actual') == 'error_comm')

    # Agrupar por sector
    por_sector = {}
    for d in dispositivos.data:
        s = d.get('sector') or 'General'
        por_sector.setdefault(s, []).append(d)

    lineas = [f"📊 *Estado general — {datetime.now(timezone.utc).strftime('%H:%M')}*\n"]
    lineas.append(f"Dispositivos: {online}/{total} en línea"
                  + (f" · {errores} sin señal" if errores else "") + "\n")

    for sector, disps in list(por_sector.items())[:4]:
        lineas.append(f"\n*{sector}*")
        for d in disps[:4]:
            val  = d.get('ultimo_valor')
            unid = d.get('unidad', '')
            est  = d.get('estado_actual', '')
            if est == 'online' and val is not None:
                icono = '✅'
                val_str = f"{val}{unid}"
            elif est == 'error_comm':
                icono, val_str = '🔴', 'Sin señal'
            else:
                icono, val_str = '⚪', est
            lineas.append(f"{icono} {d['nombre']}: {val_str}")

    if alarmas.data:
        lineas.append(f"\n⚠️ *{len(alarmas.data)} alarma(s) activa(s)*")
        for a in alarmas.data[:2]:
            sev = '🔴' if a['severidad'] == 'critico' else '⚠️'
            lineas.append(f"{sev} {a['mensaje'][:55]}...")
        if len(alarmas.data) > 2:
            lineas.append(f"... escribe *alarmas* para ver todas")
    else:
        lineas.append("\n✅ Sin alarmas activas")

    return '\n'.join(lineas)


def cmd_listar_alarmas(planta_id: str) -> str:
    alarmas = supabase.table('alarmas')\
        .select('id, tipo, severidad, mensaje, created_at, nivel_escalamiento')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .order('severidad')\
        .execute()

    if not alarmas.data:
        return "✅ No hay alarmas activas en este momento."

    ahora  = datetime.now(timezone.utc)
    lineas = [f"🚨 *{len(alarmas.data)} alarma(s) activa(s)*\n"]

    for a in alarmas.data[:5]:
        creada = datetime.fromisoformat(a['created_at'].replace('Z', '+00:00'))
        min_act = int((ahora - creada).total_seconds() / 60)
        sev     = '🔴 CRÍTICO' if a['severidad'] == 'critico' else '⚠️ ALERTA'
        id_corto = a['id'][:8]

        lineas.append(
            f"{sev} · hace {min_act}min\n"
            f"{a['mensaje'][:70]}\n"
            f"Para reconocer: *reconocer {id_corto}*\n"
        )

    if len(alarmas.data) > 5:
        lineas.append(f"... y {len(alarmas.data)-5} más")

    return '\n'.join(lineas)


def cmd_reconocer_alarma(id_parcial: str, operador: dict, planta_id: str) -> str:
    # Buscar alarma por ID parcial (primeros 8 caracteres)
    alarmas = supabase.table('alarmas')\
        .select('id, mensaje, severidad')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .execute()

    alarma = next(
        (a for a in (alarmas.data or [])
         if a['id'].startswith(id_parcial)),
        None
    )

    if not alarma:
        return (
            f"❌ No encontré alarma activa con ID *{id_parcial}*\n"
            f"Escribe *alarmas* para ver la lista."
        )

    supabase.table('alarmas').update({
        'estado':          'reconocida',
        'reconocida_por':  operador['whatsapp'],
        'reconocida_at':   datetime.now(timezone.utc).isoformat()
    }).eq('id', alarma['id']).execute()

    return (
        f"✅ Alarma reconocida por {operador['nombre']}\n"
        f"{alarma['mensaje'][:80]}\n\n"
        f"Recuerda resolverla y escribe *resuelta {id_parcial}* cuando esté lista."
    )


def cmd_consultar_dispositivo(nombre_busqueda: str, planta_id: str) -> str:
    # Búsqueda flexible por nombre
    dispositivos = supabase.table('dispositivos_industriales')\
        .select('*')\
        .eq('planta_id', planta_id)\
        .ilike('nombre', f'%{nombre_busqueda}%')\
        .limit(3)\
        .execute()

    if not dispositivos.data:
        return (
            f"❌ No encontré dispositivo con nombre *{nombre_busqueda}*\n"
            f"Escribe *estado* para ver todos los dispositivos."
        )

    d    = dispositivos.data[0]
    val  = d.get('ultimo_valor')
    unid = d.get('unidad', '')
    est  = d.get('estado_actual', 'desconocido')

    # Obtener historial de 1 hora
    hace_1h = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    hist = supabase.table('lecturas')\
        .select('valor, timestamp')\
        .eq('dispositivo_id', d['id'])\
        .gte('timestamp', hace_1h)\
        .order('timestamp')\
        .execute()

    tendencia = calcular_tendencia_simple(hist.data or [])
    ultima_lect = d.get('ultima_lectura', '')

    # Verificar alarmas activas de este dispositivo
    alarma_activa = supabase.table('alarmas')\
        .select('severidad, mensaje')\
        .eq('dispositivo_id', d['id'])\
        .eq('estado', 'activa')\
        .execute()

    lineas = [f"🔧 *{d['nombre']}*"]
    lineas.append(f"Sector: {d.get('sector','—')} · {d.get('tipo','').replace('_',' ')}")

    if val is not None:
        rango = ''
        if d.get('rango_normal_min') is not None:
            rango = f" (normal: {d['rango_normal_min']}–{d['rango_normal_max']}{unid})"
        lineas.append(f"Valor actual: *{val}{unid}*{rango}")
    else:
        lineas.append(f"Estado: {est}")

    lineas.append(f"Tendencia 1h: {tendencia}")

    if ultima_lect:
        ultima_dt = datetime.fromisoformat(ultima_lect.replace('Z', '+00:00'))
        min_ago = int((datetime.now(timezone.utc) - ultima_dt).total_seconds() / 60)
        lineas.append(f"Última lectura: hace {min_ago} min")

    if alarma_activa.data:
        for a in alarma_activa.data:
            sev = '🔴' if a['severidad'] == 'critico' else '⚠️'
            lineas.append(f"{sev} {a['mensaje'][:60]}")

    return '\n'.join(lineas)


def cmd_control_dispositivo(mensaje: str, operador: dict, planta_id: str) -> str:
    # Verificar permisos
    if not operador.get('puede_comandar', False):
        return "❌ No tienes permisos para enviar comandos en esta planta."

    # Extraer acción y nombre de dispositivo
    acciones = {
        'encender': 'on', 'activar': 'on',
        'apagar': 'off', 'desactivar': 'off',
        'abrir': 'open', 'cerrar': 'close'
    }

    accion_detectada = None
    for palabra, accion in acciones.items():
        if palabra in mensaje:
            accion_detectada = accion
            break

    if not accion_detectada:
        return "❌ No entendí el comando. Ejemplo: *encender bomba 2*"

    nombre_disp = extraer_nombre_dispositivo_post_verbo(mensaje)
    if not nombre_disp:
        return "❌ Especifica el dispositivo. Ejemplo: *encender bomba principal*"

    dispositivo = supabase.table('dispositivos_industriales')\
        .select('id, nombre, permite_comando_remoto, requiere_confirmacion, tipo')\
        .eq('planta_id', planta_id)\
        .ilike('nombre', f'%{nombre_disp}%')\
        .eq('es_actuador', True)\
        .limit(1)\
        .execute()

    if not dispositivo.data:
        return (
            f"❌ No encontré actuador con nombre *{nombre_disp}*\n"
            f"Verifica el nombre con *estado*."
        )

    d = dispositivo.data[0]

    if not d.get('permite_comando_remoto', True):
        return (
            f"❌ {d['nombre']} no permite control remoto.\n"
            f"Debe operarse localmente en tablero."
        )

    # Si requiere confirmación, pedir primero
    if d.get('requiere_confirmacion', False):
        # Guardar comando pendiente en sesión temporal
        guardar_comando_pendiente(
            operador['whatsapp'], planta_id,
            d['id'], d['nombre'], accion_detectada
        )
        return (
            f"⚠️ *Confirmación requerida:*\n"
            f"{accion_detectada.upper()} → {d['nombre']}\n\n"
            f"Responde *confirmar* para ejecutar\n"
            f"o *cancelar* para anular."
        )

    # Ejecutar directamente
    return ejecutar_comando_gateway(
        planta_id, d['id'], d['nombre'], accion_detectada
    )


def cmd_iniciar_riego(mensaje: str, operador: dict, planta_id: str) -> str:
    if not operador.get('puede_comandar', False):
        return "❌ No tienes permisos de comando."

    # Extraer zona y duración
    # Patrones: "riego zona a 30 minutos", "iniciar riego zona b por 45 min"
    zona    = re.search(r'zona\s+([a-z0-9]+)', mensaje)
    duracion = re.search(r'(\d+)\s*(min|minutos|horas?)', mensaje)

    if not zona:
        return (
            "❌ Especifica la zona. Ejemplo:\n"
            "*iniciar riego zona A por 30 minutos*"
        )

    zona_nombre = zona.group(1).upper()
    minutos     = 30  # default

    if duracion:
        cantidad = int(duracion.group(1))
        if 'hora' in duracion.group(2):
            minutos = cantidad * 60
        else:
            minutos = cantidad

    # Buscar electroválvula de la zona
    valvula = supabase.table('dispositivos_industriales')\
        .select('id, nombre, tipo')\
        .eq('planta_id', planta_id)\
        .ilike('sector', f'%{zona_nombre}%')\
        .in_('tipo', ['electrovalvula_riego', 'valvula_solenoide'])\
        .limit(1)\
        .execute()

    if not valvula.data:
        return (
            f"❌ No encontré válvula de riego en Zona {zona_nombre}.\n"
            f"Verifica el nombre de la zona con *estado*."
        )

    v = valvula.data[0]

    # Guardar confirmación pendiente
    guardar_comando_pendiente(
        operador['whatsapp'], planta_id,
        v['id'], v['nombre'], 'open',
        duracion_min=minutos
    )

    return (
        f"⚠️ *Confirmación requerida:*\n"
        f"Iniciar riego Zona {zona_nombre} durante {minutos} minutos\n"
        f"Válvula: {v['nombre']}\n\n"
        f"Responde *confirmar* para ejecutar\n"
        f"o *cancelar* para anular."
    )


def cmd_diagnostico_ia(mensaje: str, planta_id: str) -> str:
    # Construir contexto de planta para el LLM
    dispositivos = supabase.table('dispositivos_industriales')\
        .select('nombre, tipo, sector, ultimo_valor, unidad, estado_actual')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .execute()

    alarmas = supabase.table('alarmas')\
        .select('tipo, severidad, mensaje, created_at')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'activa')\
        .execute()

    contexto_planta = json.dumps({
        'dispositivos': dispositivos.data or [],
        'alarmas_activas': alarmas.data or []
    }, ensure_ascii=False, default=str)

    prompt = (
        f"Eres UPTIME, agente de IA industrial de AIRBOOK IoT.\n"
        f"Estado actual de la planta:\n{contexto_planta}\n\n"
        f"Consulta del operador: {mensaje}\n\n"
        f"Responde en máximo 150 palabras. "
        f"Sé técnico y concreto. Español. Sin emojis de texto. "
        f"Usa solo: ✅ ⚠️ 🔴"
    )

    respuesta = llamar_groq(prompt)
    return respuesta or "❌ No pude generar el diagnóstico. Intenta de nuevo."


def cmd_ayuda(operador: dict) -> str:
    rol = operador.get('rol', 'operador')
    base = (
        "*UPTIME IA Industrial*\n"
        "Comandos disponibles:\n\n"
        "📊 *Estado*\n"
        "  estado — resumen de todos los sistemas\n"
        "  cómo está [nombre] — detalle de un dispositivo\n"
        "  alarmas — lista de alarmas activas\n\n"
        "🔄 *Turno*\n"
        "  inicio de turno\n"
        "  fin de turno\n\n"
        "🔧 *Control*\n"
        "  encender/apagar [dispositivo]\n"
        "  abrir/cerrar [válvula]\n"
        "  iniciar riego zona [A/B/C] por [N] minutos\n\n"
        "🤖 *IA*\n"
        "  diagnóstico — análisis inteligente\n"
        "  por qué [pregunta] — análisis de causa\n\n"
        "✅ *Reconocer*\n"
        "  reconocer [id] — confirmar atención de alarma"
    )

    if rol in ('supervisor', 'gerente'):
        base += (
            "\n\n👔 *Supervisor*\n"
            "  reporte turno — resumen del turno activo"
        )
    return base


def cmd_fallback_llm(
    mensaje: str,
    operador: dict,
    planta_id: str,
    planta: dict
) -> str:
    # Contexto mínimo para respuesta general
    dispositivos = supabase.table('dispositivos_industriales')\
        .select('nombre, tipo, ultimo_valor, unidad, estado_actual')\
        .eq('planta_id', planta_id)\
        .eq('activo', True)\
        .limit(10)\
        .execute()

    ctx = json.dumps(
        dispositivos.data or [],
        ensure_ascii=False, default=str
    )

    prompt = (
        f"Eres UPTIME, agente de IA industrial de AIRBOOK IoT.\n"
        f"Planta: {planta.get('nombre','')}\n"
        f"Operador: {operador.get('nombre','')} ({operador.get('rol','')})\n"
        f"Dispositivos (muestra): {ctx}\n\n"
        f"Mensaje: {mensaje}\n\n"
        f"Responde útilmente en máximo 100 palabras. "
        f"Si no entiendes el mensaje, sugiere comandos relevantes. "
        f"Español. Usa solo estos símbolos: ✅ ⚠️ 🔴"
    )

    return llamar_groq(prompt) or (
        "No entendí el mensaje.\n"
        "Escribe *ayuda* para ver los comandos disponibles."
    )


# ═══════════════════════════════════════════════════════════════════
# HELPERS DE NEGOCIO
# ═══════════════════════════════════════════════════════════════════

def identificar_operador(numero: str) -> dict | None:
    resultado = supabase.table('operadores')\
        .select('id, nombre, rol, planta_id, whatsapp, puede_comandar')\
        .eq('whatsapp', numero)\
        .eq('activo', True)\
        .single()\
        .execute()
    return resultado.data


def obtener_planta(planta_id: str) -> dict | None:
    resultado = supabase.table('plantas')\
        .select('id, nombre, tipo, palabra_clave')\
        .eq('id', planta_id)\
        .single()\
        .execute()
    return resultado.data


def verificar_sesion_activa(numero: str) -> bool:
    resultado = supabase.table('sesiones_whatsapp')\
        .select('id, expira_at')\
        .eq('numero', numero)\
        .single()\
        .execute()

    if not resultado.data:
        return False

    expira = datetime.fromisoformat(
        resultado.data['expira_at'].replace('Z', '+00:00')
    )
    return expira > datetime.now(timezone.utc)


def activar_sesion(numero: str, planta_id: str):
    expira = (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()
    supabase.table('sesiones_whatsapp').upsert({
        'numero':    numero,
        'planta_id': planta_id,
        'expira_at': expira
    }).execute()


def extraer_nombre_dispositivo(mensaje: str) -> str:
    patrones = [
        r'cómo está (.+)',
        r'como esta (.+)',
        r'estado de (.+)',
        r'valor de (.+)',
    ]
    for patron in patrones:
        m = re.search(patron, mensaje)
        if m:
            return m.group(1).strip()
    return ''


def extraer_nombre_dispositivo_post_verbo(mensaje: str) -> str:
    verbos = ['encender', 'apagar', 'activar', 'desactivar', 'abrir', 'cerrar']
    for verbo in verbos:
        if verbo in mensaje:
            partes = mensaje.split(verbo, 1)
            if len(partes) > 1:
                return partes[1].strip()
    return ''


def calcular_tendencia_simple(historial: list) -> str:
    valores = [r['valor'] for r in historial if r.get('valor') is not None]
    if len(valores) < 3:
        return 'sin datos suficientes'

    inicio = sum(valores[:3]) / 3
    fin    = sum(valores[-3:]) / 3

    if inicio == 0:
        return 'estable ↔'

    cambio_pct = (fin - inicio) / inicio * 100

    if cambio_pct > 5:
        return f'creciente ↗ (+{cambio_pct:.1f}%)'
    if cambio_pct < -5:
        return f'decreciente ↘ ({cambio_pct:.1f}%)'
    return f'estable ↔ ({cambio_pct:+.1f}%)'


def guardar_comando_pendiente(
    numero: str,
    planta_id: str,
    dispositivo_id: str,
    nombre_disp: str,
    accion: str,
    duracion_min: int = 0
):
    expira = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
    supabase.table('comandos_pendientes_confirmacion').upsert({
        'numero':         numero,
        'planta_id':      planta_id,
        'dispositivo_id': dispositivo_id,
        'nombre_disp':    nombre_disp,
        'accion':         accion,
        'duracion_min':   duracion_min,
        'expira_at':      expira
    }).execute()


def ejecutar_comando_gateway(
    planta_id: str,
    dispositivo_id: str,
    nombre_disp: str,
    accion: str
) -> str:
    # Obtener el gateway de la planta
    gateway = supabase.table('gateways')\
        .select('id')\
        .eq('planta_id', planta_id)\
        .eq('estado', 'online')\
        .limit(1)\
        .execute()

    if not gateway.data:
        return (
            f"❌ Gateway offline — no se puede enviar el comando.\n"
            f"Verifica la conexión del gateway en planta."
        )

    # Insertar comando para que el ESP32 lo recoja en su próximo poll
    supabase.table('comandos_gateway').insert({
        'gateway_id':     gateway.data[0]['id'],
        'dispositivo_id': dispositivo_id,
        'tipo':           'relay',
        'estado_destino': accion,
        'estado':         'pendiente',
        'created_at':     datetime.now(timezone.utc).isoformat()
    }).execute()

    accion_texto = {
        'on': 'encendido', 'off': 'apagado',
        'open': 'abierto', 'close': 'cerrado'
    }.get(accion, accion)

    return (
        f"✅ Comando enviado: {nombre_disp} → {accion_texto}\n"
        f"El ESP32 ejecutará en los próximos 10 segundos.\n"
        f"Escribe *cómo está {nombre_disp}* para confirmar."
    )


def flujo_registro(numero: str, mensaje: str) -> str:
    return (
        "👋 Hola. Soy *UPTIME IA Industrial* de AIRBOOK IoT.\n\n"
        "No estás registrado en ninguna planta.\n"
        "Contacta al administrador de tu planta para que te agregue como operador.\n\n"
        "Si eres administrador, registra tu planta en:\n"
        "https://uptime-ia.vercel.app/registro"
    )


# ═══════════════════════════════════════════════════════════════════
# TRANSCRIPCIÓN DE AUDIO (notas de voz)
# ═══════════════════════════════════════════════════════════════════

def transcribir_audio(media_url: str) -> str | None:
    if not GROQ_API_KEY:
        return None

    import base64
    creds = base64.b64encode(f'{TWILIO_SID}:{TWILIO_TOKEN}'.encode()).decode()

    # Descargar audio a /tmp (igual que en Mayordomito)
    import subprocess
    tmp_path = '/tmp/audio_uptime.ogg'
    try:
        result = subprocess.run(
            ['curl', '-s', '-u', f'{TWILIO_SID}:{TWILIO_TOKEN}',
             media_url, '-o', tmp_path],
            timeout=15, capture_output=True
        )
        if result.returncode != 0:
            return None
    except Exception:
        return None

    # Enviar a Groq Whisper
    try:
        import subprocess as sp
        result = sp.run([
            'curl', '-s',
            'https://api.groq.com/openai/v1/audio/transcriptions',
            '-H', f'Authorization: Bearer {GROQ_API_KEY}',
            '-F', f'file=@{tmp_path}',
            '-F', 'model=whisper-large-v3',
            '-F', 'language=es'
        ], timeout=20, capture_output=True, text=True)

        data = json.loads(result.stdout)
        return data.get('text', '').strip() or None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
# LLAMADA A GROQ
# ═══════════════════════════════════════════════════════════════════

def llamar_groq(prompt: str) -> str | None:
    if not GROQ_API_KEY:
        return None

    payload = json.dumps({
        'model':       'llama3-8b-8192',
        'messages':    [{'role': 'user', 'content': prompt}],
        'max_tokens':  300,
        'temperature': 0.4
    }).encode()

    req = urllib.request.Request(
        GROQ_URL,
        data=payload,
        headers={
            'Content-Type':  'application/json',
            'Authorization': f'Bearer {GROQ_API_KEY}'
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"Error Groq: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# HELPERS DE RESPUESTA
# ═══════════════════════════════════════════════════════════════════

def response_twiml(xml: str):
    return {
        'statusCode': 200,
        'headers':    {'Content-Type': 'text/xml'},
        'body':       xml
    }


def response_json(data: dict, status: int = 200):
    return {
        'statusCode': status,
        'headers':    {'Content-Type': 'application/json'},
        'body':       json.dumps(data, ensure_ascii=False, default=str)
    }
