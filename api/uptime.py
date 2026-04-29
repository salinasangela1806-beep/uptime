import os
import json
import re
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler
from supabase import create_client

spbs = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_URL     = 'https://api.groq.com/openai/v1/chat/completions'
TWILIO_SID   = os.environ.get('TWILIO_ACCOUNT_SID', '')
TWILIO_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN', '')
TWILIO_FROM  = os.environ.get('TWILIO_WHATSAPP_FROM', '')
VERCEL_URL   = os.environ.get('VERCEL_URL', '')


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        largo = int(self.headers.get('Content-Length', 0))
        body  = self.rfile.read(largo).decode('utf-8')

        # Twilio envía form-urlencoded
        params   = dict(urllib.parse.parse_qsl(body))
        numero   = params.get('From', '').replace('whatsapp:', '').strip()
        mensaje  = params.get('Body', '').strip()
        media_ct = params.get('MediaContentType0', '')
        media_url = params.get('MediaUrl0', '')

        if not numero.startswith('+'):
            numero = '+' + numero

        if media_url and media_ct.startswith('audio'):
            mensaje = transcribir_audio(media_url) or mensaje

        respuesta = procesar_mensaje(numero, mensaje)
        respuesta_safe = respuesta.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

        xml  = f'<Response><Message>{respuesta_safe}</Message></Response>'
        body_out = xml.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/xml')
        self.send_header('Content-Length', str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def do_GET(self):
        body = json.dumps({'status': 'ok', 'agente': 'UPTIME IA Industrial'}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


# ═══════════════════════════════════════════════════════════════════
# PROCESADOR CENTRAL
# ═══════════════════════════════════════════════════════════════════

def procesar_mensaje(numero: str, mensaje: str) -> str:
    operador = identificar_operador(numero)

    if not operador:
        return (
            "👋 Hola. Soy *UPTIME IA Industrial* de AIRBOOK IoT.\n\n"
            "No estás registrado en ninguna planta.\n"
            "Contacta al administrador para que te registre como operador."
        )

    planta_id = operador['planta_id']
    planta    = obtener_planta(planta_id)

    if planta and planta.get('palabra_clave'):
        if not verificar_sesion_activa(numero):
            if mensaje.lower().strip() == planta['palabra_clave'].lower():
                activar_sesion(numero, planta_id)
                return (
                    f"✅ *UPTIME IA Industrial*\n"
                    f"Bienvenido, {operador['nombre']}.\n"
                    f"Planta: {planta['nombre']}\n\n"
                    f"Escribe *ayuda* para ver comandos."
                )
            return "🔒 Escribe la palabra clave para acceder."

    return enrutar(mensaje.lower().strip(), operador, planta_id, planta)


# ═══════════════════════════════════════════════════════════════════
# ENRUTADOR
# ═══════════════════════════════════════════════════════════════════

def enrutar(m: str, operador: dict, planta_id: str, planta: dict) -> str:

    if any(x in m for x in ['inicio de turno', 'iniciar turno', 'entro de turno']):
        return cmd_iniciar_turno(operador, planta_id)

    if any(x in m for x in ['fin de turno', 'finalizar turno', 'cerrar turno', 'salgo de turno']):
        return cmd_cerrar_turno(operador, planta_id, m)

    if 'sin novedades' in m:
        return cmd_cerrar_turno(operador, planta_id, m, novedades='Sin novedades')

    if any(x in m for x in ['alarmas', 'alertas']):
        return cmd_listar_alarmas(planta_id)

    if m.startswith('reconocer ') or m.startswith('ack '):
        return cmd_reconocer_alarma(m.split()[-1], operador, planta_id)

    if m.startswith('resuelta '):
        return cmd_resolver_alarma(m.split()[-1], operador, planta_id)

    if any(x in m for x in ['estado', 'cómo está todo', 'como esta todo', 'status']):
        if 'turno' not in m:
            return cmd_estado_general(planta_id)

    if any(x in m for x in ['cómo está', 'como esta', 'estado de', 'valor de']):
        nombre = extraer_post(m, ['cómo está','como esta','estado de','valor de'])
        if nombre:
            return cmd_consultar_dispositivo(nombre, planta_id)

    if any(x in m for x in ['encender','apagar','abrir','cerrar','activar','desactivar']):
        return cmd_control(m, operador, planta_id)

    if any(x in m for x in ['iniciar riego','riego zona','regar']):
        return cmd_riego(m, operador, planta_id)

    if any(x in m for x in ['diagnóstico','diagnostico','analiza','por qué','por que']):
        return cmd_diagnostico(m, planta_id)

    if any(x in m for x in ['confirmar']):
        return cmd_confirmar(operador, planta_id)

    if any(x in m for x in ['cancelar']):
        return cmd_cancelar(operador)

    if any(x in m for x in ['ayuda','help','comandos','?']):
        return cmd_ayuda(operador)

    return cmd_fallback(m, operador, planta_id, planta)


# ═══════════════════════════════════════════════════════════════════
# COMANDOS
# ═══════════════════════════════════════════════════════════════════

def cmd_iniciar_turno(operador: dict, planta_id: str) -> str:
    try:
        resp = urllib.request.urlopen(urllib.request.Request(
            f'{VERCEL_URL}/api/shifts',
            data=json.dumps({'accion':'iniciar','planta_id':planta_id,
                             'operador_whatsapp':operador['whatsapp']}).encode(),
            headers={'Content-Type':'application/json'}, method='POST'
        ), timeout=8)
        data = json.loads(resp.read())
        return data.get('reporte_whatsapp', '✅ Turno iniciado.')
    except Exception as e:
        return f"❌ Error al iniciar turno: {e}"


def cmd_cerrar_turno(operador: dict, planta_id: str, m: str, novedades: str = '') -> str:
    if not novedades:
        if 'fin de turno:' in m:
            novedades = m.split('fin de turno:', 1)[1].strip()
        elif m.strip() in ('fin de turno', 'finalizar turno', 'cerrar turno', 'salgo de turno'):
            return (
                "📝 Escribe las novedades del turno:\n"
                "Ejemplo: *fin de turno: Se cambió filtro bomba 2*\n"
                "O responde *sin novedades* para cerrar sin novedad."
            )

    try:
        resp = urllib.request.urlopen(urllib.request.Request(
            f'{VERCEL_URL}/api/shifts',
            data=json.dumps({'accion':'cerrar','planta_id':planta_id,
                             'operador_whatsapp':operador['whatsapp'],
                             'novedades':novedades or 'Sin novedades'}).encode(),
            headers={'Content-Type':'application/json'}, method='POST'
        ), timeout=8)
        data = json.loads(resp.read())
        return data.get('reporte_whatsapp', '✅ Turno cerrado.')
    except Exception as e:
        return f"❌ Error al cerrar turno: {e}"


def cmd_estado_general(planta_id: str) -> str:
    disps   = spbs.table('dispositivos_industriales')\
        .select('nombre, tipo, sector, estado_actual, ultimo_valor, unidad')\
        .eq('planta_id', planta_id).eq('activo', True).order('sector').execute()
    alarmas = spbs.table('alarmas')\
        .select('severidad, mensaje')\
        .eq('planta_id', planta_id).eq('estado', 'activa').execute()

    if not disps.data:
        return "⚪ No hay dispositivos registrados."

    total  = len(disps.data)
    online = sum(1 for d in disps.data if d.get('estado_actual') == 'online')
    errores = sum(1 for d in disps.data if d.get('estado_actual') == 'error_comm')

    por_sector = {}
    for d in disps.data:
        s = d.get('sector') or 'General'
        por_sector.setdefault(s, []).append(d)

    lineas = [f"📊 *Estado — {datetime.now(timezone.utc).strftime('%H:%M')}*\n"]
    lineas.append(f"Dispositivos: {online}/{total} en línea"
                  + (f" · {errores} sin señal" if errores else ""))

    for sector, items in list(por_sector.items())[:4]:
        lineas.append(f"\n*{sector}*")
        for d in items[:4]:
            val = d.get('ultimo_valor')
            est = d.get('estado_actual', '')
            if est == 'online' and val is not None:
                lineas.append(f"✅ {d['nombre']}: {val}{d.get('unidad','')}")
            elif est == 'error_comm':
                lineas.append(f"🔴 {d['nombre']}: Sin señal")
            else:
                lineas.append(f"⚪ {d['nombre']}: {est}")

    if alarmas.data:
        lineas.append(f"\n⚠️ *{len(alarmas.data)} alarma(s)* — escribe *alarmas*")
    else:
        lineas.append("\n✅ Sin alarmas activas")

    return '\n'.join(lineas)


def cmd_listar_alarmas(planta_id: str) -> str:
    alarmas = spbs.table('alarmas')\
        .select('id, severidad, mensaje, created_at, nivel_escalamiento')\
        .eq('planta_id', planta_id).eq('estado', 'activa').order('severidad').execute()

    if not alarmas.data:
        return "✅ No hay alarmas activas."

    ahora  = datetime.now(timezone.utc)
    lineas = [f"🚨 *{len(alarmas.data)} alarma(s) activa(s)*\n"]

    for a in alarmas.data[:5]:
        creada  = datetime.fromisoformat(a['created_at'].replace('Z', '+00:00'))
        min_act = int((ahora - creada).total_seconds() / 60)
        sev     = '🔴 CRÍTICO' if a['severidad'] == 'critico' else '⚠️ ALERTA'
        id_c    = a['id'][:8]
        lineas.append(f"{sev} · {min_act}min\n{a['mensaje'][:65]}\n*reconocer {id_c}*\n")

    return '\n'.join(lineas)


def cmd_reconocer_alarma(id_parcial: str, operador: dict, planta_id: str) -> str:
    alarmas = spbs.table('alarmas').select('id, mensaje')\
        .eq('planta_id', planta_id).eq('estado', 'activa').execute()
    alarma  = next((a for a in (alarmas.data or []) if a['id'].startswith(id_parcial)), None)

    if not alarma:
        return f"❌ No encontré alarma activa *{id_parcial}*. Escribe *alarmas* para ver la lista."

    spbs.table('alarmas').update({
        'estado':         'reconocida',
        'reconocida_por': operador['whatsapp'],
        'reconocida_at':  datetime.now(timezone.utc).isoformat()
    }).eq('id', alarma['id']).execute()

    return (
        f"✅ Alarma reconocida por {operador['nombre']}\n"
        f"{alarma['mensaje'][:70]}\n\n"
        f"Escribe *resuelta {id_parcial}* cuando esté solucionada."
    )


def cmd_resolver_alarma(id_parcial: str, operador: dict, planta_id: str) -> str:
    alarmas = spbs.table('alarmas').select('id, mensaje')\
        .eq('planta_id', planta_id).in_('estado', ['activa','reconocida']).execute()
    alarma  = next((a for a in (alarmas.data or []) if a['id'].startswith(id_parcial)), None)

    if not alarma:
        return f"❌ Alarma *{id_parcial}* no encontrada."

    spbs.table('alarmas').update({
        'estado':       'resuelta',
        'resuelta_at':  datetime.now(timezone.utc).isoformat()
    }).eq('id', alarma['id']).execute()

    return f"✅ Alarma resuelta y cerrada. Buen trabajo."


def cmd_consultar_dispositivo(nombre_busqueda: str, planta_id: str) -> str:
    disps = spbs.table('dispositivos_industriales').select('*')\
        .eq('planta_id', planta_id)\
        .ilike('nombre', f'%{nombre_busqueda}%').limit(1).execute()

    if not disps.data:
        return f"❌ No encontré *{nombre_busqueda}*. Escribe *estado* para ver todos."

    d    = disps.data[0]
    val  = d.get('ultimo_valor')
    unid = d.get('unidad', '')
    est  = d.get('estado_actual', 'desconocido')

    hace_1h = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    hist    = spbs.table('lecturas').select('valor')\
        .eq('dispositivo_id', d['id'])\
        .gte('timestamp', hace_1h).order('timestamp').execute()

    tendencia = calcular_tendencia([r['valor'] for r in (hist.data or []) if r.get('valor') is not None])

    alarma = spbs.table('alarmas').select('severidad, mensaje')\
        .eq('dispositivo_id', d['id']).eq('estado', 'activa').execute()

    lineas = [f"🔧 *{d['nombre']}*", f"Sector: {d.get('sector','—')}"]

    if val is not None:
        rango = ''
        if d.get('rango_normal_min') is not None:
            rango = f" (normal: {d['rango_normal_min']}–{d['rango_normal_max']}{unid})"
        lineas.append(f"Valor: *{val}{unid}*{rango}")
    else:
        lineas.append(f"Estado: {est}")

    lineas.append(f"Tendencia 1h: {tendencia}")

    if alarma.data:
        sev = '🔴' if alarma.data[0]['severidad'] == 'critico' else '⚠️'
        lineas.append(f"{sev} {alarma.data[0]['mensaje'][:60]}")

    return '\n'.join(lineas)


def cmd_control(m: str, operador: dict, planta_id: str) -> str:
    if not operador.get('puede_comandar'):
        return "❌ No tienes permisos de comando en esta planta."

    ACCIONES = {'encender':'on','activar':'on','apagar':'off',
                'desactivar':'off','abrir':'open','cerrar':'close'}
    accion = next((v for k, v in ACCIONES.items() if k in m), None)
    if not accion:
        return "❌ No entendí el comando. Ejemplo: *encender bomba principal*"

    nombre = extraer_post(m, list(ACCIONES.keys()))
    if not nombre:
        return "❌ Especifica el dispositivo. Ejemplo: *encender bomba 2*"

    disp = spbs.table('dispositivos_industriales').select('id, nombre, permite_comando_remoto, requiere_confirmacion')\
        .eq('planta_id', planta_id).ilike('nombre', f'%{nombre}%')\
        .eq('es_actuador', True).limit(1).execute()

    if not disp.data:
        return f"❌ No encontré actuador *{nombre}*."

    d = disp.data[0]
    if not d.get('permite_comando_remoto', True):
        return f"❌ {d['nombre']} solo se opera localmente."

    if d.get('requiere_confirmacion'):
        guardar_pendiente(operador['whatsapp'], planta_id, d['id'], d['nombre'], accion)
        return (
            f"⚠️ *Confirmar:* {accion.upper()} → {d['nombre']}\n"
            f"Responde *confirmar* o *cancelar*."
        )

    return ejecutar_en_gateway(planta_id, d['id'], d['nombre'], accion)


def cmd_riego(m: str, operador: dict, planta_id: str) -> str:
    if not operador.get('puede_comandar'):
        return "❌ No tienes permisos de comando."

    zona     = re.search(r'zona\s+([a-z0-9]+)', m)
    duracion = re.search(r'(\d+)\s*(min|minutos|horas?)', m)

    if not zona:
        return "❌ Especifica la zona. Ejemplo: *iniciar riego zona A por 30 minutos*"

    zona_nombre = zona.group(1).upper()
    minutos     = int(duracion.group(1)) * (60 if duracion and 'hora' in duracion.group(2) else 1) if duracion else 30

    valvula = spbs.table('dispositivos_industriales').select('id, nombre')\
        .eq('planta_id', planta_id).ilike('sector', f'%{zona_nombre}%')\
        .in_('tipo', ['electrovalvula_riego','valvula_solenoide']).limit(1).execute()

    if not valvula.data:
        return f"❌ No encontré válvula de riego en Zona {zona_nombre}."

    v = valvula.data[0]
    guardar_pendiente(operador['whatsapp'], planta_id, v['id'], v['nombre'], 'open', duracion_min=minutos)

    return (
        f"⚠️ *Confirmar:*\n"
        f"Riego Zona {zona_nombre} — {minutos} minutos\n"
        f"Válvula: {v['nombre']}\n\n"
        f"Responde *confirmar* o *cancelar*."
    )


def cmd_confirmar(operador: dict, planta_id: str) -> str:
    pend = spbs.table('comandos_pendientes_confirmacion')\
        .select('*').eq('numero', operador['whatsapp'])\
        .gt('expira_at', datetime.now(timezone.utc).isoformat())\
        .single().execute()

    if not pend.data:
        return "❌ No hay comando pendiente de confirmar (puede haber expirado en 3 min)."

    p = pend.data
    spbs.table('comandos_pendientes_confirmacion').delete().eq('numero', operador['whatsapp']).execute()
    return ejecutar_en_gateway(planta_id, p['dispositivo_id'], p['nombre_disp'], p['accion'])


def cmd_cancelar(operador: dict) -> str:
    spbs.table('comandos_pendientes_confirmacion').delete().eq('numero', operador['whatsapp']).execute()
    return "❌ Comando cancelado."


def cmd_diagnostico(m: str, planta_id: str) -> str:
    disps   = spbs.table('dispositivos_industriales')\
        .select('nombre, tipo, sector, ultimo_valor, unidad, estado_actual')\
        .eq('planta_id', planta_id).eq('activo', True).execute()
    alarmas = spbs.table('alarmas').select('tipo, severidad, mensaje')\
        .eq('planta_id', planta_id).eq('estado', 'activa').execute()

    ctx = json.dumps({'dispositivos': disps.data or [], 'alarmas': alarmas.data or []},
                     ensure_ascii=False, default=str)

    prompt = (
        f"Eres UPTIME, agente IA industrial de AIRBOOK IoT.\n"
        f"Estado actual:\n{ctx}\n\n"
        f"Consulta: {m}\n\n"
        f"Responde en máximo 150 palabras. Técnico y concreto. Español. "
        f"Solo estos símbolos: ✅ ⚠️ 🔴"
    )
    return llamar_groq(prompt) or "❌ No pude generar el diagnóstico. Intenta de nuevo."


def cmd_ayuda(operador: dict) -> str:
    rol  = operador.get('rol', 'operador')
    base = (
        "*UPTIME IA Industrial — AIRBOOK IoT*\n\n"
        "📊 *Consultas*\n"
        "  estado\n"
        "  cómo está [dispositivo]\n"
        "  alarmas\n\n"
        "🔄 *Turno*\n"
        "  inicio de turno\n"
        "  fin de turno\n\n"
        "🔧 *Control*\n"
        "  encender/apagar [dispositivo]\n"
        "  abrir/cerrar [válvula]\n"
        "  iniciar riego zona [A/B] por [N] minutos\n\n"
        "✅ *Alarmas*\n"
        "  reconocer [id]\n"
        "  resuelta [id]\n\n"
        "🤖 *IA*\n"
        "  diagnóstico\n"
        "  por qué [pregunta]"
    )
    if rol in ('supervisor', 'gerente'):
        base += "\n\n👔 *Supervisor*\n  reporte turno"
    return base


def cmd_fallback(m: str, operador: dict, planta_id: str, planta: dict) -> str:
    disps = spbs.table('dispositivos_industriales')\
        .select('nombre, tipo, ultimo_valor, unidad, estado_actual')\
        .eq('planta_id', planta_id).eq('activo', True).limit(8).execute()

    prompt = (
        f"Eres UPTIME, agente IA industrial de AIRBOOK IoT.\n"
        f"Planta: {planta.get('nombre','')}\n"
        f"Operador: {operador.get('nombre','')} ({operador.get('rol','')})\n"
        f"Dispositivos: {json.dumps(disps.data or [], ensure_ascii=False, default=str)}\n\n"
        f"Mensaje: {m}\n\n"
        f"Responde en máximo 100 palabras. Si no entiendes, sugiere comandos. "
        f"Español. Solo: ✅ ⚠️ 🔴"
    )
    return llamar_groq(prompt) or "No entendí. Escribe *ayuda* para ver los comandos."


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def identificar_operador(numero: str) -> dict | None:
    r = spbs.table('operadores')\
        .select('id, nombre, rol, planta_id, whatsapp, puede_comandar')\
        .eq('whatsapp', numero).eq('activo', True).single().execute()
    return r.data


def obtener_planta(planta_id: str) -> dict | None:
    r = spbs.table('plantas').select('id, nombre, tipo, palabra_clave')\
        .eq('id', planta_id).single().execute()
    return r.data


def verificar_sesion_activa(numero: str) -> bool:
    r = spbs.table('sesiones_whatsapp').select('expira_at')\
        .eq('numero', numero).single().execute()
    if not r.data:
        return False
    expira = datetime.fromisoformat(r.data['expira_at'].replace('Z', '+00:00'))
    return expira > datetime.now(timezone.utc)


def activar_sesion(numero: str, planta_id: str):
    expira = (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()
    spbs.table('sesiones_whatsapp').upsert({'numero': numero, 'planta_id': planta_id, 'expira_at': expira}).execute()


def extraer_post(mensaje: str, verbos: list) -> str:
    for verbo in verbos:
        if verbo in mensaje:
            partes = mensaje.split(verbo, 1)
            if len(partes) > 1:
                return partes[1].strip()
    return ''


def calcular_tendencia(valores: list) -> str:
    if len(valores) < 3:
        return 'sin datos suficientes'
    inicio = sum(valores[:3]) / 3
    fin    = sum(valores[-3:]) / 3
    if inicio == 0:
        return 'estable ↔'
    pct = (fin - inicio) / inicio * 100
    if pct > 5:   return f'creciente ↗ (+{pct:.1f}%)'
    if pct < -5:  return f'decreciente ↘ ({pct:.1f}%)'
    return f'estable ↔ ({pct:+.1f}%)'


def guardar_pendiente(numero, planta_id, dispositivo_id, nombre_disp, accion, duracion_min=0):
    expira = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
    spbs.table('comandos_pendientes_confirmacion').upsert({
        'numero': numero, 'planta_id': planta_id,
        'dispositivo_id': dispositivo_id, 'nombre_disp': nombre_disp,
        'accion': accion, 'duracion_min': duracion_min, 'expira_at': expira
    }).execute()


def ejecutar_en_gateway(planta_id, dispositivo_id, nombre_disp, accion) -> str:
    gw = spbs.table('gateways').select('id')\
        .eq('planta_id', planta_id).eq('estado', 'online').limit(1).execute()
    if not gw.data:
        return "❌ Gateway offline — verifica la conexión del dispositivo en planta."

    spbs.table('comandos_gateway').insert({
        'gateway_id':     gw.data[0]['id'],
        'dispositivo_id': dispositivo_id,
        'tipo':           'relay',
        'estado_destino': accion,
        'estado':         'pendiente',
        'created_at':     datetime.now(timezone.utc).isoformat()
    }).execute()

    texto = {'on':'encendido','off':'apagado','open':'abierto','close':'cerrado'}.get(accion, accion)
    return (
        f"✅ Comando enviado: *{nombre_disp}* → {texto}\n"
        f"El ESP32 ejecuta en los próximos 10 segundos.\n"
        f"Escribe *cómo está {nombre_disp}* para confirmar."
    )


def llamar_groq(prompt: str) -> str | None:
    if not GROQ_API_KEY:
        return None
    payload = json.dumps({'model':'llama3-8b-8192',
                          'messages':[{'role':'user','content':prompt}],
                          'max_tokens':300,'temperature':0.4}).encode()
    req = urllib.request.Request(GROQ_URL, data=payload,
                                 headers={'Content-Type':'application/json',
                                          'Authorization':f'Bearer {GROQ_API_KEY}'})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"Error Groq: {e}")
        return None


def transcribir_audio(media_url: str) -> str | None:
    if not GROQ_API_KEY or not TWILIO_SID:
        return None
    import base64, subprocess
    tmp = '/tmp/audio_uptime.ogg'
    try:
        subprocess.run(['curl','-s','-u',f'{TWILIO_SID}:{TWILIO_TOKEN}',
                        media_url,'-o',tmp], timeout=15, check=True)
        r = subprocess.run(['curl','-s',
                            'https://api.groq.com/openai/v1/audio/transcriptions',
                            '-H',f'Authorization: Bearer {GROQ_API_KEY}',
                            '-F',f'file=@{tmp}','-F','model=whisper-large-v3',
                            '-F','language=es'], timeout=20, capture_output=True, text=True)
        return json.loads(r.stdout).get('text','').strip() or None
    except Exception:
        return None
