import os
import json
import math
from datetime import datetime, timezone, timedelta
from supabase import create_client

supabase = create_client(
    os.environ['SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions'


# ═══════════════════════════════════════════════════════════════════
# HANDLER PRINCIPAL — llamado por cron-job.org cada minuto
# ═══════════════════════════════════════════════════════════════════

def handler(request):
    if request.method != 'POST':
        return response_json({'status': 'ok', 'servicio': 'analyzer'})

    resultados = []

    plantas = supabase.table('plantas')\
        .select('id, nombre, tipo')\
        .eq('activa', True).execute()

    for planta in (plantas.data or []):
        resultado = analizar_planta(planta['id'], planta['nombre'])
        resultados.append(resultado)

    return response_json({
        'status': 'ok',
        'plantas_analizadas': len(resultados),
        'alertas_generadas': sum(r['alertas'] for r in resultados),
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


# ═══════════════════════════════════════════════════════════════════
# ANÁLISIS POR PLANTA
# ═══════════════════════════════════════════════════════════════════

def analizar_planta(planta_id: str, planta_nombre: str) -> dict:
    alertas_generadas = 0

    dispositivos = supabase.table('dispositivos_industriales')\
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

            # Si la anomalía requiere razonamiento IA (no solo regla)
            if anomalia.get('requiere_ia'):
                razonamiento = razonar_con_llm(dispositivo, anomalia)
                if razonamiento:
                    enviar_alerta_whatsapp(planta_id, dispositivo, razonamiento)

    return {'planta_id': planta_id, 'alertas': alertas_generadas}


# ═══════════════════════════════════════════════════════════════════
# ANÁLISIS DE UN DISPOSITIVO — 4 CAPAS DE DETECCIÓN
# ═══════════════════════════════════════════════════════════════════

def analizar_dispositivo(dispositivo: dict, planta_id: str) -> list:
    anomalias = []
    sensor_id = dispositivo['id']

    # --- CAPA 1: Verificar comunicación ---
    sin_comm = verificar_comunicacion(dispositivo)
    if sin_comm:
        return [sin_comm]  # Si no hay datos, no analizamos nada más

    # --- Obtener historial ---
    hist_1h  = obtener_historial(sensor_id, horas=1)
    hist_8h  = obtener_historial(sensor_id, horas=8)
    hist_24h = obtener_historial(sensor_id, horas=24)
    hist_7d  = obtener_historial(sensor_id, horas=168)

    if not hist_1h:
        return []

    valor_actual = hist_1h[-1]['valor']

    # --- CAPA 2: Umbral estático ---
    umbral = verificar_umbrales(dispositivo, valor_actual)
    if umbral:
        anomalias.append(umbral)

    # --- CAPA 3: Anomalía estadística (desviación del promedio histórico) ---
    if len(hist_24h) >= 10:
        anomalia_stat = detectar_anomalia_estadistica(
            dispositivo, valor_actual, hist_24h
        )
        if anomalia_stat:
            anomalias.append(anomalia_stat)

    # --- CAPA 4: Tendencia creciente/decreciente sostenida ---
    if len(hist_7d) >= 24:
        tendencia = detectar_tendencia_sostenida(
            dispositivo, hist_7d, hist_1h
        )
        if tendencia:
            anomalias.append(tendencia)

    # --- CAPA 5: Correlación entre sensores ---
    correlacion = verificar_correlaciones(dispositivo, planta_id, valor_actual)
    if correlacion:
        anomalias.append(correlacion)

    return anomalias


# ═══════════════════════════════════════════════════════════════════
# CAPA 1: COMUNICACIÓN
# ═══════════════════════════════════════════════════════════════════

def verificar_comunicacion(dispositivo: dict) -> dict | None:
    ultima = dispositivo.get('ultima_lectura')
    if not ultima:
        return None

    ultima_dt = datetime.fromisoformat(ultima.replace('Z', '+00:00'))
    ahora = datetime.now(timezone.utc)
    minutos_sin_dato = (ahora - ultima_dt).total_seconds() / 60

    # Umbral según tipo de sensor
    UMBRALES_COMM = {
        'critico':  5,   # bombas, válvulas de emergencia
        'proceso':  15,  # sensores de proceso normal
        'ambiental': 60, # temperatura ambiente, humedad
        'default':  30
    }
    categoria = clasificar_sensor(dispositivo['tipo'])
    umbral_min = UMBRALES_COMM.get(categoria, UMBRALES_COMM['default'])

    if minutos_sin_dato > umbral_min:
        return {
            'tipo': 'sin_comunicacion',
            'severidad': 'critico' if categoria == 'critico' else 'alerta',
            'mensaje': (
                f"Sin señal hace {int(minutos_sin_dato)} minutos "
                f"(umbral: {umbral_min} min)"
            ),
            'valor': None,
            'requiere_ia': False,
            'minutos_sin_dato': int(minutos_sin_dato)
        }
    return None


# ═══════════════════════════════════════════════════════════════════
# CAPA 2: UMBRALES ESTÁTICOS
# ═══════════════════════════════════════════════════════════════════

def verificar_umbrales(dispositivo: dict, valor: float) -> dict | None:
    critico_min = dispositivo.get('umbral_critico_min')
    critico_max = dispositivo.get('umbral_critico_max')
    alerta_min  = dispositivo.get('umbral_alerta_min')
    alerta_max  = dispositivo.get('umbral_alerta_max')
    unidad      = dispositivo.get('unidad', '')

    if critico_max is not None and valor > critico_max:
        return {
            'tipo': 'umbral_critico_alto',
            'severidad': 'critico',
            'mensaje': (
                f"Valor CRÍTICO ALTO: {valor:.2f}{unidad} "
                f"(límite: {critico_max}{unidad})"
            ),
            'valor': valor,
            'umbral': critico_max,
            'requiere_ia': False
        }

    if critico_min is not None and valor < critico_min:
        return {
            'tipo': 'umbral_critico_bajo',
            'severidad': 'critico',
            'mensaje': (
                f"Valor CRÍTICO BAJO: {valor:.2f}{unidad} "
                f"(límite: {critico_min}{unidad})"
            ),
            'valor': valor,
            'umbral': critico_min,
            'requiere_ia': False
        }

    if alerta_max is not None and valor > alerta_max:
        return {
            'tipo': 'umbral_alerta_alto',
            'severidad': 'alerta',
            'mensaje': (
                f"Valor alto: {valor:.2f}{unidad} "
                f"(umbral alerta: {alerta_max}{unidad})"
            ),
            'valor': valor,
            'umbral': alerta_max,
            'requiere_ia': True  # pide contexto al LLM
        }

    if alerta_min is not None and valor < alerta_min:
        return {
            'tipo': 'umbral_alerta_bajo',
            'severidad': 'alerta',
            'mensaje': (
                f"Valor bajo: {valor:.2f}{unidad} "
                f"(umbral alerta: {alerta_min}{unidad})"
            ),
            'valor': valor,
            'umbral': alerta_min,
            'requiere_ia': True
        }

    return None


# ═══════════════════════════════════════════════════════════════════
# CAPA 3: ANOMALÍA ESTADÍSTICA
# ═══════════════════════════════════════════════════════════════════

def detectar_anomalia_estadistica(
    dispositivo: dict, valor_actual: float, historial: list
) -> dict | None:

    valores = [r['valor'] for r in historial if r['valor'] is not None]
    if len(valores) < 10:
        return None

    promedio = sum(valores) / len(valores)
    varianza = sum((v - promedio) ** 2 for v in valores) / len(valores)
    desviacion = math.sqrt(varianza)

    if desviacion < 0.001:  # sensor con valor constante esperado
        return None

    z_score = abs(valor_actual - promedio) / desviacion

    # z > 3 = estadísticamente anómalo (99.7% confianza)
    if z_score > 3.0:
        porcentaje_desviacion = abs(valor_actual - promedio) / promedio * 100
        return {
            'tipo': 'anomalia_estadistica',
            'severidad': 'alerta',
            'mensaje': (
                f"Valor estadísticamente anómalo: {valor_actual:.2f} "
                f"({porcentaje_desviacion:.1f}% fuera del promedio de 24h "
                f"de {promedio:.2f}{dispositivo.get('unidad','')})"
            ),
            'valor': valor_actual,
            'promedio_24h': round(promedio, 3),
            'z_score': round(z_score, 2),
            'requiere_ia': True  # necesita contexto para explicar POR QUÉ
        }

    return None


# ═══════════════════════════════════════════════════════════════════
# CAPA 4: TENDENCIA SOSTENIDA (el más valioso para mantenimiento)
# ═══════════════════════════════════════════════════════════════════

def detectar_tendencia_sostenida(
    dispositivo: dict, hist_7d: list, hist_1h: list
) -> dict | None:

    valores_7d = [r['valor'] for r in hist_7d if r['valor'] is not None]
    if len(valores_7d) < 24:
        return None

    # Calcular pendiente por regresión lineal simple (mínimos cuadrados)
    n = len(valores_7d)
    indices = list(range(n))
    media_x = sum(indices) / n
    media_y = sum(valores_7d) / n

    numerador   = sum((indices[i] - media_x) * (valores_7d[i] - media_y) for i in range(n))
    denominador = sum((indices[i] - media_x) ** 2 for i in range(n))

    if denominador == 0:
        return None

    pendiente = numerador / denominador  # unidades por lectura

    # Normalizar: cambio porcentual respecto al valor inicial
    valor_inicio = valores_7d[0]
    if valor_inicio == 0:
        return None

    cambio_total_pct = abs(pendiente * n / valor_inicio) * 100

    # Umbral: tendencia significativa = >15% de cambio en 7 días
    if cambio_total_pct < 15:
        return None

    # ¿Qué tan rápido va a llegar al umbral de alarma?
    valor_actual = hist_1h[-1]['valor']
    unidad = dispositivo.get('unidad', '')
    direccion = 'creciente' if pendiente > 0 else 'decreciente'

    # Proyectar cuándo llegará al umbral crítico
    proyeccion_texto = ''
    umbral_critico = dispositivo.get('umbral_critico_max') \
        if pendiente > 0 else dispositivo.get('umbral_critico_min')

    if umbral_critico is not None and pendiente != 0:
        lecturas_hasta_critico = (umbral_critico - valor_actual) / pendiente
        if 0 < lecturas_hasta_critico < n * 2:
            # Asumiendo 1 lectura por minuto
            horas = lecturas_hasta_critico / 60
            if horas < 48:
                proyeccion_texto = (
                    f" Proyección: alcanza umbral crítico "
                    f"({umbral_critico}{unidad}) en ~{horas:.0f}h."
                )

    return {
        'tipo': 'tendencia_sostenida',
        'severidad': 'alerta',
        'mensaje': (
            f"Tendencia {direccion} del {cambio_total_pct:.1f}% "
            f"en 7 días. Valor actual: {valor_actual:.2f}{unidad}."
            f"{proyeccion_texto}"
        ),
        'valor': valor_actual,
        'pendiente': round(pendiente, 4),
        'cambio_pct_7d': round(cambio_total_pct, 1),
        'direccion': direccion,
        'requiere_ia': True  # este es el caso estrella del LLM
    }


# ═══════════════════════════════════════════════════════════════════
# CAPA 5: CORRELACIONES ENTRE SENSORES
# ═══════════════════════════════════════════════════════════════════

def verificar_correlaciones(
    dispositivo: dict, planta_id: str, valor_actual: float
) -> dict | None:

    tipo = dispositivo.get('tipo', '')
    sector = dispositivo.get('sector', '')

    # Regla: Bomba ON pero caudal en su sector = 0
    if tipo == 'motor_arranque' and dispositivo.get('estado_actual') == 'on':
        sensor_caudal = supabase.table('dispositivos_industriales')\
            .select('id, ultimo_valor, unidad')\
            .eq('planta_id', planta_id)\
            .eq('sector', sector)\
            .eq('tipo', 'caudal')\
            .execute()

        if sensor_caudal.data:
            caudal = sensor_caudal.data[0].get('ultimo_valor', 0)
            if caudal is not None and caudal < 0.5:
                return {
                    'tipo': 'correlacion_bomba_sin_caudal',
                    'severidad': 'critico',
                    'mensaje': (
                        f"Bomba encendida pero caudal = {caudal:.2f} "
                        f"{sensor_caudal.data[0].get('unidad','')}"
                        f" en sector {sector}. "
                        f"Posible: cavitación, tubería obstruida o válvula cerrada."
                    ),
                    'valor': caudal,
                    'requiere_ia': True
                }

    # Regla: Nivel tanque sube aunque válvula de entrada cerrada
    if tipo == 'nivel_ultrasonico':
        lecturas_recientes = obtener_historial(dispositivo['id'], horas=1)
        if len(lecturas_recientes) >= 3:
            subida = lecturas_recientes[-1]['valor'] - lecturas_recientes[0]['valor']
            valvula_entrada = supabase.table('dispositivos_industriales')\
                .select('estado_actual')\
                .eq('planta_id', planta_id)\
                .eq('sector', sector)\
                .eq('tipo', 'electrovalvula_riego')\
                .execute()

            if (valvula_entrada.data and
                    valvula_entrada.data[0].get('estado_actual') == 'off' and
                    subida > 2.0):
                return {
                    'tipo': 'correlacion_nivel_valvula_cerrada',
                    'severidad': 'alerta',
                    'mensaje': (
                        f"Nivel sube {subida:.1f}% con válvula de entrada cerrada. "
                        f"Posible: válvula con fuga o bypass abierto."
                    ),
                    'valor': valor_actual,
                    'requiere_ia': True
                }

    return None


# ═══════════════════════════════════════════════════════════════════
# RAZONAMIENTO CON LLM (Groq — llama3-8b-8192)
# ═══════════════════════════════════════════════════════════════════

def razonar_con_llm(dispositivo: dict, anomalia: dict) -> str | None:
    if not GROQ_API_KEY:
        return None

    # Construir contexto rico para el LLM
    hist_7d = obtener_historial(dispositivo['id'], horas=168)
    valores_7d = [r['valor'] for r in hist_7d if r['valor'] is not None]

    # Resumen estadístico del historial (no enviamos todos los datos)
    if valores_7d:
        min_val = min(valores_7d)
        max_val = max(valores_7d)
        prom_val = sum(valores_7d) / len(valores_7d)
        # Dividir en días para mostrar tendencia
        dia_size = max(1, len(valores_7d) // 7)
        promedios_diarios = []
        for i in range(0, len(valores_7d), dia_size):
            chunk = valores_7d[i:i+dia_size]
            promedios_diarios.append(round(sum(chunk)/len(chunk), 2))
        resumen_hist = (
            f"Mínimo 7d: {min_val:.2f}, Máximo: {max_val:.2f}, "
            f"Promedio: {prom_val:.2f}\n"
            f"Tendencia diaria: {' → '.join(str(v) for v in promedios_diarios)}"
        )
    else:
        resumen_hist = "Sin historial disponible"

    prompt = f"""Eres HORUS, agente de IA industrial para la plataforma AIRBOOK IoT.
Analiza esta anomalía detectada y genera un mensaje para el supervisor de planta.

DISPOSITIVO:
- Nombre: {dispositivo.get('nombre', 'Desconocido')}
- Tipo: {dispositivo.get('tipo', 'desconocido')}
- Sector: {dispositivo.get('sector', 'sin sector')}
- Unidad de medida: {dispositivo.get('unidad', '')}
- Rango normal: {dispositivo.get('rango_normal_min')} a {dispositivo.get('rango_normal_max')} {dispositivo.get('unidad','')}

ANOMALÍA DETECTADA:
- Tipo: {anomalia['tipo']}
- Severidad: {anomalia['severidad']}
- Detalle técnico: {anomalia['mensaje']}
- Valor actual: {anomalia.get('valor')} {dispositivo.get('unidad','')}

HISTORIAL 7 DÍAS:
{resumen_hist}

INSTRUCCIONES:
1. En máximo 4 líneas, explica qué está pasando en lenguaje técnico pero comprensible
2. Da 2-3 causas probables ordenadas de más a menos probable
3. Recomienda acción concreta con urgencia (inmediata/esta semana/próximo mantenimiento)
4. Si es tendencia, estima el tiempo antes de falla si no se actúa
5. Responde en español, sin emojis de texto, usa solo estos símbolos: ✅ ⚠️ 🔴

Sé conciso. Máximo 150 palabras."""

    import urllib.request

    payload = json.dumps({
        'model': 'llama3-8b-8192',
        'messages': [{'role': 'user', 'content': prompt}],
        'max_tokens': 300,
        'temperature': 0.3
    }).encode()

    req = urllib.request.Request(
        GROQ_URL,
        data=payload,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {GROQ_API_KEY}'
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data['choices'][0]['message']['content']
    except Exception as e:
        print(f"Error LLM: {e}")
        return anomalia['mensaje']  # fallback: mensaje de regla


# ═══════════════════════════════════════════════════════════════════
# REGISTRAR ALARMA EN SUPABASE
# ═══════════════════════════════════════════════════════════════════

def registrar_alarma(dispositivo: dict, planta_id: str, anomalia: dict):
    # Verificar si ya existe una alarma activa del mismo tipo
    existente = supabase.table('alarmas')\
        .select('id')\
        .eq('dispositivo_id', dispositivo['id'])\
        .eq('tipo', anomalia['tipo'])\
        .eq('estado', 'activa')\
        .execute()

    if existente.data:
        return  # Ya registrada, no duplicar

    supabase.table('alarmas').insert({
        'planta_id':          planta_id,
        'dispositivo_id':     dispositivo['id'],
        'tipo':               anomalia['tipo'],
        'severidad':          anomalia['severidad'],
        'valor_disparador':   anomalia.get('valor'),
        'mensaje':            anomalia['mensaje'],
        'estado':             'activa',
        'nivel_escalamiento': 1,
        'notificados':        [],
        'proxima_escalada':   (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat()
    }).execute()


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def obtener_historial(sensor_id: str, horas: int) -> list:
    desde = (datetime.now(timezone.utc) - timedelta(hours=horas)).isoformat()
    result = supabase.table('lecturas')\
        .select('valor, timestamp')\
        .eq('dispositivo_id', sensor_id)\
        .gte('timestamp', desde)\
        .order('timestamp', desc=False)\
        .limit(2000)\
        .execute()
    return result.data or []


def clasificar_sensor(tipo: str) -> str:
    CRITICOS = {
        'motor_arranque', 'valvula_solenoide', 'presion_bar',
        'nivel_ultrasonico', 'corriente_ac', 'electrovalvula_riego'
    }
    AMBIENTALES = {
        'temperatura_ambiente', 'humedad_ambiente',
        'radiacion_solar', 'velocidad_viento', 'precipitacion'
    }
    if tipo in CRITICOS:
        return 'critico'
    if tipo in AMBIENTALES:
        return 'ambiental'
    return 'proceso'


def enviar_alerta_whatsapp(planta_id: str, dispositivo: dict, mensaje: str):
    # Obtener operadores que deben recibir alertas
    operadores = supabase.table('operadores')\
        .select('whatsapp, nombre, rol')\
        .eq('planta_id', planta_id)\
        .eq('recibe_alertas', True)\
        .eq('activo', True)\
        .execute()

    for op in (operadores.data or []):
        # Insertar en cola de mensajes para que api/horus.py los envíe
        supabase.table('cola_mensajes_whatsapp').insert({
            'planta_id':     planta_id,
            'destinatario':  op['whatsapp'],
            'mensaje':       f"⚠️ HORUS — {dispositivo.get('nombre','Sensor')}\n\n{mensaje}",
            'tipo':          'alerta_automatica',
            'estado':        'pendiente',
            'created_at':    datetime.now(timezone.utc).isoformat()
        }).execute()


def response_json(data: dict, status: int = 200):
    import http
    return http.HTTPResponse(
        status=status,
        headers={'Content-Type': 'application/json'},
        body=json.dumps(data)
    )