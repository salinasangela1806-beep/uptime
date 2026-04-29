# UPTIME IA Industrial — Guía de Deployment en Vercel

## Estructura del repositorio

```
uptime-ia/
├── api/
│   ├── analyzer.py     ← cron cada minuto (detección anomalías)
│   ├── alerts.py       ← cron cada minuto (escalamiento + WhatsApp)
│   ├── gateway.py      ← recibe datos del ESP32
│   ├── shifts.py       ← gestión de turnos operacionales
│   ├── uptime.py       ← agente WhatsApp principal
│   └── dashboard.py    ← API JSON para el panel web
├── public/
│   └── index.html      ← dashboard web
├── requirements.txt
└── vercel.json
```

---

## Paso 1 — Crear repositorio en GitHub

```bash
git init uptime-ia
cd uptime-ia

# Copiar todos los archivos a esta carpeta
# Luego:
git add .
git commit -m "Initial commit — UPTIME IA Industrial"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/uptime-ia.git
git push -u origin main
```

---

## Paso 2 — Importar en Vercel

1. Ir a https://vercel.com/new
2. Importar el repositorio `uptime-ia` desde GitHub
3. Framework Preset: **Other**
4. Root Directory: `.` (raíz)
5. Build Command: dejar vacío
6. Output Directory: dejar vacío
7. Clic en **Deploy**

El primer deploy puede fallar si no están las variables de entorno. Eso es normal.

---

## Paso 3 — Variables de entorno en Vercel

En el proyecto de Vercel → Settings → Environment Variables.
Agregar estas variables (todas en Production + Preview + Development):

| Variable                | Valor                          | Dónde obtenerlo               |
|------------------------|-------------------------------|-------------------------------|
| SUPABASE_URL            | https://xxx.supabase.co        | Supabase → Settings → API     |
| SUPABASE_SERVICE_KEY    | eyJ...                         | Supabase → Settings → API     |
| GROQ_API_KEY            | gsk_...                        | console.groq.com              |
| TWILIO_ACCOUNT_SID      | ACxxx...                       | console.twilio.com            |
| TWILIO_AUTH_TOKEN       | xxx...                         | console.twilio.com            |
| TWILIO_WHATSAPP_FROM    | whatsapp:+14155238886          | Twilio Sandbox                |
| VERCEL_URL              | https://uptime-ia.vercel.app   | URL de tu proyecto en Vercel  |

Después de agregar las variables: **Redeploy** desde Vercel.

---

## Paso 4 — Configurar Supabase

### 4.1 Ejecutar el SQL de tablas base

En Supabase → SQL Editor, ejecutar primero el archivo
`supabase_tablas_base.sql` (plantas, dispositivos, lecturas, alarmas,
turnos, ciclos, operadores).

### 4.2 Ejecutar el SQL de tablas adicionales

Luego ejecutar `supabase_tablas_adicionales.sql` (gateways,
errores_comunicacion, cola_mensajes_whatsapp, comandos_gateway,
sesiones_whatsapp, comandos_pendientes_confirmacion).

### 4.3 Insertar primera planta de prueba

```sql
INSERT INTO plantas (nombre, tipo, ciudad, departamento, palabra_clave, whatsapp_titular, plan)
VALUES (
  'Finca El Progreso',
  'finca',
  'Chía',
  'Cundinamarca',
  'uptime2024',
  '+573XXXXXXXXX',
  'pro'
);
```

### 4.4 Insertar primer gateway

```sql
-- Primero obtener el ID de la planta:
SELECT id FROM plantas WHERE nombre = 'Finca El Progreso';

-- Luego insertar el gateway:
INSERT INTO gateways (planta_id, secret_hash, activo)
VALUES (
  'UUID_DE_LA_PLANTA',
  encode(sha256('mi-secret-seguro-aqui'), 'hex'),
  true
);

-- Anotar el codigo generado automáticamente (AIRBOOK-G-0001)
SELECT codigo, id FROM gateways ORDER BY created_at DESC LIMIT 1;
```

### 4.5 Insertar primer operador

```sql
INSERT INTO operadores (planta_id, whatsapp, nombre, rol, puede_comandar, recibe_alertas)
VALUES (
  'UUID_DE_LA_PLANTA',
  '+573XXXXXXXXX',
  'Tu Nombre',
  'gerente',
  true,
  true
);
```

---

## Paso 5 — Configurar cron-job.org

Los endpoints `analyzer` y `alerts` deben ejecutarse cada minuto.

1. Ir a https://cron-job.org y crear cuenta gratuita
2. Crear dos trabajos:

**Trabajo 1 — Analyzer**
- URL: `https://uptime-ia.vercel.app/api/analyzer`
- Método: GET
- Frecuencia: cada 1 minuto
- Zona horaria: America/Bogota

**Trabajo 2 — Alerts**
- URL: `https://uptime-ia.vercel.app/api/alerts`
- Método: GET
- Frecuencia: cada 1 minuto
- Zona horaria: America/Bogota

---

## Paso 6 — Configurar WhatsApp con Twilio

1. Ir a https://console.twilio.com
2. Messaging → Try it out → Send a WhatsApp message
3. En "Sandbox Settings", configurar:
   - **When a message comes in**: `https://uptime-ia.vercel.app/api/uptime`
   - Método: HTTP POST

Para que un número pueda hablar con el bot:
- El usuario envía `join <sandbox-word>` al número de Twilio
- Eso activa el sandbox para ese número

---

## Paso 7 — Verificar que todo funciona

### Test del gateway (desde Postman o curl):

```bash
curl -X POST https://uptime-ia.vercel.app/api/gateway \
  -H "Content-Type: application/json" \
  -d '{
    "gateway_id": "AIRBOOK-G-0001",
    "gateway_secret": "mi-secret-seguro-aqui",
    "lecturas": [
      {"sensor_id": "UUID-SENSOR", "valor": 42.5, "unidad": "%"}
    ]
  }'
```

Respuesta esperada: `{"status": "ok", "recibidas": 1, ...}`

### Test del dashboard:

Abrir en el navegador:
`https://uptime-ia.vercel.app`

Debe mostrar el panel con selector de plantas.

### Test del agente WhatsApp:

Desde el número registrado en el sandbox, enviar:
```
uptime2024
```
Respuesta esperada: mensaje de bienvenida de UPTIME.

---

## Endpoints disponibles

| Endpoint                        | Método | Función                        |
|---------------------------------|--------|-------------------------------|
| `/api/gateway`                  | POST   | Recibir lecturas del ESP32    |
| `/api/gateway`                  | GET    | Entregar comandos al ESP32    |
| `/api/gateway/boot`             | POST   | Registro de arranque ESP32    |
| `/api/gateway/ack`              | POST   | Confirmar comando ejecutado   |
| `/api/analyzer`                 | GET    | Detectar anomalías (cron)     |
| `/api/alerts`                   | GET    | Escalar alarmas (cron)        |
| `/api/shifts`                   | POST   | Iniciar/cerrar turno          |
| `/api/shifts`                   | GET    | Estado del turno activo       |
| `/api/uptime`                   | POST   | Webhook WhatsApp              |
| `/api/dashboard`                | GET    | Estado general planta         |
| `/api/dashboard/plantas`        | GET    | Lista de plantas              |
| `/api/dashboard/dispositivos`   | GET    | Dispositivos por planta       |
| `/api/dashboard/alarmas`        | GET    | Alarmas activas               |
| `/api/dashboard/historial`      | GET    | Lecturas de un sensor         |
| `/api/dashboard/turno`          | GET    | Turno activo                  |

---

## Costos mensuales estimados

| Servicio        | Plan              | Costo       |
|-----------------|-------------------|-------------|
| Vercel          | Hobby (gratuito)  | $0          |
| Supabase        | Free tier         | $0          |
| Groq            | Free tier         | $0 (límite generoso) |
| Twilio WhatsApp | Sandbox (pruebas) | $0          |
| cron-job.org    | Gratuito          | $0          |
| **TOTAL MVP**   |                   | **$0/mes**  |

Para producción con clientes reales:
- Vercel Pro: $20/mes
- Supabase Pro: $25/mes
- Twilio WhatsApp Business: ~$0.005/mensaje
- **Total producción**: ~$50/mes para los primeros 10 clientes

---

## Próximo paso — ESP32

Con el deployment funcionando, el siguiente paso es:
1. Flashear MicroPython en el ESP32
2. Configurar `config.py` con el `GATEWAY_ID` y `GATEWAY_SECRET`
3. Conectar el primer sensor 4-20mA
4. Verificar que las lecturas llegan a Supabase
