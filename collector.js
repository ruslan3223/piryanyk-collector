// Збирач даних: слухає MQTT і пише T7/T8 у Supabase, навіть коли сайт закритий.
// Запуск постійно:   node collector.js
// Запуск на N секунд (для GitHub Actions):   RUN_SECONDS=90 node collector.js
//
// npm i mqtt @supabase/supabase-js
//
// Змінні середовища:
//   SUPABASE_URL, SUPABASE_SERVICE_KEY  (service_role / secret key з Supabase -> Settings -> API)
//   MQTT_URL, MQTT_USER, MQTT_PASS      (необов'язково, є значення за замовчуванням нижче)

const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const MQTT_URL  = process.env.MQTT_URL  || 'wss://se6619d9.ala.eu-central-1.emqxsl.com:8084/mqtt';
const MQTT_USER = process.env.MQTT_USER || 'esp32_asic';
const MQTT_PASS = process.env.MQTT_PASS;
const TOPIC_BASE = 'home/esp';

const SAVE_INTERVAL_MS = 15000;          // як часто писати кожен датчик
const KEEP_MS = 26 * 3600 * 1000;        // скільки зберігати історію
const CLEANUP_EVERY_MS = 3600 * 1000;    // чистка старого раз на годину
const TEMP_OFFSETS = { 6: -8, 8: -7 };   // та сама корекція, що й на сайті
const SENSORS = [5,6,7, 8];                  // що пишемо в базу
const RUN_SECONDS = parseInt(process.env.RUN_SECONDS || '0', 10);

if (!SUPABASE_URL || !SUPABASE_KEY || !MQTT_PASS) {
  console.error('Потрібні SUPABASE_URL, SUPABASE_SERVICE_KEY, MQTT_PASS');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const lastSave = {};
let buffer = [];

async function flush() {
  if (!buffer.length) return;
  const rows = buffer;
  buffer = [];
  const { error } = await supabase.from('temp_readings').insert(rows);
  if (error) {
    console.error('Supabase insert error:', error.message);
    buffer = rows.concat(buffer).slice(-500); // спробуємо ще раз пізніше
  } else {
    console.log(new Date().toISOString(), 'saved', rows.length, 'rows');
  }
}

async function cleanup() {
  const cutoff = new Date(Date.now() - KEEP_MS).toISOString();
  const { error } = await supabase.from('temp_readings').delete().lt('created_at', cutoff);
  if (error) console.error('Cleanup error:', error.message);
}

const client = mqtt.connect(MQTT_URL, {
  clientId: 'collector-' + Math.random().toString(16).slice(2, 10),
  username: MQTT_USER,
  password: MQTT_PASS,
  clean: true,
  reconnectPeriod: 3000,
  connectTimeout: 8000,
});

client.on('connect', () => {
  console.log('MQTT connected');
  client.subscribe(TOPIC_BASE + '/+');
});
client.on('error', (e) => console.error('MQTT error', e.message));

client.on('message', (topic, payload) => {
  const m = topic.split('/').pop().match(/^temp(\d+)$/i);
  if (!m) return;
  const idx = parseInt(m[1], 10);
  if (!SENSORS.includes(idx)) return;
  const raw = parseFloat(payload.toString());
  if (isNaN(raw)) return;
  const now = Date.now();
  if (now - (lastSave[idx] || 0) < SAVE_INTERVAL_MS) return;
  lastSave[idx] = now;
  buffer.push({ sensor: idx, temperature: raw + (TEMP_OFFSETS[idx] || 0) });
});

setInterval(flush, 10000);
setInterval(cleanup, CLEANUP_EVERY_MS);
cleanup();

if (RUN_SECONDS > 0) {
  setTimeout(async () => {
    await flush();
    client.end(true, () => process.exit(0));
  }, RUN_SECONDS * 1000);
}
