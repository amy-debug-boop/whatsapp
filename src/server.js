// Read-only WhatsApp MCP connector.
// Links to your own WhatsApp as a "linked device", stores text messages in SQLite,
// and exposes search tools to Claude over MCP (Streamable HTTP).

import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import Database from 'better-sqlite3';
import QRCode from 'qrcode';
import pino from 'pino';
import { z } from 'zod';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';

const require = createRequire(import.meta.url);
const baileys = require('@whiskeysockets/baileys');
const makeWASocket = baileys.makeWASocket ?? baileys.default;
const { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers } = baileys;

// ---------- config ----------
const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.DATA_DIR || './data';
const TIMEZONE = process.env.TIMEZONE || 'UTC';
const AUTH_SECRET = process.env.AUTH_SECRET || '';
const AUTH_DIR = path.join(DATA_DIR, 'auth');
const logger = pino({ level: process.env.LOG_LEVEL || 'warn' });

if (AUTH_SECRET.length < 16) {
  console.error('AUTH_SECRET must be set and at least 16 characters long.');
  process.exit(1);
}
fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------- storage ----------
const db = new Database(path.join(DATA_DIR, 'messages.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS messages (
    chat_jid TEXT NOT NULL,
    id TEXT NOT NULL,
    sender_jid TEXT,
    sender_name TEXT,
    from_me INTEGER NOT NULL DEFAULT 0,
    ts INTEGER NOT NULL,
    text TEXT NOT NULL,
    PRIMARY KEY (chat_jid, id)
  );
  CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (chat_jid, ts);
  CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts);
  CREATE TABLE IF NOT EXISTS names (
    jid TEXT PRIMARY KEY,
    name TEXT NOT NULL
  );
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (chat_jid, id, sender_jid, sender_name, from_me, ts, text)
  VALUES (@chat_jid, @id, @sender_jid, @sender_name, @from_me, @ts, @text)
  ON CONFLICT (chat_jid, id) DO UPDATE SET text = excluded.text
`);
// Names from your address book or chat titles win over WhatsApp display names.
const setNameStrong = db.prepare(`
  INSERT INTO names (jid, name) VALUES (?, ?)
  ON CONFLICT (jid) DO UPDATE SET name = excluded.name
`);
const setNameWeak = db.prepare(`INSERT OR IGNORE INTO names (jid, name) VALUES (?, ?)`);

const SKIP_TYPES = new Set([
  'protocolMessage', 'reactionMessage', 'senderKeyDistributionMessage',
  'messageContextInfo', 'pollUpdateMessage', 'keepInChatMessage',
]);

function unwrap(content) {
  let c = content;
  for (let i = 0; i < 5 && c; i++) {
    const inner = c.ephemeralMessage?.message || c.viewOnceMessage?.message
      || c.viewOnceMessageV2?.message || c.documentWithCaptionMessage?.message
      || c.editedMessage?.message;
    if (!inner) break;
    c = inner;
  }
  return c;
}

function extractText(content) {
  const c = unwrap(content);
  if (!c) return null;
  const text = c.conversation || c.extendedTextMessage?.text || c.imageMessage?.caption
    || c.videoMessage?.caption || c.documentMessage?.caption
    || c.buttonsResponseMessage?.selectedDisplayText || c.listResponseMessage?.title;
  if (text) return text;
  if (c.locationMessage) {
    const l = c.locationMessage;
    return `[location] ${l.name || ''} ${l.address || ''} (${l.degreesLatitude}, ${l.degreesLongitude})`.trim();
  }
  if (c.contactMessage) return `[contact card] ${c.contactMessage.displayName || ''}`.trim();
  const type = Object.keys(c).find((k) => !SKIP_TYPES.has(k));
  if (!type) return null;
  return `[${type.replace(/Message$/, '')}]`;
}

function toSeconds(t) {
  if (t == null) return Math.floor(Date.now() / 1000);
  if (typeof t === 'object' && typeof t.toNumber === 'function') return t.toNumber();
  return Number(t);
}

const saveMessages = db.transaction((msgs) => {
  for (const m of msgs || []) {
    const chat = m.key?.remoteJid;
    if (!chat || chat === 'status@broadcast' || !m.message) continue;
    const text = extractText(m.message);
    if (!text) continue;
    const fromMe = m.key.fromMe ? 1 : 0;
    const sender = fromMe ? 'me' : (m.key.participant || chat);
    if (!fromMe && m.pushName) setNameWeak.run(sender, m.pushName);
    insertMessage.run({
      chat_jid: chat,
      id: m.key.id,
      sender_jid: sender,
      sender_name: fromMe ? 'Me' : (m.pushName || null),
      from_me: fromMe,
      ts: toSeconds(m.messageTimestamp),
      text,
    });
  }
});

const saveContacts = db.transaction((contacts) => {
  for (const c of contacts || []) {
    if (!c?.id) continue;
    if (c.name) setNameStrong.run(c.id, c.name);
    else if (c.verifiedName) setNameWeak.run(c.id, c.verifiedName);
    else if (c.notify) setNameWeak.run(c.id, c.notify);
  }
});

const saveChats = db.transaction((chats) => {
  for (const c of chats || []) {
    if (c?.id && c.name) setNameStrong.run(c.id, c.name);
  }
});

// ---------- WhatsApp connection ----------
let sock = null;
let status = 'starting';
let latestQR = null;
let pairingCode = null;
let linkedAs = null;
let restartTimer = null;

async function startWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const config = {
    auth: state,
    logger,
    browser: Browsers.macOS('Desktop'), // desktop identity gets the fullest history sync
    syncFullHistory: true,
    markOnlineOnConnect: false,          // don't change your "online" status
    printQRInTerminal: false,
  };
  try {
    const { version } = await fetchLatestBaileysVersion();
    if (version) config.version = version;
  } catch { /* fall back to the library default */ }

  sock = makeWASocket(config);
  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (u) => {
    if (u.qr) {
      latestQR = u.qr;
      status = 'waiting_for_link';
    }
    if (u.connection === 'open') {
      status = 'connected';
      latestQR = null;
      pairingCode = null;
      linkedAs = sock.user?.id || null;
      logger.warn(`WhatsApp connected as ${linkedAs}`);
    }
    if (u.connection === 'close') {
      const code = u.lastDisconnect?.error?.output?.statusCode;
      pairingCode = null;
      if (code === DisconnectReason.loggedOut) {
        status = 'logged_out';
        linkedAs = null;
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
      } else {
        status = 'reconnecting';
      }
      clearTimeout(restartTimer);
      restartTimer = setTimeout(() => startWhatsApp().catch((e) => logger.error(e)), 3000);
    }
  });

  sock.ev.on('messaging-history.set', ({ chats, contacts, messages }) => {
    saveContacts(contacts);
    saveChats(chats);
    saveMessages(messages);
  });
  sock.ev.on('messages.upsert', ({ messages }) => saveMessages(messages));
  sock.ev.on('contacts.upsert', saveContacts);
  sock.ev.on('contacts.update', saveContacts);
  sock.ev.on('chats.upsert', saveChats);
}

// ---------- query helpers ----------
const fmt = new Intl.DateTimeFormat('en-US', {
  timeZone: TIMEZONE, year: 'numeric', month: 'short', day: '2-digit',
  weekday: 'short', hour: '2-digit', minute: '2-digit',
});
const fmtTime = (ts) => fmt.format(new Date(ts * 1000));

function parseDate(s, endOfDay = false) {
  if (!s) return null;
  const d = new Date(/^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T${endOfDay ? '23:59:59' : '00:00:00'}Z` : s);
  if (Number.isNaN(d.getTime())) throw new Error(`Couldn't read the date "${s}". Use YYYY-MM-DD.`);
  return Math.floor(d.getTime() / 1000);
}

function nameFor(jid) {
  return db.prepare('SELECT name FROM names WHERE jid = ?').get(jid)?.name || null;
}

// Finds chats by name, phone number, or exact JID.
function findChats(query) {
  const q = query.trim();
  if (q.includes('@')) return [{ jid: q, name: nameFor(q) }];
  const digits = q.replace(/\D/g, '');
  const like = `%${q}%`;
  const rows = db.prepare(`
    SELECT jid, name FROM (
      SELECT n.jid AS jid, n.name AS name FROM names n WHERE n.name LIKE @like
      UNION
      SELECT m.chat_jid AS jid, MAX(m.sender_name) AS name FROM messages m
        WHERE m.sender_name LIKE @like GROUP BY m.chat_jid
      UNION
      SELECT DISTINCT m.chat_jid AS jid, NULL AS name FROM messages m
        WHERE @digits <> '' AND length(@digits) >= 6 AND m.chat_jid LIKE '%' || @digits || '%'
    )
    WHERE jid IN (SELECT DISTINCT chat_jid FROM messages)
    GROUP BY jid
  `).all({ like, digits });
  return rows.map((r) => ({ jid: r.jid, name: nameFor(r.jid) || r.name }));
}

function renderMessages(rows) {
  if (!rows.length) return 'No messages found.';
  return rows.map((r) => {
    const who = r.from_me ? 'Me' : (nameFor(r.sender_jid) || r.sender_name || r.sender_jid);
    const chat = r.chat_jid.endsWith('@g.us') ? ` [group: ${nameFor(r.chat_jid) || r.chat_jid}]` : '';
    return `${fmtTime(r.ts)}${chat} | ${who}: ${r.text}`;
  }).join('\n');
}

const text = (t) => ({ content: [{ type: 'text', text: t }] });
const notConnectedNote = () => (status === 'connected'
  ? '' : `\n\n(Note: WhatsApp is currently "${status}", so recent messages may be missing.)`);

// ---------- MCP server ----------
function buildMcpServer() {
  const server = new McpServer({ name: 'whatsapp-reader', version: '1.0.0' });
  const readOnly = { readOnlyHint: true, openWorldHint: false };

  server.registerTool('whatsapp_status', {
    title: 'WhatsApp status',
    description: 'Shows whether WhatsApp is linked and how many messages are stored.',
    annotations: readOnly,
  }, async () => {
    const count = db.prepare('SELECT COUNT(*) AS n FROM messages').get().n;
    const range = db.prepare('SELECT MIN(ts) AS a, MAX(ts) AS b FROM messages').get();
    return text([
      `Status: ${status}`,
      `Linked as: ${linkedAs || 'not linked'}`,
      `Stored messages: ${count}`,
      count ? `Oldest: ${fmtTime(range.a)} | Newest: ${fmtTime(range.b)}` : '',
      `Times shown in: ${TIMEZONE}`,
    ].filter(Boolean).join('\n'));
  });

  server.registerTool('whatsapp_find_chats', {
    title: 'Find WhatsApp chats',
    description: 'Finds chats and contacts by name or phone number. Use this first to get the chat ID for a person or group.',
    inputSchema: { query: z.string().min(1).describe('Name or phone number, e.g. "Dani Eldas" or "+504 9999"') },
    annotations: readOnly,
  }, async ({ query }) => {
    const chats = findChats(query);
    if (!chats.length) return text(`No chats matched "${query}".${notConnectedNote()}`);
    const lines = chats.map((c) => {
      const s = db.prepare('SELECT COUNT(*) AS n, MAX(ts) AS last FROM messages WHERE chat_jid = ?').get(c.jid);
      return `${c.name || '(no name)'} | chat_id: ${c.jid} | ${s.n} messages | last: ${s.last ? fmtTime(s.last) : 'n/a'}`;
    });
    return text(lines.join('\n'));
  });

  server.registerTool('whatsapp_read_chat', {
    title: 'Read a WhatsApp chat',
    description: 'Reads messages in one chat, oldest first. Pass a chat_id from whatsapp_find_chats, or a name.',
    inputSchema: {
      chat: z.string().min(1).describe('chat_id (e.g. 50499999999@s.whatsapp.net) or a contact/group name'),
      since: z.string().optional().describe('Start date, YYYY-MM-DD'),
      until: z.string().optional().describe('End date, YYYY-MM-DD (inclusive)'),
      limit: z.number().int().min(1).max(1000).optional().describe('Max messages, default 300 (most recent in range)'),
    },
    annotations: readOnly,
  }, async ({ chat, since, until, limit = 300 }) => {
    const matches = findChats(chat);
    if (!matches.length) return text(`No chat matched "${chat}".${notConnectedNote()}`);
    if (matches.length > 1 && !chat.includes('@')) {
      return text(`Several chats match "${chat}". Pick one chat_id:\n${matches.map((m) => `${m.name || '(no name)'} | ${m.jid}`).join('\n')}`);
    }
    const jid = matches[0].jid;
    const rows = db.prepare(`
      SELECT * FROM messages WHERE chat_jid = @jid AND ts >= @since AND ts <= @until
      ORDER BY ts DESC LIMIT @limit
    `).all({ jid, since: parseDate(since) ?? 0, until: parseDate(until, true) ?? 9e12, limit }).reverse();
    return text(`Chat: ${matches[0].name || jid}\n\n${renderMessages(rows)}${notConnectedNote()}`);
  });

  server.registerTool('whatsapp_search_messages', {
    title: 'Search WhatsApp messages',
    description: 'Searches message text across all chats, or within one chat. Matches any of the space-separated words unless match_all is true.',
    inputSchema: {
      query: z.string().min(1).describe('Words to look for, e.g. "tour october"'),
      chat: z.string().optional().describe('Limit to one chat: chat_id or name'),
      match_all: z.boolean().optional().describe('Require every word (default false)'),
      since: z.string().optional().describe('Start date, YYYY-MM-DD'),
      until: z.string().optional().describe('End date, YYYY-MM-DD (inclusive)'),
      limit: z.number().int().min(1).max(500).optional().describe('Max results, default 100'),
    },
    annotations: readOnly,
  }, async ({ query, chat, match_all = false, since, until, limit = 100 }) => {
    const words = query.split(/\s+/).filter(Boolean).slice(0, 10);
    const params = { since: parseDate(since) ?? 0, until: parseDate(until, true) ?? 9e12, limit };
    const clauses = words.map((w, i) => { params[`w${i}`] = `%${w}%`; return `text LIKE @w${i}`; });
    let chatFilter = '';
    if (chat) {
      const jids = findChats(chat).map((c) => c.jid);
      if (!jids.length) return text(`No chat matched "${chat}".`);
      chatFilter = `AND chat_jid IN (${jids.map((j, i) => { params[`c${i}`] = j; return `@c${i}`; }).join(',')})`;
    }
    const rows = db.prepare(`
      SELECT * FROM messages
      WHERE (${clauses.join(match_all ? ' AND ' : ' OR ')}) ${chatFilter}
        AND ts >= @since AND ts <= @until
      ORDER BY ts DESC LIMIT @limit
    `).all(params).reverse();
    return text(`${renderMessages(rows)}${notConnectedNote()}`);
  });

  return server;
}

// ---------- HTTP ----------
function secretOk(given) {
  const a = Buffer.from(String(given || ''));
  const b = Buffer.from(AUTH_SECRET);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '4mb' }));
app.use(express.urlencoded({ extended: false }));

app.get('/health', (_req, res) => res.json({ ok: true }));

// MCP endpoint. The secret lives in the URL because Claude's custom connector form takes a URL.
app.post('/mcp/:secret', async (req, res) => {
  if (!secretOk(req.params.secret)) return res.status(404).end();
  const server = buildMcpServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  res.on('close', () => { transport.close(); server.close(); });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    logger.error(err);
    if (!res.headersSent) {
      res.status(500).json({ jsonrpc: '2.0', error: { code: -32603, message: 'Internal error' }, id: null });
    }
  }
});
app.all('/mcp/:secret', (_req, res) => res.status(405).set('Allow', 'POST').end());

// Linking page: shows a QR code, or a pairing code for linking from the same phone.
// It polls /state in the background so the phone-number field is never wiped.
const page = (s) => `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Link WhatsApp</title>
<style>
 body{font-family:-apple-system,system-ui,Segoe UI,Roboto,sans-serif;max-width:480px;margin:0 auto;padding:24px;color:#1a1a1a;background:#fafafa}
 h1{font-size:22px} .card{background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:16px}
 img{width:100%;max-width:300px;display:block;margin:0 auto} .code{font-size:34px;letter-spacing:4px;text-align:center;font-weight:700}
 input{width:100%;box-sizing:border-box;padding:12px;font-size:16px;border:1px solid #ccc;border-radius:8px}
 button{margin-top:10px;width:100%;padding:12px;font-size:16px;border:0;border-radius:8px;background:#25D366;color:#fff;font-weight:600}
 small{color:#666} .hide{display:none}
</style></head><body><h1>Link WhatsApp</h1>
<div class="card" id="statusCard"><p>Status: <b id="status">loading…</b></p></div>
<div class="card hide" id="doneCard"><p>✅ Linked as <b id="linkedAs"></b>.</p>
 <p><span id="count">0</span> messages stored. Older history keeps arriving for a few minutes after linking.</p></div>
<div class="card hide" id="pairCard"><p>On your phone: WhatsApp → Settings → Linked devices → Link a device → <b>Link with phone number instead</b>, then enter:</p>
 <p class="code" id="pairCode"></p><small>This page will show ✅ once linked.</small></div>
<div class="card hide" id="qrCard"><p><b>Option A, scan from another screen:</b> WhatsApp → Settings → Linked devices → Link a device, then scan.</p>
 <img id="qr" alt="WhatsApp QR code"><small>The code changes every ~20 seconds and updates here on its own.</small></div>
<div class="card hide" id="formCard"><p><b>Option B, only have your phone?</b> Enter your WhatsApp number with country code to get an 8-character code.</p>
 <form id="pairForm"><input id="phone" placeholder="+504 9999 9999" inputmode="tel" required>
 <button type="submit">Get pairing code</button></form><small id="err"></small></div>
<script>
const base = '/link/${s}';
const $ = (id) => document.getElementById(id);
const show = (id, on) => $(id).classList.toggle('hide', !on);
async function poll() {
  try {
    const st = await (await fetch(base + '/state', { cache: 'no-store' })).json();
    $('status').textContent = st.status;
    const done = st.status === 'connected';
    show('statusCard', !done); show('doneCard', done);
    $('linkedAs').textContent = st.linkedAs || ''; $('count').textContent = st.count;
    show('pairCard', !done && !!st.pairingCode); $('pairCode').textContent = st.pairingCode || '';
    show('qrCard', !done && !st.pairingCode && !!st.qr); if (st.qr) $('qr').src = st.qr;
    show('formCard', !done && !st.pairingCode);
  } catch (e) {}
}
$('pairForm').addEventListener('submit', async (e) => {
  e.preventDefault(); $('err').textContent = 'Requesting code…';
  const r = await fetch(base + '/pair', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ phone: $('phone').value }) });
  const j = await r.json().catch(() => ({}));
  $('err').textContent = r.ok ? '' : (j.error || 'Something went wrong, try again.');
  poll();
});
poll(); setInterval(poll, 4000);
</script></body></html>`;

app.get('/link/:secret', (req, res) => {
  if (!secretOk(req.params.secret)) return res.status(404).end();
  res.set('Cache-Control', 'no-store').send(page(req.params.secret));
});

app.get('/link/:secret/state', async (req, res) => {
  if (!secretOk(req.params.secret)) return res.status(404).end();
  const count = db.prepare('SELECT COUNT(*) AS n FROM messages').get().n;
  const qr = latestQR && status !== 'connected' ? await QRCode.toDataURL(latestQR, { margin: 1, width: 300 }) : null;
  res.set('Cache-Control', 'no-store').json({ status, linkedAs, count, qr, pairingCode });
});

app.post('/link/:secret/pair', async (req, res) => {
  if (!secretOk(req.params.secret)) return res.status(404).end();
  const phone = String(req.body?.phone || '').replace(/\D/g, '');
  if (phone.length < 8) return res.status(400).json({ error: 'Enter the full number with country code.' });
  if (!sock || sock.authState?.creds?.registered) return res.status(409).json({ error: 'Already linked or not ready yet.' });
  try {
    const code = await sock.requestPairingCode(phone);
    pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
    res.json({ pairingCode });
  } catch (err) {
    logger.error(err);
    res.status(500).json({ error: `Couldn't get a code: ${String(err.message || err)}` });
  }
});

app.listen(PORT, () => console.log(`Listening on :${PORT}`));
startWhatsApp().catch((e) => logger.error(e));

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => { try { db.close(); } catch {} process.exit(0); });
}
