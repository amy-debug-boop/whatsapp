// Read-only WhatsApp MCP connector.
// Links to your own WhatsApp as a "linked device", stores text messages in Supabase (Postgres),
// and exposes search tools to Claude over MCP (Streamable HTTP).

import express from 'express';
import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import pg from 'pg';
import QRCode from 'qrcode';
import pino from 'pino';
import { z } from 'zod';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';

const require = createRequire(import.meta.url);
const baileys = require('baileys');
const makeWASocket = baileys.makeWASocket ?? baileys.default;
const {
  DisconnectReason, fetchLatestWaWebVersion, Browsers, BufferJSON, initAuthCreds, proto,
  makeCacheableSignalKeyStore,
} = baileys;

// ---------- config ----------
const PORT = Number(process.env.PORT || 3000);
const TIMEZONE = process.env.TIMEZONE || 'UTC';
const AUTH_SECRET = process.env.AUTH_SECRET || '';
const DATABASE_URL = process.env.DATABASE_URL || '';
const SCHEMA = 'whatsapp'; // private schema: Supabase's public API only exposes "public"
const logger = pino({ level: process.env.LOG_LEVEL || 'warn' });

if (AUTH_SECRET.length < 16) {
  console.error('AUTH_SECRET must be set and at least 16 characters long.');
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error('DATABASE_URL must be set to your Supabase Postgres connection string.');
  process.exit(1);
}

// ---------- storage (Supabase Postgres) ----------
const isLocal = /@(localhost|127\.0\.0\.1)[:/]/.test(DATABASE_URL);
const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: isLocal ? false : { rejectUnauthorized: false },
  max: 5,
});
pool.on('error', (e) => logger.error({ err: e }, 'postgres pool error'));
const q = (text, params) => pool.query(text, params);

async function initDb() {
  await q(`
    CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
    CREATE TABLE IF NOT EXISTS ${SCHEMA}.messages (
      chat_jid TEXT NOT NULL,
      id TEXT NOT NULL,
      sender_jid TEXT,
      sender_name TEXT,
      from_me BOOLEAN NOT NULL DEFAULT FALSE,
      ts BIGINT NOT NULL,
      text TEXT NOT NULL,
      PRIMARY KEY (chat_jid, id)
    );
    CREATE INDEX IF NOT EXISTS messages_chat_ts ON ${SCHEMA}.messages (chat_jid, ts);
    CREATE INDEX IF NOT EXISTS messages_ts ON ${SCHEMA}.messages (ts);
    CREATE TABLE IF NOT EXISTS ${SCHEMA}.names (
      jid TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ${SCHEMA}.auth (
      id TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);
}

// WhatsApp login state, stored in Postgres instead of files.
async function usePostgresAuthState() {
  const read = async (id) => {
    const r = await q(`SELECT value FROM ${SCHEMA}.auth WHERE id = $1`, [id]);
    return r.rows[0] ? JSON.parse(r.rows[0].value, BufferJSON.reviver) : null;
  };
  const write = (id, data) => q(
    `INSERT INTO ${SCHEMA}.auth (id, value) VALUES ($1, $2)
     ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value`,
    [id, JSON.stringify(data, BufferJSON.replacer)],
  );
  const creds = (await read('creds')) || initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const keys = ids.map((id) => `${type}-${id}`);
          const r = await q(`SELECT id, value FROM ${SCHEMA}.auth WHERE id = ANY($1)`, [keys]);
          const found = new Map(r.rows.map((row) => [row.id, row.value]));
          const out = {};
          for (const id of ids) {
            const raw = found.get(`${type}-${id}`);
            let value = raw ? JSON.parse(raw, BufferJSON.reviver) : null;
            if (value && type === 'app-state-sync-key') value = proto.Message.AppStateSyncKeyData.fromObject(value);
            out[id] = value;
          }
          return out;
        },
        set: async (data) => {
          const upserts = [];
          const deletes = [];
          for (const category of Object.keys(data)) {
            for (const id of Object.keys(data[category])) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              if (value) upserts.push([key, JSON.stringify(value, BufferJSON.replacer)]);
              else deletes.push(key);
            }
          }
          if (upserts.length) {
            await q(
              `INSERT INTO ${SCHEMA}.auth (id, value)
               SELECT * FROM unnest($1::text[], $2::text[])
               ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value`,
              [upserts.map((u) => u[0]), upserts.map((u) => u[1])],
            );
          }
          if (deletes.length) await q(`DELETE FROM ${SCHEMA}.auth WHERE id = ANY($1)`, [deletes]);
        },
      },
    },
    saveCreds: () => write('creds', creds),
  };
}
const clearAuth = () => q(`DELETE FROM ${SCHEMA}.auth`);

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

// strong = address-book or chat title (overwrites); weak = WhatsApp display name (only if none yet)
async function saveNames(pairs, strong) {
  const seen = new Map();
  for (const [jid, name] of pairs) if (jid && name) seen.set(jid, String(name));
  if (!seen.size) return;
  await q(
    `INSERT INTO ${SCHEMA}.names (jid, name)
     SELECT * FROM unnest($1::text[], $2::text[])
     ON CONFLICT (jid) DO ${strong ? 'UPDATE SET name = EXCLUDED.name' : 'NOTHING'}`,
    [[...seen.keys()], [...seen.values()]],
  );
}

async function saveMessages(msgs) {
  const rows = new Map();
  const pushNames = [];
  for (const m of msgs || []) {
    const chat = m.key?.remoteJid;
    if (!chat || chat === 'status@broadcast' || !m.message || !m.key.id) continue;
    const text = extractText(m.message);
    if (!text) continue;
    const fromMe = !!m.key.fromMe;
    const sender = fromMe ? 'me' : (m.key.participant || chat);
    if (!fromMe && m.pushName) pushNames.push([sender, m.pushName]);
    rows.set(`${chat}|${m.key.id}`, [chat, m.key.id, sender, fromMe ? 'Me' : (m.pushName || null),
      fromMe, toSeconds(m.messageTimestamp), text.replace(/\u0000/g, '')]);
  }
  const all = [...rows.values()];
  for (let i = 0; i < all.length; i += 500) {
    const chunk = all.slice(i, i + 500);
    const col = (k) => chunk.map((r) => r[k]);
    await q(
      `INSERT INTO ${SCHEMA}.messages (chat_jid, id, sender_jid, sender_name, from_me, ts, text)
       SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bool[], $6::bigint[], $7::text[])
       ON CONFLICT (chat_jid, id) DO UPDATE SET text = EXCLUDED.text`,
      [col(0), col(1), col(2), col(3), col(4), col(5), col(6)],
    );
  }
  await saveNames(pushNames, false);
}

async function saveContacts(contacts) {
  const strong = [];
  const weak = [];
  for (const c of contacts || []) {
    if (!c?.id) continue;
    if (c.name) strong.push([c.id, c.name]);
    else if (c.verifiedName || c.notify) weak.push([c.id, c.verifiedName || c.notify]);
  }
  await saveNames(strong, true);
  await saveNames(weak, false);
}

const saveChats = (chats) => saveNames((chats || []).filter((c) => c?.id && c.name).map((c) => [c.id, c.name]), true);

// Never let a storage error crash the WhatsApp connection.
const safe = (label, fn) => (arg) => fn(arg).catch((err) => logger.error({ err }, `failed to save ${label}`));

// ---------- WhatsApp connection ----------
let sock = null;
let status = 'starting';
let latestQR = null;
let pairingCode = null;
let linkedAs = null;
let restartTimer = null;
let failedConnects = 0; // back off so a rejected connection doesn't hammer WhatsApp every few seconds
let waVersionLogged = false;

// WhatsApp hangs up right after registration on clients reporting an outdated web version, so never send
// anything older than this known-good one (see WhiskeySockets/Baileys#2777). WA_VERSION overrides it all.
const MIN_WA_VERSION = [2, 3000, 1045716975];
const newer = (a, b) => (a[0] - b[0] || a[1] - b[1] || a[2] - b[2]) >= 0 ? a : b;
let lastLiveVersion = null; // the lookup is flaky, so remember the last version it returned

async function pickWaVersion() {
  const override = process.env.WA_VERSION?.split('.').map(Number);
  let version;
  let source;
  if (override?.length === 3 && override.every(Number.isFinite)) {
    version = override;
    source = 'WA_VERSION';
  } else {
    let problem;
    try {
      const r = await fetchLatestWaWebVersion();
      if (r.isLatest) { version = lastLiveVersion = r.version; source = 'web.whatsapp.com'; }
      else problem = r.error?.message || JSON.stringify(r.error);
    } catch (err) {
      problem = err.message;
    }
    if (!version && lastLiveVersion) {
      version = lastLiveVersion;
      source = 'last live lookup';
    }
    if (!version) {
      version = MIN_WA_VERSION;
      source = `known-good fallback (live lookup failed: ${problem})`;
    }
    version = newer(version, MIN_WA_VERSION);
  }
  if (!waVersionLogged) { logger.warn(`Using WhatsApp Web version ${version.join('.')} from ${source}`); waVersionLogged = true; }
  return version;
}

let linkReady = false; // current socket has reached the QR stage, so it can hand out a pairing code

// Wait until the current socket can accept a pairing-code request (it reconnects every so often while unlinked).
async function waitForLinkReady(skip = null, timeoutMs = 20000) {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (sock && sock !== skip && linkReady) return sock;
    await new Promise((r) => setTimeout(r, 250));
  }
  return null;
}

async function startWhatsApp() {
  const { state, saveCreds } = await usePostgresAuthState();
  // requestPairingCode saves your number as creds.me before the phone confirms. If that pairing never
  // finished (no creds.account), Baileys would keep trying to log in as a linked device and WhatsApp
  // rejects it before ever offering a QR or pairing code. Drop the half-finished pairing and start fresh.
  if (state.creds.me && !state.creds.account) {
    logger.warn('Clearing an unfinished pairing attempt');
    delete state.creds.me;
    delete state.creds.pairingCode;
    await saveCreds();
  }
  const config = {
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
    logger,
    browser: Browsers.macOS('Desktop'), // desktop identity gets the fullest history sync
    syncFullHistory: true,
    markOnlineOnConnect: false,          // don't change your "online" status
    printQRInTerminal: false,
  };
  config.version = await pickWaVersion();

  linkReady = false;
  sock = makeWASocket(config);
  sock.ev.on('creds.update', safe('creds', saveCreds));

  sock.ev.on('connection.update', async (u) => {
    if (u.qr) {
      latestQR = u.qr;
      linkReady = true;
      failedConnects = 0;
      status = 'waiting_for_link';
    }
    if (u.connection === 'open') {
      status = 'connected';
      failedConnects = 0;
      latestQR = null;
      pairingCode = null;
      linkedAs = sock.user?.id || null;
      logger.warn(`WhatsApp connected as ${linkedAs}`);
    }
    if (u.connection === 'close') {
      linkReady = false;
      const code = u.lastDisconnect?.error?.output?.statusCode;
      logger.warn(`WhatsApp connection closed (${code ?? 'no code'}): ${u.lastDisconnect?.error?.message || ''}`);
      pairingCode = null;
      if (code === DisconnectReason.loggedOut) {
        status = 'logged_out';
        linkedAs = null;
        await clearAuth().catch((err) => logger.error({ err }, 'failed to clear auth'));
      } else {
        status = 'reconnecting';
      }
      // 3s, 6s, 12s ... up to 5 minutes while WhatsApp keeps refusing us; reset once it offers a QR or connects.
      const delay = Math.min(3000 * 2 ** failedConnects, 5 * 60 * 1000);
      failedConnects++;
      clearTimeout(restartTimer);
      restartTimer = setTimeout(() => startWhatsApp().catch((e) => logger.error(e)), delay);
    }
  });

  sock.ev.on('messaging-history.set', safe('history', async ({ chats, contacts, messages }) => {
    await saveContacts(contacts);
    await saveChats(chats);
    await saveMessages(messages);
  }));
  sock.ev.on('messages.upsert', safe('messages', ({ messages }) => saveMessages(messages)));
  sock.ev.on('contacts.upsert', safe('contacts', saveContacts));
  sock.ev.on('contacts.update', safe('contacts', saveContacts));
  sock.ev.on('chats.upsert', safe('chats', saveChats));
}

// ---------- query helpers ----------
const fmt = new Intl.DateTimeFormat('en-US', {
  timeZone: TIMEZONE, year: 'numeric', month: 'short', day: '2-digit',
  weekday: 'short', hour: '2-digit', minute: '2-digit',
});
const fmtTime = (ts) => fmt.format(new Date(Number(ts) * 1000));

function parseDate(s, endOfDay = false) {
  if (!s) return null;
  const d = new Date(/^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T${endOfDay ? '23:59:59' : '00:00:00'}Z` : s);
  if (Number.isNaN(d.getTime())) throw new Error(`Couldn't read the date "${s}". Use YYYY-MM-DD.`);
  return Math.floor(d.getTime() / 1000);
}

async function namesFor(jids) {
  if (!jids.length) return new Map();
  const r = await q(`SELECT jid, name FROM ${SCHEMA}.names WHERE jid = ANY($1)`, [[...new Set(jids)]]);
  return new Map(r.rows.map((x) => [x.jid, x.name]));
}

// Finds chats by name, phone number, or exact JID.
async function findChats(query) {
  const term = query.trim();
  if (term.includes('@')) return [{ jid: term, name: (await namesFor([term])).get(term) || null }];
  const digits = term.replace(/\D/g, '');
  const r = await q(`
    SELECT jid, MAX(name) AS name FROM (
      SELECT n.jid, n.name FROM ${SCHEMA}.names n WHERE n.name ILIKE $1
      UNION ALL
      SELECT m.chat_jid, m.sender_name FROM ${SCHEMA}.messages m WHERE m.sender_name ILIKE $1
      UNION ALL
      SELECT DISTINCT m.chat_jid, NULL FROM ${SCHEMA}.messages m
        WHERE length($2) >= 6 AND m.chat_jid LIKE '%' || $2 || '%'
    ) x
    WHERE jid IN (SELECT DISTINCT chat_jid FROM ${SCHEMA}.messages)
    GROUP BY jid
    LIMIT 25
  `, [`%${term}%`, digits]);
  const names = await namesFor(r.rows.map((x) => x.jid));
  return r.rows.map((x) => ({ jid: x.jid, name: names.get(x.jid) || x.name }));
}

async function renderMessages(rows) {
  if (!rows.length) return 'No messages found.';
  const names = await namesFor(rows.flatMap((r) => [r.sender_jid, r.chat_jid]));
  return rows.map((r) => {
    const who = r.from_me ? 'Me' : (names.get(r.sender_jid) || r.sender_name || r.sender_jid);
    const chat = r.chat_jid.endsWith('@g.us') ? ` [group: ${names.get(r.chat_jid) || r.chat_jid}]` : '';
    return `${fmtTime(r.ts)}${chat} | ${who}: ${r.text}`;
  }).join('\n');
}

const text = (t) => ({ content: [{ type: 'text', text: t }] });
const notConnectedNote = () => (status === 'connected'
  ? '' : `\n\n(Note: WhatsApp is currently "${status}", so recent messages may be missing.)`);

// ---------- MCP server ----------
function buildMcpServer() {
  const server = new McpServer({ name: 'whatsapp-reader', version: '1.1.0' });
  const readOnly = { readOnlyHint: true, openWorldHint: false };

  server.registerTool('whatsapp_status', {
    title: 'WhatsApp status',
    description: 'Shows whether WhatsApp is linked and how many messages are stored.',
    annotations: readOnly,
  }, async () => {
    const r = (await q(`SELECT COUNT(*)::int AS n, MIN(ts) AS a, MAX(ts) AS b FROM ${SCHEMA}.messages`)).rows[0];
    return text([
      `Status: ${status}`,
      `Linked as: ${linkedAs || 'not linked'}`,
      `Stored messages: ${r.n}`,
      r.n ? `Oldest: ${fmtTime(r.a)} | Newest: ${fmtTime(r.b)}` : '',
      `Times shown in: ${TIMEZONE}`,
    ].filter(Boolean).join('\n'));
  });

  server.registerTool('whatsapp_find_chats', {
    title: 'Find WhatsApp chats',
    description: 'Finds chats and contacts by name or phone number. Use this first to get the chat ID for a person or group.',
    inputSchema: { query: z.string().min(1).describe('Name or phone number, e.g. "Dani Eldas" or "+504 9999"') },
    annotations: readOnly,
  }, async ({ query }) => {
    const chats = await findChats(query);
    if (!chats.length) return text(`No chats matched "${query}".${notConnectedNote()}`);
    const stats = await q(
      `SELECT chat_jid, COUNT(*)::int AS n, MAX(ts) AS last FROM ${SCHEMA}.messages
       WHERE chat_jid = ANY($1) GROUP BY chat_jid`, [chats.map((c) => c.jid)],
    );
    const byJid = new Map(stats.rows.map((r) => [r.chat_jid, r]));
    return text(chats.map((c) => {
      const s = byJid.get(c.jid) || { n: 0 };
      return `${c.name || '(no name)'} | chat_id: ${c.jid} | ${s.n} messages | last: ${s.last ? fmtTime(s.last) : 'n/a'}`;
    }).join('\n'));
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
    const matches = await findChats(chat);
    if (!matches.length) return text(`No chat matched "${chat}".${notConnectedNote()}`);
    if (matches.length > 1 && !chat.includes('@')) {
      return text(`Several chats match "${chat}". Pick one chat_id:\n${matches.map((m) => `${m.name || '(no name)'} | ${m.jid}`).join('\n')}`);
    }
    const jid = matches[0].jid;
    const r = await q(
      `SELECT * FROM ${SCHEMA}.messages WHERE chat_jid = $1 AND ts >= $2 AND ts <= $3
       ORDER BY ts DESC LIMIT $4`,
      [jid, parseDate(since) ?? 0, parseDate(until, true) ?? 9e12, limit],
    );
    const rows = r.rows.reverse();
    return text(`Chat: ${matches[0].name || jid}\n\n${await renderMessages(rows)}${notConnectedNote()}`);
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
    const words = query.split(/\s+/).filter(Boolean).slice(0, 10).map((w) => `%${w}%`);
    const params = [words, parseDate(since) ?? 0, parseDate(until, true) ?? 9e12, limit];
    let chatFilter = '';
    if (chat) {
      const jids = (await findChats(chat)).map((c) => c.jid);
      if (!jids.length) return text(`No chat matched "${chat}".`);
      params.push(jids);
      chatFilter = 'AND chat_jid = ANY($5)';
    }
    const r = await q(
      `SELECT * FROM ${SCHEMA}.messages
       WHERE text ILIKE ${match_all ? 'ALL' : 'ANY'}($1) ${chatFilter}
         AND ts >= $2 AND ts <= $3
       ORDER BY ts DESC LIMIT $4`, params,
    );
    return text(`${await renderMessages(r.rows.reverse())}${notConnectedNote()}`);
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
  const count = (await q(`SELECT COUNT(*)::int AS n FROM ${SCHEMA}.messages`)).rows[0].n;
  const qr = latestQR && status !== 'connected' ? await QRCode.toDataURL(latestQR, { margin: 1, width: 300 }) : null;
  res.set('Cache-Control', 'no-store').json({ status, linkedAs, count, qr, pairingCode });
});

app.post('/link/:secret/pair', async (req, res) => {
  if (!secretOk(req.params.secret)) return res.status(404).end();
  const phone = String(req.body?.phone || '').replace(/\D/g, '');
  if (phone.length < 8) return res.status(400).json({ error: 'Enter the full number with country code.' });
  if (status === 'connected' || sock?.authState?.creds?.registered) return res.status(409).json({ error: 'Already linked.' });
  let lastErr;
  let failed = null;
  // The socket can close between becoming ready and our request; try once more on the next socket.
  for (let attempt = 0; attempt < 2; attempt++) {
    const s = await waitForLinkReady(failed);
    if (!s) break;
    try {
      const code = await s.requestPairingCode(phone);
      pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
      return res.json({ pairingCode });
    } catch (err) {
      lastErr = err;
      logger.error(err);
      failed = s;
    }
  }
  res.status(503).json({ error: `Couldn't get a code${lastErr ? `: ${String(lastErr.message || lastErr)}` : ''}. Wait a few seconds and try again.` });
});

initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`Listening on :${PORT}`));
    return startWhatsApp();
  })
  .catch((err) => {
    console.error('Startup failed. Check DATABASE_URL (use the Supabase Session pooler string):', err.message);
    process.exit(1);
  });

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => { pool.end().finally(() => process.exit(0)); });
}
