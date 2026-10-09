# WhatsApp Reader for Claude

A small server that links to your own WhatsApp the same way WhatsApp Web does. It saves your text messages and lets Claude search and read them. It's **read-only**: Claude can't send messages, delete anything, or change your "online" status.

> **Heads up:** this uses an unofficial WhatsApp library. It's against WhatsApp's terms, and WhatsApp can ban numbers that use tools like this. Bans are uncommon for light, read-only use, but the risk is real. You can unlink it anytime from WhatsApp → Settings → Linked devices.

## What it costs

Render's **Starter** plan plus a 1 GB disk (last I knew, about $7 a month plus a few cents for the disk; check Render's pricing page for current rates). The free plan won't work: it has no disk, so the WhatsApp login and saved messages would be wiped on every restart.

## Setup (about 15 minutes, easiest on a computer)

### 1. Put the code on GitHub
1. Sign in at github.com and create a new **private** repository (e.g. `whatsapp-reader-mcp`).
2. Click **uploading an existing file** and drag in everything from this folder, keeping the `src` folder. Commit.

### 2. Deploy on Render
1. In Render, click **New → Blueprint**, connect GitHub, and pick the repository.
2. Render reads `render.yaml` and sets everything up: Starter plan, a 1 GB disk, and a random `AUTH_SECRET`. Click **Apply**.
3. Wait for the deploy to show **Live** (a few minutes).
4. Open the service → **Environment** and copy the value of `AUTH_SECRET`. This is your password. Anyone who has it can read your WhatsApp, so don't share it.
5. Note your service address at the top, e.g. `https://whatsapp-reader-mcp-abcd.onrender.com`.

### 3. Link your WhatsApp
Open `https://YOUR-ADDRESS/link/YOUR_AUTH_SECRET` in a browser.

- **On a computer:** scan the QR code with your phone (WhatsApp → Settings → Linked devices → Link a device).
- **Phone only:** type your number with country code and tap **Get pairing code**. Then in WhatsApp go to Settings → Linked devices → Link a device → **Link with phone number instead**, and enter the code.

The page shows ✅ when it's linked. Leave it for 5–10 minutes so your older chat history can download.

### 4. Add it to Claude
1. In Claude, go to **Settings → Connectors → Add custom connector**.
2. Name: `WhatsApp`. URL: `https://YOUR-ADDRESS/mcp/YOUR_AUTH_SECRET`
3. Save, then turn it on in a chat from the tools menu.

Then ask something like: *"Check my WhatsApp chat with Dani Eldas for tours in October 2026 and add them to my Tours Oct 2026 sheet."*

## Good to know
- **History:** when you first link, WhatsApp sends the server a chunk of your past messages, usually several months. It doesn't always include everything, so very old chats may be missing. Everything from the moment you link onward is saved.
- **Photos and voice notes** show up as `[image]`, `[audio]`, etc. Captions are saved.
- **Your phone** needs to come online at least every couple of weeks, or WhatsApp unlinks the device.
- **Times** are shown in Honduras time (`TIMEZONE` in Render's Environment tab, using names like `America/Chicago`).
- **To stop:** unlink it in WhatsApp → Linked devices, then delete the service in Render.
- **If WhatsApp updates and the link stops working:** update the `@whiskeysockets/baileys` version in `package.json` and redeploy.

## Tools Claude gets
| Tool | What it does |
|---|---|
| `whatsapp_status` | Whether it's linked and how many messages are saved |
| `whatsapp_find_chats` | Finds a person or group by name or number |
| `whatsapp_read_chat` | Reads one chat, with optional date range |
| `whatsapp_search_messages` | Searches message text across chats |
