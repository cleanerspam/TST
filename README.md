
<p align="center">
  <img src="https://iili.io/KhN0ztj.png" alt="Logo" width="400"/>
</p>

<p align="center">
  A powerful, self-hosted <b>Telegram Stremio Media Server</b> built with <b>FastAPI</b>, <b>MongoDB</b>, and <b>PyroFork</b> — seamlessly integrated with <b>Stremio</b> for automated media streaming and discovery.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/UV%20Package%20Manager-2B7A77?logo=uv&logoColor=white" alt="UV Package Manager" />
  <img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white" alt="MongoDB" />
  <img src="https://img.shields.io/badge/PyroFork-EE3A3A?logo=python&logoColor=white" alt="PyroFork" />
  <img src="https://img.shields.io/badge/Stremio-8D3DAF?logo=stremio&logoColor=white" alt="Stremio" />
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" alt="Docker" />
</p>

---

## 🧭 Quick Navigation

- [🚀 Introduction](#-introduction)
- [📝 Prerequisites & Variables](#-prerequisites--variables-how-to-get-them)
- [☁️ Deployment (VPS Recommended)](#-deployment)
- [🔐 Access Management (Tokens)](#-access-management--tokens)
- [📺 Setting up Stremio](#-setting-up-stremio)
- [🤖 Bot Commands](#-bot-commands)
- [⚙️ Environment Variables](#-environment-variables)

---

# 🚀 Introduction

This project is a **next-generation media server** that bridges **Telegram** and **Stremio**. It allows you to index files from your Telegram channels and stream them directly on Stremio devices (TVs, Phones, PC) with **zero transcoding** and **no file expiration**.

### ✨ Key Features

- ⚡ **Direct Streaming**: Streams directly from Telegram servers to your device with **zero transcoding**.
- 🚀 **Multi-Bot Aggregated Streaming**: Uses **3 bots simultaneously** per stream for 3x faster speeds (perfect for 4K at 2.5x playback).
- 🧠 **Smart Load Balancing**: Intelligent least-connection dispatching with auto-failover for maximum reliability.
- 💾 **500MB Global RAM Cache**: Instant backward seeking and cross-user cache sharing for popular content.
- 🖥️ **Web Dashboard**: A beautiful, dark-themed admin panel to manage your server, view system stats, and control access.
- 🔐 **Advanced Access Control**: Create invite links with precise **Daily/Monthly bandwidth limits** and track usage in real-time.
- 🛠️ **Manual Update Manager**: A dedicated UI to resolve quality conflicts (e.g., upgrading 720p to 1080p), handle duplicates, and **auto-fix** naming errors (like double extensions).
- 🔄 **Smart Metadata**: Robust parsing for Movies, TV Shows, and Anime, including support for year-based seasons (e.g., *Tom & Jerry S1940*).
- 🧹 **Self-Healing**: Background tasks automatically clean up missing files, reset user quotas at midnight, and pull server updates from your config branch.
- 📡 **Load Balancing**: Supports multiple bot tokens to bypass Telegram's flood limits.

---

# 🖥️ Web Dashboard Capabilities

The **v2.3 Dashboard** gives you complete control and real-time system monitoring:

1. **System Status**: View uptime, database statistics, **Active Streams**, and bot workloads with auto-correcting sanitization.
2. **Streaming Performance**: Monitor live stream speeds, peak throughput, and multi-bot aggregation status.
3. **Pending Updates**: Review incoming files that conflict with existing media.
    - *Auto-Resolve*: Automatically accepts better quality files or fixes filenames.
    - *Bulk Actions*: Approve or reject updates in batches.
4. **Token Management**: Create, revoke, and edit user tokens.
    - Set **GB limits** (Daily/Monthly) to prevent abuse.
    - View live usage bars for every user.
5. **Media Management**: Edit metadata, incorrect matches, or delete content directly from the UI.

---

# 📝 Prerequisites & Variables (How to get them)

Before deploying, you need to gather a few keys. Here is how to find them:

### 1. Telegram API Keys (`API_ID`, `API_HASH`)

1. Go to [my.telegram.org](https://my.telegram.org) and log in.
2. Click on **API development tools**.
3. Create a new application (name it anything).
4. Copy the **App api_id** and **App api_hash**.

### 2. Bot Tokens (`BOT_TOKEN`, `HELPER_BOT_TOKEN`)

1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` and follow the instructions to create your **Main Bot**.
3. Copy the HTTP API Token -> This is your `BOT_TOKEN`.
4. Send `/newbot` again to create a **Helper Bot** (used for background tasks).
5. Copy its token -> This is your `HELPER_BOT_TOKEN`.

### 3. MongoDB URI (`DATABASE`)

1. Create a free account at [MongoDB Atlas](https://www.mongodb.com/cloud/atlas).
2. Create a **Cluster** (free tier works fine).
3. In **Database Access**, create a user (e.g., `admin`) and password.
4. In **Network Access**, allow access from `0.0.0.0/0` (Anywhere).
5. Click **Connect** -> **Drivers** -> Copy the Connection String.
   - It looks like: `mongodb+srv://admin:password@cluster0.abcde.mongodb.net/?retryWrites=true&w=majority`

### 4. Channel ID (`AUTH_CHANNEL`)

1. Create a **Telegram Channel** (or use an existing one).
2. Add **both** your bots (Main + Helper) as **Admins** in this channel.
3. Forward a message from this channel to a bot like **@Rose** or **@GetIDs Bot** to find the numeric ID.
   - It usually starts with `-100...` (e.g., `-1001234567890`).

---

# ☁️ Deployment

> [!IMPORTANT]
> **VPS (Virtual Private Server) is strongly recommended.**
> Free platforms like Heroku/Render put apps to sleep and have strict bandwidth/resource limits which cause buffering. A cheap VPS ($5/mo) ensures 24/7 uptime and smooth streaming.

### 🐧 VPS Guide (Docker Compose) - **PREFERRED**

1. **Clone the Repository**:

    ```bash
    git clone https://github.com/weebzone/Telegram-Stremio
    cd Telegram-Stremio
    ```

2. **Configure Environment**:

    ```bash
    cp sample_config.env config.env
    nano config.env
    ```

    *Fill in the variables you gathered in the previous step.*

3. **Start the Server**:

    ```bash
    docker compose up -d
    ```

    Your server is now running at: `http://<YOUR_VPS_IP>:8000`

### 🌐 Domain & HTTPS (Optional but Recommended)

Stremio requires HTTPS for many add-ons. To add HTTPS easily:

1. Point your domain's **A Record** to your VPS IP.
2. Install **Caddy** (Automatic HTTPS):

    ```bash
    sudo apt install -y caddy
    ```

3. Edit Caddyfile:

    ```bash
    sudo nano /etc/caddy/Caddyfile
    ```

    ```
    your-domain.com {
        reverse_proxy localhost:8000
    }
    ```

4. Reload Caddy:

    ```bash
    sudo systemctl reload caddy
    ```

Your API is now at: `https://your-domain.com`

---

# 🔐 Access Management (Tokens)

To protect your server from unauthorized usage, this project uses a **Token System**.

1. **Login to Dashboard**:
    - Go to `https://your-domain.com/login` (or `http://ip:8000/login`).
    - Use the `ADMIN_USERNAME` and `ADMIN_PASSWORD` from your config.

2. **Create a User Token**:
    - Click **"New Token"**.
    - Enter a name (e.g., "Friend's TV").
    - **Set Limits (Optional)**: You can restrict daily/monthly bandwidth (e.g., 2GB Daily).
    - Click **Create**.

3. **Get Stremio Link**:
    - In the table, click **Copy Link**.
    - This link (`https://.../stremio/<token>/manifest.json`) is what you give to the user.

> [!TIP]
> If a user exceeds their limit, their stream will stop immediately to save bandwidth.

---

# 📺 Setting up Stremio

Now that you have your **Tokenized Stremio Link**:

1. Open **Stremio** on your device (PC/Mobile/TV).
2. Go to the **Addons** tab.
3. Paste your **Copied Link** into the search bar.
4. Click **Install**.

🎉 **Done!** You will now see your Telegram files appear in Stremio search results.

---

# 🤖 Bot Commands

Interact with your bot in Telegram:

| Command | Description |
| :--- | :--- |
| `/start` | Check if bot is alive. |
| `/log` | Get system log file (Admin only). |
| `/set <imdb_url>` | **Index a Movie/Series**. Send command -> Forward files -> Send `/set` again to finish. |
| `/delete` | Reply to a file in the channel to delete it from DB. |

**How to Index Content:**

1. Send `/set https://www.imdb.com/title/tt123456/` to the bot.
2. **Forward** the video files from your channel to the bot (or the channel itself, depending on configured permissions).
3. Send `/set` again to save.

---

# ⚙️ Environment Variables

Full list of variables in `config.env`:

| Variable | Description |
| :--- | :--- |
| `API_ID` | Telegram API ID. |
| `API_HASH` | Telegram API Hash. |
| `BOT_TOKEN` | Main Bot Token. |
| `HELPER_BOT_TOKEN` | Helper Bot Token. |
| `OWNER_ID` | Your numeric Telegram ID (for admin commands). |
| `AUTH_CHANNEL` | Channel ID to index files from (e.g. `-100xxxx`). |
| `DATABASE` | MongoDB Connection URI. |
| `BASE_URL` | Domain or IP with protocol (e.g. `https://my-stremio.com`). |
| `TMDB_API` | (Optional) For better metadata fetching. |
| **Performance Tuning (v2.3.0+)** | |
| `PARALLEL` | Concurrent chunks downloaded per stream (Default: `5`). Higher = faster, more RAM. |
| `PRE_FETCH` | Buffer size in chunks (Default: `50`). Higher = less buffering, more RAM. |
| `WORKERS` | Pyrogram worker threads (Default: `50`). Handles multiple streams efficiently. |
| `MULTI_TOKEN1-10` | (Optional) Additional bot tokens for multi-bot streaming. Each token = 3x speed boost. |

---

<p align="center">
  Made with ❤️ by the Open Source Community
</p>
