# Binance WebSocket Signal Bot — Railway Deploy

## Railway එකට Deploy කරන විදිහ (Step by Step)

### 1. Railway account
- https://railway.app ට යන්න
- GitHub account එකෙන් Login කරන්න (හෝ Email)

### 2. New Project
- **New Project** → **Empty Project** (හෝ Deploy from GitHub)

### 3. GitHub හරහා deploy කරනවා නම් (recommended)
1. GitHub එකේ new private repository එකක් හදන්න (උදා: `binance-signal-bot`)
2. මේ folder එකේ files (`bot.py`, `requirements.txt`, `Procfile`) upload / push කරන්න
3. Railway → New Project → **Deploy from GitHub repo** → ඒ repo එක select කරන්න

### 4. Environment Variables දාන්න (අනිවාර්යයි)
Railway project එකේ **Variables** tab එකට යන්න, මේ දෙක add කරන්න:

| Variable | Value |
|----------|-------|
| `TELEGRAM_BOT_TOKEN` | `8963812615:AAEZPuVl_PUX657Xq5Ip_-R-UPN7j_CBvbw` |
| `TELEGRAM_CHAT_ID`   | `-1003980402379` |

Optional:
| Variable | Default | Description |
|----------|---------|-------------|
| `TOP_N_SPOT` | 25 | Spot pairs count |
| `TOP_N_FUTURES` | 25 | Futures pairs count |
| `KLINE_INTERVAL` | 1h | Candle timeframe |
| `TOP_PAIRS_REFRESH` | 600 | Top pairs refresh (seconds) |

### 5. Deploy
- Railway automatically detect කරලා build කරනවා
- **Deployments** tab එකේ logs බලන්න
- `WebSocket Signal Bot Online` message එක Telegram channel එකට ආවොත් success

### 6. Service type
- Procfile එකේ `worker:` තියෙන නිසා Railway එක worker process විදිහට run කරනවා
- Web service නෙවෙයි — background bot එකක්

---

## Local test (optional)
```bash
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="-1003980402379"
pip install -r requirements.txt
python bot.py
```

## Important
- Bot token එක කවදාවත් public GitHub repo එකේ hardcode කරන්න එපා
- Telegram channel එකේ bot එක **Admin** වෙන්න ඕනේ
- Railway free tier එකේ limits තියෙනවා — usage බලන්න

Not financial advice.
