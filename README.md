# Mahan Panel — VLESS WS/XHTTP

پنل مدیریت کانفیگ‌های VLESS روی WebSocket/XHTTP، با موتور داخلی Node.js (بدون xray-core).
سازگار با **Abasthan** و **Deplexo** — دو پلتفرمی که رفتار مخالفی دارند:

| | Abasthan | Deplexo |
|---|---|---|
| بیلد | محیط **تمیز**؛ Build Command باید وابستگیها را نصب کند | Dockerfile؛ `npm ci` داخل ایمیج |
| استارت | `npm start` → `PORT` تزریق خودکار | CMD ایمیج `node server.js` |
| فایل سیستم | قابلنوشتن | **فقطخواندنی** + volume پایدار `/data` |

برای همین `scripts/ensure-deps.js` وجود دارد: **فقط وقتی وابستگیها واقعاً کم باشند**
`npm ci` میکند. روی Deplexo یک no-op است و هیچ `npm install`ای در استارتاپ اجرا نمیشود
(جلوگیری از خطای `EROFS`)؛ روی Abasthan نصب میکند تا `Cannot find module 'express'`
و بهدنبالش `no_live_port` رخ ندهد.

## اجرای محلی

```bash
npm ci
npm start              # http://localhost:3000/  (admin / admin)
```

## دیپلوی روی Abasthan

| فیلد دشبورد | مقدار |
|---|---|
| Service type | Web Service |
| Runtime | Node.js 22 |
| Root Directory | `./` |
| **Build Command** | `npm ci && npm run build` |
| **Start Command** | `npm start` (یا `npm run start`) |
| Environment | `PORT` را دستی ست نکن؛ Abasthan تزریق میکند |

سپس `/health` و `/api/health` را تست کنید و پسورد `admin/admin` را عوض کنید.
جزئیات و عیبیابی `no_live_port`: **[ABASTHAN_DEPLOY_GUIDE.md](ABASTHAN_DEPLOY_GUIDE.md)**.

## دیپلوی روی Deplexo

1. فایلهای `deplexo.yaml`، `Dockerfile`، `package-lock.json`، `server.js`، `scripts/`،
   `db.json` و `public/` داخل کامیت متصل باشند.
2. **Storage / Mounts** روی مسیر پیشفرض `/data` بماند (یا `DB_PATH` هماهنگ شود).
3. **Redeploy** کنید (نه صرفاً Restart)؛ `deplexo.yaml` فقط هنگام دیپلوی خوانده میشود.
4. لاگ: `[DB] Storage mode: persistent-volume` و
   `[INFO] Boot: ... | host = 0.0.0.0` — و هیچ `npm install`ای در runtime.

جزئیات کامل: **[DEPLEXO_DEPLOY_GUIDE.md](DEPLEXO_DEPLOY_GUIDE.md)**.

### نکات کلیدی

- سرور همیشه روی `0.0.0.0` و `process.env.PORT` (فالبک 3000) گوش میدهد؛
  `/`, `/health`, `/healthz` همگی 200 هستند، `/api/health` وضعیت storage/RAM را میگوید.
- دیتابیس روی volume پایدار `/data/db.json`؛ در صورت عدم دسترسی به `/tmp` فالبک میکند.
- متغیرها: `.env.example` را ببینید.
- پلن رایگان Deplexo فقط 128MB RAM / 0.25 CPU دارد؛ حالت کمحافظه بافرهای XHTTP را کوچک میکند.

> ⚠️ **هشدار مهم:** قوانین [Deplexo](https://docs.deplexo.com/reference/acceptable-use/#proxy-vpn-and-traffic-relay-services)
> اجرای سرور پروکسی/VPN/تانل ترافیک عمومی را حتی خصوصی ممنوع کرده است. رفع مشکلات فنی
> دیپلوی بهمعنای مجاز بودن آن نیست. برای Abasthan هم قوانین سرویس را در دشبورد چک کنید.
