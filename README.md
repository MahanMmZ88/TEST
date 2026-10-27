# Mahan Panel — VLESS WS/XHTTP

پنل مدیریت کانفیگ‌های VLESS روی WebSocket/XHTTP، با موتور داخلی Node.js (بدون xray-core).
سازگار با **Abasthan** و **Deplexo** — دو پلتفرمی که رفتار مخالفی دارند:

| | Abasthan | Deplexo | Belmo (Coolify) |
|---|---|---|---|
| بیلد | محیط **تمیز**؛ Build Command باید وابستگیها را نصب کند | Dockerfile؛ `npm ci` داخل ایمیج | Dockerfile (توصیه) یا Nixpacks |
| استارت | `npm start` → `PORT` تزریق خودکار | CMD ایمیج `node server.js` | `node server.js` / `npm start` |
| فایل سیستم | قابلنوشتن | **فقطخواندنی** + volume پایدار `/data` | قابلنوشتن (موقت) |

**ماتریس Build Command — خیلی مهم:**

| پلتفرم | Build Command |
|---|---|
| Abasthan | `npm ci && npm run build` |
| **Belmo / Coolify (Nixpacks)** | خالی یا `npm run build` |
| **Belmo / Coolify (Dockerfile)** | خالی |
| Deplexo | خالی |

> ⚠️ هرگز `npm ci && npm run build`ـه Abasthan را روی Nixpacks/Coolify کپی نکنید؛
> Nixpacks خودش `npm ci` را در فاز install اجرا میکند و `npm ci` دوم با کش
> `/app/node_modules/.cache` برخورد میکند → `EBUSY` → بیلد fail.
> `nixpacks.toml` این پروژه جلویش را میگیرد.

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

## دیپلوی روی Belmo (Coolify)

**توصیه:** تو دشبورد Build Pack را بگذارید **Dockerfile**، `Ports Exposes = 3000`،
و هر سه فیلد Install/Build/Start Command را **خالی** کنید. اگر Nixpacks ماند،
فقط `npm run build` بگذارید (نه `npm ci && npm run build`) و بگذارید
`nixpacks.toml` بقیه را انجام دهد. جزئیات و عیبیابی:
**[BELMO_DEPLOY_GUIDE.md](BELMO_DEPLOY_GUIDE.md)**.

## پلن ۱۲۸ مگابایتی (کاهش خطر OOM و حفظ اتصال‌ها)

گارد حافظه مصرف cgroup را پایش می‌کند و همراه backpressure، بافرهای WS/XHTTP را
در فشار کم می‌کند و جریان TCP را موقتاً آهسته می‌کند. اتصال‌های برقرارشده برای
آزادکردن حافظه عمداً بسته نمی‌شوند، اما در بار مداوم ممکن است اتصال جدید بعد از
انتظار `503` بگیرد. OOM یا نبودِ قطعی در همهٔ شرایط قابل تضمین نیست.

برای یک سرویس ۱۲۸ MiB، در Environment داشبورد:

```text
MEMORY_LIMIT_MB=128
NODE_OPTIONS=--max-old-space-size=48 --max-semi-space-size=1
```

بعد از Redeploy، `GET /api/health` و فیلد `memory_guard` را بررسی کنید. جزئیات،
محدودیت‌ها و نتایج تست واقعی cgroup: **[LOW_MEMORY_128MB_GUIDE.md](LOW_MEMORY_128MB_GUIDE.md)**.
تست رگرسیون: `npm test`.

### نکات کلیدی

- سرور همیشه روی `0.0.0.0` و `process.env.PORT` (فالبک 3000) گوش میدهد؛
  `/`, `/health`, `/healthz` همگی 200 هستند، `/api/health` وضعیت storage/RAM را میگوید.
- دیتابیس روی volume پایدار `/data/db.json`؛ در صورت عدم دسترسی به `/tmp` فالبک میکند.
- متغیرها: `.env.example` را ببینید.

> ⚠️ **هشدار مهم:** قوانین [Deplexo](https://docs.deplexo.com/reference/acceptable-use/#proxy-vpn-and-traffic-relay-services)
> اجرای سرور پروکسی/VPN/تانل ترافیک عمومی را حتی خصوصی ممنوع کرده است. رفع مشکلات فنی
> دیپلوی بهمعنای مجاز بودن آن نیست. برای Abasthan هم قوانین سرویس را در دشبورد چک کنید.
