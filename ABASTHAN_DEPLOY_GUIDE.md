# راهنمای رفع خطای `no_live_port` روی Abasthan

تاریخ: 2026-10-25 · نسخه: 2.1.0

---

## ۱. خطای شما دقیقاً چه میگوید؟

```
Environment: node 22
⚙️ Start Command: npm run start
⚡ Executing start command: npm run start
No live ports found yet. Retrying in 5s...
❌ Deployment Error: App Crashed (Exit Code 1)
Timeout: Application failed to open any live ports within 120 seconds.
```

`no_live_port` یعنی **پلتفرم هیچ سوکت Listening پیدا نکرد**، نه اینکه پورت اشتباه باشد.
دلیلش این است که فرایند `npm run start` **قبل از باز کردن پورت کرش کرده (Exit Code 1)**.

من همان سناریو را لوکالی بازسازی کردم (یک کپی تمیز بدون `node_modules`):

```
> mahan-panel-vless-ws-node@2.1.0 start
> node server.js
Error: Cannot find module 'express'
...
NO LIVE PORT
```

یعنی: **وابستگیها نصب نشدهاند.** این دقیقاً همان خطایی است که PR #1 این ریپو
(«fix: add prestart install and build command for Abasthan deployment») هم
برای `Cannot find module 'express'` روی Abasthan باز شده بود.

### چرا نصب نشده؟

مستندات رسمی Abasthan صراحتاً میگوید
([Deploying Node.js](https://abasthan.com/docs/deploy-nodejs)،
[Deployment Flow](https://abasthan.com/docs/deployment-flow)):

> Build Command **must include dependency installation** because Abasthan starts
> each build in a **clean environment**. → `npm install && npm run build`

و اینکه `PORT` بهصورت خودکار تزریق میشود و سرور باید روی `0.0.0.0` گوش دهد.

مشکل کد قبلی ما: `"build": "node --check server.js"` بود — یعنی اگر Build Command
دشبورد `npm run build` تنظیم شده باشد، **هیچ `node_modules`ای ساخته نمیشود** و استارت
کرش میکند. (ضمن اینکه قبلاً بهخاطر `prestart: npm install` روی Deplexo خطای `EROFS`
گرفته بودیم و آن را حذف کرده بودیم.)

---

## ۲. راهحل اعمالشده — نصب «شرطی» وابستگیها

فایل جدید `scripts/ensure-deps.js`: چک میکند آیا `express`, `cookie-parser`,
`qrcode`, `ws` قابل resolve هستند یا نه.

- **اگر هستند** (مثلاً ایمیج Deplexo که `npm ci` در بیلد انجام شده): فقط
  `[deps] OK` چاپ میکند و خارج میشود — **هیچ `npm install`ای اجرا نمیشود**،
  پس خطای `EROFS` برنمیگردد.
- **اگر نیستند** (محیط تمیز Abasthan): `npm ci --omit=dev` (یا در صورت شکست
  `npm install --omit=dev`) اجرا میکند، دوباره چک میکند و در صورت شکست exit 1
  میدهد تا لاگ شفاف باشد.

و در `package.json`:

```json
"prestart": "node scripts/ensure-deps.js",
"start": "node server.js",
"build": "node scripts/ensure-deps.js && node --check server.js"
```

پس سه لایه محافظت داریم:

| سناریو | چه اتفاقی میافتد |
|---|---|
| Build Command = `npm ci && npm run build` (توصیهشده) | بیلد نصب میکند؛ `prestart` موقع استارت no-op است. |
| Build Command = `npm run build` | همان `build` خودش نصب میکند. |
| Build Command خالی / فقط `npm run start` | `prestart` موقع استارت نصب میکند. |

همچنین `server.on('error')` اضافه شد: اگر پورت قابل bind نباشد، لاگ
`[FATAL]` مینویسد و سریع exit 1 میدهد (قبلًا `uncaughtException` فقط لاگ میکرد و
فرایند زنده میماند بدون پورت → ۱۲۰ ثانیه انتظار بیهوده). و لاگ استارتاپ حالا
مقدار `PORT env` و پورت نهایی را چاپ میکند.

فایلهای اضافهشده: `Procfile` (`web: npm start`) و `.node-version` (`22`) برای
سازگاری با دیتکتور رانتیوم. `Dockerfile` هم `scripts/` را کپی میکند تا اگر
کسی روی Deplexo از `npm start` استفاده کرد، `prestart` فایلش موجود باشد.

---

## ۳. تنظیمات دقیق دشبورد Abasthan ✅

| فیلد | مقدار |
|---|---|
| Service type | **Web Service** |
| Runtime | **Node.js 22** |
| Root Directory | `./` (ریشه ریپو) |
| **Build Command** | `npm ci && npm run build` |
| **Start Command** | `npm start` (یا `npm run start` — هر دو OK است) |
| Environment | نیازی به `PORT` نیست؛ Abasthan خودش تزریق میکند |
| Branch | برنچی که این تغییرات روی آن است (مثلاً `main` بعد از مرج PR) |

> ⚠️ **این Build Command مخصوص Abasthan است.** روی پلتفرمهای مبتنی بر
> **Nixpacks/Coolify** (مثل Belmo، Dokploy، Easypanel، Railway) فقط
> `npm run build` بگذارید — چون Nixpacks خودش در فاز `install` یک `npm ci` اجرا
> میکند و `npm ci` دوم با کش `/app/node_modules/.cache` برخورد کرده و خطای
> `EBUSY ... rmdir '/app/node_modules/.cache'` میدهد. جزئیات:
> `BELMO_DEPLOY_GUIDE.md` و فایل `nixpacks.toml`.

> نمونهٔ واقعی دیگری از دیپلوی موفق روی Abasthan هم دقیقاً همین مقادیر را دارد:
> `Build command: npm install && npm run build`، `Start command: npm start`،
> `PORT` تزریق خودکار، health check روی `/health`
> ([cryo-chat DEPLOYMENT.md](https://github.com/RaihanAizvan/cryo-chat/blob/main/DEPLOYMENT.md)).

### انتظار از لاگ بعد از اصلاح

```
> mahan-panel-vless-ws-node@2.1.0 prestart
> node scripts/ensure-deps.js
[deps] OK - all runtime dependencies present; skipping npm install   (یا Installing...)
> node server.js
[DB] Database path: ...
[INFO] Boot: node v22.x | PORT env = 3xxx | resolved = 3xxx | host = 0.0.0.0
[INFO] Mahan Panel VLESS-WS/XHTTP active on 0.0.0.0:3xxx
✅ Live port detected
```

### تست بعد از دیپلوی

```bash
curl -i https://<app>.abasthan.app/health     # 200
curl -i https://<app>.abasthan.app/healthz    # 200
curl -s  https://<app>.abasthan.app/api/health
curl -I  https://<app>.abasthan.app/          # 200 (صفحه لاگین)
```

سپس وارد `/login` شو (`admin` / `admin`) و حتماً پسورد را عوض کن.

---

## ۴. نکات مهم دیگر

- **دیتابیس:** Abasthan هر بیلد را در محیط تمیز میسازد و مستنداتی برای volume
  پایدار پیدا نکردم (فقط «File Secrets» در `/etc/secrets/` و SFTP روی
  `/home/abasthan/app` ذکر شده:
  [Environment Variables](https://abasthan.com/docs/environment-variables) و
  [SSH & SFTP](https://abasthan.com/docs/ssh-access)).
  کد ما در صورت خواندنیبودن FS به `/tmp` یا فایل لوکال فالبک میکند، ولی آن دیتا
  با ریستارت/ریدیپلوی پاک میشود. یا volume بگیر و `DB_PATH` را روی آن تنظیم کن،
  یا مرتب از طریق `/api/backup/restore` بکآپ بگیر.
- **اگر Build Command را عوض نمیکنی:** حداقل `prestart` بهتنهایی هم کار میکند،
  ولی توصیه رسمی Abasthan همان `npm ci && npm run build` است و سریعتر/قابلاعتمادتر است.
- **رم:** اگر پلن رایگان Abasthan هم RAM کمی دارد، همان متغیرهای کمحافظهٔ
  `DEPLEXO_DEPLOY_GUIDE.md` (مثل `SMART_BOOSTER=0`, `NODE_OPTIONS`) اینجا هم قابلاستفادهاند.
- **سیاست استفاده:** برخلاف Deplexo، در مستندات عمومی Abasthan منع صریحی برای
  VPN/پروکسی پیدا نکردم (حتی صفحهای دربارهٔ «Private Mesh VPN» دارند)، ولی
  صفحهٔ Terms/AUP در دسترس نبود. قبل از استفادهٔ جدی، قوانین سرویس را در دشبورد
  یا تیکت پشتیبانی چک کن.

---

## ۵. چکلیست سریع عیبیابی `no_live_port`

1. لاگ **Build** را بخوان — آیا `npm ci`/`npm install` واقعاً اجرا شده؟
2. لاگ **Runtime** را بخوان — آیا `Cannot find module ...` وجود دارد؟
3. آیا Start Command چیزی جز `npm start` / `node server.js` است؟ (مثلاً `npm run dev` یا `npm run build`)
4. آیا لاگ `[INFO] Boot: ... resolved = <port>` چاپ شده؟ اگر نه، اپ قبل از `listen` مرده.
5. آیا `PORT` را دستی روی مقدار عجیبی ست کردهای؟ بگذار پلتفرم تزریق کند.
6. Root Directory درست است؟ (باید ریپو روت باشد، نه `public/`)
7. هنوز گیر کردی: لاگ کامل build + runtime را بفرست.
