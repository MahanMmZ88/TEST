# گزارش کامل سازگاری با Deplexo و رفع خطای دیپلوی

تاریخ بررسی: 2026-10-25
فایل لاگ بررسیشده: `crimson-cedar-6218-2026-10-25T10-26-08-823Z.log.txt`

---

## 1. خطای دقیق لاگ شما

```
> mahan-panel-vless-ws-node@2.0.0 prestart
> npm install
npm error code EROFS
npm error syscall open
npm error path /app/package-lock.json
npm error errno -30
npm error rofs EROFS: read-only file system, open '/app/package-lock.json'
npm error Log files were not written ... /home/appuser/.npm/_logs
```

**علت اصلی:** در کامیت `b161b0a` (2026-10-25 03:20) این اسکریپت اضافه شده بود:

```json
"prestart": "npm install"
```

`npm start` بهطور خودکار `prestart` را اجرا میکند. یعنی **هر بار که کانتینر استارت میخورد،
npm قصد نصب مجدد پکیجها و بازنویسی `/app/package-lock.json` را داشت**. طبق مستندات رسمی
Deplexo، ریشه فایل سیستم ایمیج در زمان اجرا **فقط خواندنی (read-only)** است
([Storage and filesystem](https://docs.deplexo.com/operations/storage/#application-files-are-read-only)):

> "Writing beside source files, **installing packages at startup**, or creating a cache
> under the application directory can fail."

پس npm با `EROFS` کرش میکرد، فرایند `start` اجرا نمیشد و دیپلوی fail میشد.
خط دوم لاگ (`/home/appuser/.npm/_logs`) هم همان مشکل است: npm حتی نمیتوانست فایل لاگ
بنویسد. مسیر `/app` و کاربر `appuser` همچنین نشان میدهد دیپلوی شما از **بیلد خودکار Node**
Deplexo استفاده میکرد، نه `Dockerfile`ای که در ریپو بود.

نکته زمانی: لاگ شما ساعت 10:20 و 10:25 است. کامیت `c4022b9` در ساعت 10:37 صرفاً
`prestart` را حذف کرده و یک فالبک EROFS اضافه کرده بود. یعنی آن ارور مربوط به نسخه
قدیمیتر کد است — ولی آن fix بهتنهایی کافی نبود (پایین را ببینید).

### چرا فقط حذف `prestart` کافی نیست؟

1. تنظیمات ذخیرهشده قدیمی در دشبورد Deplexo (start command = `npm start`،
   build method = auto) سرجایشان میمانند و ممکن است دوباره لایفسیکل npm را اجرا کنند.
2. `db.json` قبلاً کنار سورس نوشته میشد → روی FS فقطخواندنی از بین میرفت
   (فالبک قبلی `/tmp` بود که **موقت** است و با هر ریدیپلوی پاک میشود).
3. `Dockerfile` قدیمی `RUN npm ci ... || npm install` داشت و `deplo.yaml` وجود نداشت،
   پس کنترلی روی build method/پورت/کامند استارت نبود.
4. `SIGTERM` بلافاصله `process.exit(0)` میکرد؛ در حالی که Deplexo کانتینر قدیمی را
   قبل از شروع کانتینر جدید متوقف میکند و انتظار drain شدن کانکشنها را دارد.
5. فقط `/health` وجود داشت؛ استارترها و پروبهای Deplexo معمولاً `/healthz` را هم چک میکنند.
6. پلن رایگان 128MB RAM است؛ هیت V8 و بافرهای 4MB×32 میتوانستند OOM بدهند.

> 📌 **بهروزرسانی 2026-10-25 (نسخه 2.1.0):** بعداً برای رفع خطای `no_live_port`
> روی **Abasthan** یک `prestart` **شرطی** اضافه شد (`scripts/ensure-deps.js`).
> برخلاف `prestart: npm install` قدیمی، این یکی فقط زمانی `npm ci` اجرا میکند که
> `express`/`ws`/... واقعاً نصب نباشند. چون ایمیج Deplexo هنگام بیلد `npm ci` کرده،
> روی Deplexo صرفاً `[deps] OK` چاپ میشود و هیچ نوشتهای روی FS فقطخواندنی انجام
> نمیشود؛ ضمن اینکه `deplexo.yaml` همچنان CMD ایمیج (`node server.js`) را بهکار
> میگیرد و اصلاً وارد لایفسیکل npm نمیشود. جزئیات در `ABASTHAN_DEPLOY_GUIDE.md`.

---

## 2. تحقیق انجامشده (منابع رسمی)

| موضوع | نتیجه | منبع |
|---|---|---|
| ریشه FS فقطخواندنی | بله، در runtime | [docs/operations/storage](https://docs.deplexo.com/operations/storage/#application-files-are-read-only) |
| مسیر پایدار | volume بهصورت پیشفرض روی `/data` | [همان صفحه](https://docs.deplexo.com/operations/storage/#use-the-data-mount-for-durable-files) |
| مسیر موقت | `/tmp`، حداکثر 100MB، غیرقابل اجرا، پاک میشود | [همان صفحه](https://docs.deplexo.com/operations/storage/#use-tmp-for-disposable-files) |
| نصب پکیج | فقط در build، داخل ایمیج | [guides/docker](https://docs.deplexo.com/guides/docker/#build-a-reproducible-image) |
| کامند استارت | exec-form `CMD`، گوش دادن روی `0.0.0.0` و `PORT` | [guides/docker](https://docs.deplexo.com/guides/docker/#start-the-correct-process) |
| `deplexo.yaml` | `version: 1`، `build.framework: dockerfile`، `run.port` | [reference/configuration](https://docs.deplexo.com/reference/configuration/) |
| کامند خالی | `run.command: ""`Override قدیمی دشبورد را پاک میکند | [همان صفحه](https://docs.deplexo.com/reference/configuration/#configure-build-and-startup-commands) |
| پورت | `run.port` جای `PORT` را میگیرد؛ `PORT` نامعتبر ممکن است در env بماند | [همان صفحه](https://docs.deplexo.com/reference/configuration/#web-ports-and-background-workers) |
| چرخه دیپلوی | کانتینر قدیمیStop → جدیدStart؛ SIGTERM مهم است | [operations/deployments](https://docs.deplexo.com/operations/deployments/) |
| منابع | Free: 128MB/0.25CPU/10Mbit/100GB · Starter: 512MB/0.5CPU/100Mbit/500GB | [deplexo.com/plans](https://deplexo.com/plans) |
| **ممنوعیت** | **پروکسی/VPN/تانل ترافیک عمومی حتی خصوصی ممنوع؛ بن و حذف دیتا** | [acceptable-use](https://docs.deplexo.com/reference/acceptable-use/#proxy-vpn-and-traffic-relay-services)، [FAQ](https://docs.deplexo.com/getting-started/faq/#content-and-account-rules) |
| الگوی رسمی Node | `node:alpine` + `npm ci --omit=dev` + `USER node` + `CMD ["node","server.js"]` | [Deplexo/examples/node-express](https://github.com/Deplexo/examples/tree/main/templates/node-express) |

در مستندات Deplexo اشاره صریحی به پشتیبانی/عدم پشتیبانی **WebSocket upgrade** و
**UDP خروجی** پیدا نشد. WS معمولاً از طریق HTTPS روتر عبور میکند، ولی ترافیک UDP
خروجی (که موتور VLESS برای `udp:443` میسازد) توسط پلتفرمهای PaaS گاهی محدود میشود؛
حتماً بعد از دیپلوی تست عملی کنید.

---

## 3. تغییرات اعمالشده

| فایل | تغییر |
|---|---|
| `deplexo.yaml` ⭐ | فایل جدید: `framework: dockerfile`، `run.port: 3000`، `run.command: ""`؛ یعنی CMD ایمیج استفاده میشود و هیچ لایفسیکل npm (و در نتیجه هیچ `prestart`ای) اجرا نمیشود. |
| `package.json` | `prestart` قدیمی (`npm install`) **حذف** شد. در نسخه 2.1.0 یک `prestart` **شرطی** (`node scripts/ensure-deps.js`) برگشت که فقط وقتی وابستگیها کم هستند نصب میکند — روی ایمیج Deplexo یک no-op است. `start` = `node server.js`؛ `engines` ≥ 20.3.0؛ `private: true`. |
| `scripts/ensure-deps.js` ⭐ | بوتاسترپ وابستگیها برای Abasthan؛ روی Deplexo چون `node_modules` داخل ایمیج است، `[deps] OK` چاپ کرده و خارج میشود، بدون اجرای `npm install`. |
| `Dockerfile` | `node:22-alpine`؛ `npm ci --omit=dev` **بدون** فالبک `npm install`؛ `COPY` فایلهای لازم + `scripts/`؛ ایجاد `/data` با مالک `node` قبل از `USER node`؛ `DB_PATH=/data/db.json`؛ `HEALTHCHECK` با busybox wget؛ `CMD ["node","server.js"]`. |
| `Procfile` / `.node-version` | `web: npm start` و پین کردن Node 22 برای دیتکتور Abasthan؛ روی بیلد Dockerfile بیتأثیرند. |
| `.dockerignore` | حذف `.git`، `node_modules`، لاگها، `.env`، فایلهای md و کانفیگ از build context (ولی `db.json` و `public/` حفظ شدند). |
| `.env.example` ⭐ | مستندسازی `PORT`، `DB_PATH`، `DB_FALLBACK_PATHS`، `KEEPALIVE_ENABLED` و تیونینگ حافظه. |
| `.gitignore` | اضافه شدن `.env`، `*.log`، `.DS_Store`. |
| `server.js` | ← جزئیات پایین |
| `README.md` / همین فایل | راهنمای فارسی دیپلوی + هشدار سیاست استفاده. |

### جزئیات `server.js`

- **پورت:** `PORT` اعتبارسنجی میشود؛ اگر دشبورد مقدار نامعتبری ذخیره کرده باشد،
  به 3000 برمیگردد (همان پورتی که روتر Deplexo استفاده میکند).
- **دیتابیس:** تابع `resolveInitialDbPath()` بهجای حدس زدن، یک لیست کاندید
  (`DB_PATH` → مسیر محلی → `/data/db.json` → `DB_FALLBACK_PATHS` → `/tmp/db.json`)
  را با نوشتن فایل پروب تست میکند و اولین مسیر قابلنوشتن را انتخاب میکند.
  `seedDbTo()` دیتابیس اولیه داخل ایمیج را فقط اگر فایل مقصد وجود نداشته باشد کپی میکند
  (پس دیتای کاربران روی volume پاک نمیشود). `saveDBNow()` هنگام خطا
  (EROFS/EACCES/EPERM/ENOENT) بهمسیر بعدی فالبک میکند و لاگ واضح مینویسد.
  `/api/health` وضعیت `storage` را گزارش میدهد تا راحت تشخیص دهید روی `/tmp` افتادهاید یا `/data`.
- **سلامت:** `/health`، `/healthz` (200، بدون کش، بدون نیاز به دیتابیس)، `/api/health`
  و همچنین `/` که حالا صفحه لاگین را با 200 برمیگرداند (قبلًا 302 به `/login` بود).
- **خاموشی نرم:** `SIGTERM/SIGINT` ابتدا ترافیک و دیتابیس را flush میکند، سپس WS/SSE/اتصالات
  فعال را میبندد، بعد از 750ms اتصالات باقیمانده HTTP را قطع و بعد از 3s خارج میشود.
- **حافظه:** `process.constrainedMemory()` محدودیت cgroup را میخواند (Node 20.3+/22
  خودکار هیپ V8 را 50% آن قرار میدهد). در پلن 128MB، `MAX_XHTTP_BUFFERED_POSTS`
  از 32 به 8 و بافر downlink از 4MB به 1MB کاهش مییابد؛ همه با env قابل overrideاند.
- **Keepalive:** `KEEPALIVE_ENABLED=0` سلفپینگ 4 دقیقهای را خاموش میکند، چون
  پلن رایگان Deplexo always-on است و نیازی به جلوگیری از خواب نیست.
- `BUILD_ID` تغییر کرد تا در لاگ/`health` بتوانید نسخه جدید را از نسخه قدیمی تشخیص دهید.

---

## 4. چکلیست تنظیمات در دشبورد Deplexo

1. **منبع/برنچ:** مطمئن شوید دیپلوی روی کامیتی است که این تغییرات را دارد
   (در لاگ بیلد، SHA کامیت را چک کنید). صرفاً `Restart` کردن کانفیگ جدید را نمیخواند.
2. **Build method:** بهلطف `deplexo.yaml` روی `dockerfile` تنظیم میشود.
   اگر دشبورد همچنان `Auto / Node.js` نشان میدهد، دستی هم Dockerfile را انتخاب کنید.
3. **Start command:** خالی بگذارید (فایل YAML با `""` آن را پاک میکند).
   چیزی مثل `npm start` یا `npm install && npm start` وارد نکنید.
4. **Install/Build command:** خالی (در بیلد Dockerfile، `npm ci` انجام میشود).
   `npm run build` اگر هم اجرا شود فقط `node --check` است و خطایی ایجاد نمیکند.
5. **Port:** 3000 (توسط `run.port` تزریق میشود؛ نیازی به `PORT` جداگانه نیست).
6. **Storage/Mounts:** volume پایدار روی `/data` فعال باشد.
   اگر مسیر دیگری (مثلاً `/mnt/data`) دادهاید، `DB_PATH` را همان قرار دهید.
7. **Environment (اختیاری اما توصیهشده):**
   `KEEPALIVE_ENABLED=0`، و در صورت نیاز مقادیر `.env.example`.
8. **Redeploy** کنید.

### انتظار از لاگ runtime (نسخه درست)

```
[DB] Skipping unwritable location: /app/db.json
[DB] Seeded database from image at /data/db.json      (فقط بار اول)
[DB] Database path: /data/db.json
[DB] Storage mode: persistent-volume
[INFO] Mahan Panel VLESS-WS/XHTTP active on 0.0.0.0:3000
[INFO] Storage: persistent-volume - /data/db.json
```

اگر دیدید `Storage mode: ephemeral-tmp` یعنی `/data` قابل نوشتن برای کاربر `node`
نیست — مالکیت volume/تنظیمات mount را بررسی کنید (مستندات Deplexo: «Test filesystem
ownership with the production image user»). در غیر اینصورت اطلاعات با ریستارت پاک میشود.

### تست بعد از دیپلوی

```bash
curl -i https://<app>.de.deplexo.com/health         # 200 ok - deplexo-compatible-...
curl -i https://<app>.de.deplexo.com/healthz        # 200
curl -s  https://<app>.de.deplexo.com/api/health    # storage, port, node, memory_limit_mb
curl -I  https://<app>.de.deplexo.com/              # 200 (صفحه لاگین)
```

سپس وارد `/login` شوید (`admin` / `admin`) و **حتماً پسورد پیشفرض را عوض کنید**.
برای پشتیبانگیری از`/api/backup/restore` و خروجی کانفیگها استفاده کنید؛ volume تکورکر
است، بین ریجنها share نمیشود و با حذف اپلیکیشن پاک میشود
([منبع](https://docs.deplexo.com/operations/storage/#understand-where-files-are-stored)).

---

## 5. محدودیتهای باقیمانده (غیرکدی)

| مورد | توضیح |
|---|---|
| **سیاست استفاده** | Deplexo رسماً اجرای «proxy or VPN servers, anonymizers, tunnels that relay general internet traffic» را حتی برای استفاده شخصی و حتی پشت پسورد ممنوع کرده؛ نقض آن میتواند به توقف اپ، تعلیق اکانت و **حذف دائمی دیتا** منجر شود. پروژه شما دقیقاً چنین سرویسی است. رفع ارور فنی، مجوز استفاده ایجاد نمیکند. |
| **ترافیک/منابع** | Free: 128MB RAM، 0.25 CPU، 10Mbit/s، 100GB/ماه (در سقف، اپ متوقف میشود). ترافیک پروکسی خیلی زود این سقف را پر میکند. Starter: 512MB / 0.5CPU / 100Mbit / 500GB. |
| **UDP خروجی** | موتور VLESS برای UDP از `dgram` استفاده میکند؛ محدودیت UDP خروجی در مستندات Deplexo مشخص نشده و باید عملاً تست شود. |
| **WebSocket** | اشاره رسمی پیدا نشد؛ مسیر `/ws` نیازمند عبور Upgrade از روتر HTTPS است. |
| **تک اینستنس** | بدون autoscaling؛ هر دیپلوی وقفه کوتاهی ایجاد میکند و کلاینتها باید reconnect کنند. |
| **بدون دیتابیس مدیریتشده** | صرفاً volume لوکال؛ بکآپ خارجی بگیرید. |

**توصیه:** اگر قصد استفاده واقعی از این پنل را دارید، از هاستی استفاده کنید که
قوانینش صراحتاً VPN/پروکسی خصوصی را مجاز بداند (مثلاً VPS شخصی یا ارائهدهندهای که
AUP آن را تأیید کرده است). کد اصلاحشده روی چنین هاستی هم بدون مشکل اجرا میشود،
چون به `Dockerfile` + `PORT` + `DB_PATH` وابسته است و به پلتفرم خاصی گره نخورده است.
