# رفع خطای دیپلوی روی Belmo (Coolify + Nixpacks)

تاریخ: 2026-10-25 · نسخه: 2.2.0 · لاگ بررسیشده: دیپلوی `6b4b4d3` روی `test-25e0.onbelmo.uk`

---

## ۱. لاگ شما در واقع چه میگوید؟

```
Found a suitable build server (build-server).
Docker 29.5.2 with BuildKit and Buildx detected ...
Generating nixpacks configuration with: nixpacks plan -f json
   --env 'NIXPACKS_NODE_VERSION=22' ...
   --build-cmd 'npm ci && npm run build' --start-cmd 'npm start'
...
╔══════════ Nixpacks v1.41.0 ══════════╗
║ setup   │ nodejs_22, npm-9_x, ...    ║
║ install │ npm ci                     ║
║ build   │ npm ci && npm run build    ║   ← دومین npm ci
║ start   │ npm start                  ║
╚══════════════════════════════════════╝
...
#10 38.26 these 81 paths will be fetched (126.98 MiB download, 607.75 MiB unpacked)
#10 39.02 copying path '/nix/store/...'      ← لاگ اینجا قطع شده
```

**نکتهٔ اول:** لاگی که فرستادید **قبل از پیام خطا تمام میشود**. آخرین خطوط مربوط به دانلود
ابزار Node/NPM توسط Nix است (فاز `setup`)، نه خود اپلیکیشن. پس متن دقیق ارور توی
چند خط آخر (که نیومده) قرار دارد. طبق الگوی پلن بالا، تقریباً قطعاً این است:

```
npm error code EBUSY
npm error syscall rmdir
npm error path /app/node_modules/.cache
npm error EBUSY: resource busy or locked, rmdir '/app/node_modules/.cache'
process "... npm ci && npm run build" did not complete successfully: exit code: 240
```

(اگر لاگ کامل چیز دیگری گفت، بخش ۶ را ببینید.)

---

## ۲. علت اصلی: تکرار `npm ci` + کش BuildKit

دستور `npm ci && npm run build` همان چیزی است که برای **Abasthan** توصیه کرده بودم،
چون Abasthan هر بیلد را در محیط کاملاً تمیز شروع میکند. ولی Belmo پشتش **Coolify**
قرار دارد و از **Nixpacks** استفاده میکند که **خودش در فاز `install` یک `npm ci` اجرا
میکند**. نتیجه: دو بار `npm ci`.

تو Dockerfile تولیدشدهٔ لاگ شما دقیقاً دیده میشود:

```dockerfile
# install phase
RUN --mount=type=cache,...,target=/root/.npm npm ci

# build phase
RUN --mount=type=cache,...,target=/app/node_modules/.cache npm ci && npm run build
```

فاز build یک **cache mount** روی `/app/node_modules/.cache` دارد. `npm ci` طبق
مستنداتش قبل از نصب، کل `node_modules` را پاک میکند ([23](https://docs.npmjs.com/cli/v10/commands/npm-ci/)،
[18](https://benjamincrozat.com/npm-ci)). نمیشود یک mount-point را `rmdir` کرد →
`EBUSY` →exit 240 → بیلد fail.

این یک باگ شناختهشده و پرتکرار Nixpacks/Railpack/Coolify است؛ نمونههای مستقل:
[1](https://github.com/AlexFitzgerald47/cracking-problems-hub/pull/15)،
[2](https://github.com/guilhermedevbh/Captador-de-Nota-Fiscal/pull/3)،
[4](https://github.com/tdechat-cmd/kay-tiendanube-mcp/pull/1)،
[7](https://github.com/OLDmamorim/Trinity/pull/3)،
[10](https://github.com/greecon-tech/web/pull/7)،
[11](https://github.com/guilhermedevbh/Captador-de-Nota-Fiscal/pull/2)،
[13](https://github.com/railwayapp/railpack/issues/255)،
[9](https://github.com/PrayudhaSatria70/sync-cakrawala/pull/2).
راهحل پیشنهادی آن PRها هم دقیقاً همین است: **حذف `npm ci` دوم** یا **سوییچ به
Dockerfile**.

### مشکل دوم: بوتاسترپ ۶۰۷ مگابایتی Nix

`'setup' │ nodejs_22, npm-9_x, curl, wget` یعنی هر بیلد (مخصوصاً بیلد اول یا بعد از
تبدیل `SOURCE_COMMIT`) حدود ۱۲۷ مگابایت دانلود و ۶۰۸ مگابایت unpack میکند. روی
بیلدسرور اشتراکی ممکن است با `no space left on device` یا تایماوت مواجه شود
([17](https://github.com/coollabsio/coolify/discussions/3192)،
[5](https://github.com/coollabsio/coolify/issues/3981) — آنجا `npm ci` با exit 137
یعنی OOM مرده). ضمن اینکه Nixpacks رسماً در حالت maintenance است و Coolify خودش
پیشنهاد میکند از Dockerfile استفاده کنید
([2](https://coolify.io/docs/applications/builds/nixpacks)،
[14](https://coolify.io/docs/applications/build-packs/railpack)).

---

## ۳. تغییراتی که اعمال شد

| فایل | تغییر |
|---|---|
| **`nixpacks.toml`** ⭐ | پلن صریح: `providers=["node"]`، فقط **یک** `npm ci` در فاز install، فاز build فقط `npm run build`، و **`cacheDirectories = []`** برای حذف اون mount مشکلدار. `start` مستقیم `node server.js`. عمداً بخش `[variables]` ندارد تا ARG/ENV خالی نسازد. |
| `Dockerfile` | هلثچک از پورت ثابت 3000 به `${PORT:-3000}` تغییر کرد (Belmo پورت تزریق میکند)؛ `STOPSIGNAL SIGTERM` اضافه شد؛ `start-period` به ۲۰ ثانیه. |
| `server.js` | `BUILD_ID` → `multi-platform-2026-10-25-b` تا نسخهٔ جدید تو `/health` قابل تشخیص باشد. |
| `package.json` / lock | نسخه `2.2.0`. |
| `README.md`، `.env.example`، `ABASTHAN_DEPLOY_GUIDE.md` | ماتریس «کدام پلتفرم چه Build Commandـی میخواهد» + هشدار عدم استفاده از دستور Abasthan روی Coolify. |

### چرا `cacheDirectories = []` کار میکند؟

سورس Nixpacks v1.41.0 رو خواندم: ترتیب mergeـه پلن
`provider < file < environment < CLI` است
([9](https://codexpedite.com/deploying-laravel-on-coolify-with-nixpacks-the-complete-production-blueprint/)،
[1](https://coolify.io/docs/builds/packs/nixpacks)). تابع
`fill_auto_in_vec(original, replacer)` در `src/nixpacks/plan/utils.rs` اگر `replacer`
یک آرایهٔ خالی باشد، همان آرایهٔ خالی را برمیگرداند — یعنی `cacheDirectories = []`
واقعاً `["node_modules/.cache"]`ـه provider را حذف میکند. نتیجه: دیگه
`--mount=...node_modules/.cache`ـای تو Dockerfile تولید نمیشود، پس **حتی اگر**
Build Command قدیمی `npm ci && npm run build` تو دشبورد بماند، `npm ci` دوم دیگر
EBUSY نمیدهد. (ولی باز هم پاک کردنش توصیه میشود — سریعتر است.)

> ⚠️ Coolify فیلدهای Install/Build/Start Command را بهصورت **CLI flag** پاس میدهد و
> CLI بالاترین اولویت را دارد. یعنی `cmds` و `start.cmd` داخل `nixpacks.toml` فقط
> وقتی کامل اعمال میشوند که آن فیلدها تو دشبورد خالی باشند؛ ولی
> `cacheDirectories = []` چون توی CLI چیزی برایش نیست، همیشه اعمال میشود.

---

## ۴. تنظیمات دشبورد Belmo ✅

### گزینهٔ A — توصیهشده: Build Pack = **Dockerfile**

این گزینه کل مرحلهٔ Nix و کش mount مشکلدار را دور میزند. سرور `Dockerfile` ما
همین الان برای چند پلتفرم تست شده است.

| فیلد | مقدار |
|---|---|
| Build Pack / Build strategy | **Dockerfile** (پیشفرض Coolify همان Nixpacks است، باید عوض شود — [6](https://coolify.io/docs/applications/build-packs/dockerfile)) |
| Base Directory | `/` |
| Dockerfile location | `/Dockerfile` |
| **Install Command / Build Command / Start Command** | **همه خالی** (Start Command خالی بماند تا `CMD` ایمیج استفاده شود) |
| **Ports Exposes** | **3000** |
| Health check | مسیر `/health` (یا بگذارید `HEALTHCHECK` خود ایمیج کار کند؛ داخل alpine همان `wget` هست — [7](https://coolify.io/docs/knowledge-base/health-checks)) |
| Environment | `PORT` توسط Belmo تزریق میشود؛ `DB_PATH` اختیاری |
| Storage (اختیاری) | volume پایدار روی `/data` برای `db.json` |

> Coolify برای Dockerfileها معمولاً healthcheck دشبورد را خاموش نگه میدارد چون
> `HEALTHCHECK` داخل ایمیج را تشخیص میدهد. اگر چک HTTP فعلاً کردید، حتماً پورت داخلی
> (3000) را بدهید، نه پورت عمومی.

### گزینهٔ B — ماندن روی Nixpacks

| فیلد | مقدار |
|---|---|
| Build Pack | Nixpacks |
| Base Directory | `/` |
| **Build Command** | **خالی** یا `npm run build` — هرگز `npm ci && npm run build` نگذارید |
| Install Command | خالی (بگذارید `nixpacks.toml` تصمیم بگیرد) |
| Start Command | خالی (می شود `node server.js`) یا `npm start` — هر دو کار میکنند |
| Ports Exposes | 3000 |
| Health check | `GET /health` (پلن شامل `curl` و `wget` است) |
| env | `NIXPACKS_NODE_VERSION=22` — اگر فاز setup شکست خورد، موقتاً `20` بگذارید ([1](https://github.com/coollabsio/coolify/issues/5065)، [3](https://github.com/coollabsio/coolify/issues/4890)، [9](https://github.com/Off-Nominal/ndb2/pull/182)) |

بعد از تغییر تنظیمات حتماً **Redeploy** کنید (Restart کانفیگ بیلد را دوباره
نمیسازد).

---

## ۵. انتظار از لاگ بعد از اصلاح

```
╔══════════════ Nixpacks v1.41.0 ═════════════╗
║ install    │ npm ci --omit=dev ...          ║
║ build      │ npm run build                  ║   ← دیگر npm ci دوم نیست
║ start      │ node server.js                 ║
╚═════════════════════════════════════════════╝
#12 RUN npm run build
[deps] OK - all runtime dependencies present; skipping npm install
#xx DONE
...
[INFO] Boot: node v22.x | PORT env = 3000 | resolved = 3000 | host = 0.0.0.0
[INFO] Mahan Panel ... active on 0.0.0.0:3000
New container is healthy. Rolling update completed.
```

### تست

```bash
curl -i https://test-25e0.onbelmo.uk/health     # 200  ok - multi-platform-2026-10-25-b
curl -i https://test-25e0.onbelmo.uk/healthz    # 200
curl -s  https://test-25e0.onbelmo.uk/api/health
curl -I  https://test-25e0.onbelmo.uk/          # 200
```

سپس `/login` (`admin`/`admin`) → حتماً پسورد را عوض کنید.

---

## ۶. اگر هنوز fail شد (لاگتان ناقص بود)

۱. تو صفحهٔ Build Logs روی **Show Debug Logs** بزنید و **۶۰ خط آخر** را بفرستید؛
   لاگی که دادید وسط فاز `setup` قطع شده و پیام خطا توش نیست.
۲. دنبال این رشتهها بگردید:
   - `EBUSY ... rmdir '/app/node_modules/.cache'` → همان مشکل بالا؛ مطمئن شوید
     `nixpacks.toml` تو برنچ `main` هست و Build Command دشبورد پاک شده.
   - `no space left on device` / `exit code: 137` → کمبود دیسک/RAM بیلدسرور؛
     گزینهٔ A (Dockerfile) را انتخاب کنید یا با پشتیبانی Belmo تماس بگیرید
     ([17](https://github.com/coollabsio/coolify/discussions/3192)،
     [23](https://azdigi.com/en/blog/self-hosted/coolify-production-backup-security)).
   - `undefined variable 'nodejs_22'` → نسخهٔ Nix ناسازگار؛ `NIXPACKS_NODE_VERSION=20`
     بگذارید ([9](https://github.com/Off-Nominal/ndb2/pull/182)،
     [26](https://coolify.io/docs/applications/build-packs/nixpacks/node-versioning)).
   - `New container is not healthy` → پورت اشتباه یا مسیر healthcheck؛
     `Ports Exposes` را 3000 و path را `/health` بگذارید
     ([27](https://coolify.io/docs/troubleshoot/applications/no-available-server)،
     [7](https://coolify.io/docs/knowledge-base/health-checks)).
   - `/.nixpacks/nixpkgs-....nix: not found` → `.dockerignore` جلوی کپی شدن
     `.nixpacks/` را گرفته ([11](https://github.com/Nexeor-dev-space/maison-palettia-design/pull/50)).
     فایل `.dockerignore` فعلی ما `.nixpacks/` را حذف نمیکند، ولی اگر تغییرش دادید
     چک کنید.
   - `map has no entry for key "Health"` → باگ شناختهشدهٔ Coolify در تشخیص
     healthcheck برای ایمیجهای Nixpacks ([6](https://github.com/coollabsio/coolify/issues/7223))؛
     گزینهٔ A (Dockerfile) یا خاموش کردن healthcheck دشبورد.
۳. برنچی که Belmo دیپلوی میکند `main` است — مطمئن شوید تغییرات
   (`nixpacks.toml` + `Dockerfile` + `.dockerignore`) روش merge شده باشد.

---

## ۷. یادآوریهای همیشگی

- **دیتابیس:** Belmo/Coolify فایل سیستم موقت است. اگر volume نگیرید، `db.json` با هر
  دیپلوی ریست میشود — کد ما به `/tmp` فالبک میکند و تو `/api/health` مینویسد
  `storage: ephemeral-tmp`. یا volume روی `/data` بگیرید و `DB_PATH=/data/db.json`
  بدهید، یا مرتب `/api/backup/restore` بکآپ بگیرید.
- **سیاست استفاده:** قبلاً در مورد Deplexo هشدار دادیم؛ برای Belmo هم قوانین
  سرویس را در مورد پروکسی/VPN چک کنید.
- **ماتریس Build Command (برای جلوگیری از تکرار این اشتباه):**

| پلتفرم | Build Command |
|---|---|
| Abasthan | `npm ci && npm run build` |
| **Belmo / Coolify (Nixpacks)** | خالی یا `npm run build` |
| **Belmo / Coolify (Dockerfile)** | خالی |
| Deplexo | خالی (`Dockerfile` همهچیز را انجام میدهد) |
