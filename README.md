# Mahan Panel — VLESS WS/XHTTP (Deplexo-compatible)

پنل مدیریت کانفیگ‌های VLESS روی WebSocket/XHTTP، با موتور داخلی Node.js (بدون xray-core).

## اجرای محلی

```bash
npm ci
npm start              # http://localhost:3000/login  (admin / admin)
```

## دیپلوی روی Deplexo

1. این فایل‌ها حتماً داخل کامیت/برنچ متصل به Deplexo باشند:
   `deplexo.yaml`، `Dockerfile`، `package-lock.json`، `server.js`، `db.json`، `public/`.
2. در تنظیمات اپلیکیشن Deplexo، **Storage / Mounts** را روی مسیر پیشفرض `/data` نگه دارید
   (یا اگر عوض کردید، متغیر `DB_PATH` را هماهنگ کنید).
3. **Redeploy** کنید (نه صرفاً Restart)؛ چون `deplexo.yaml` فقط هنگام دیپلوی خوانده میشود.
4. لاگها را چک کنید: باید `[DB] Storage mode: persistent-volume` و
   `[INFO] Mahan Panel ... on 0.0.0.0:3000` ببینید و **هیچ خط `npm install` در لاگ runtime نباشد**.
5. `https://<app>.de.deplexo.com/health` و `/api/health` را تست کنید، سپس پسورد ادمین را عوض کنید.

### نکات کلیدی

- ریشه فایل سیستم Deplexo در زمان اجرا **فقط خواندنی** است؛ وابستگیها فقط در زمان
  **build** داخل ایمیج نصب میشوند (`npm ci`). هیچ `prestart`/`npm install`ای در استارتاپ اجرا نمیشود.
- دیتابیس روی `/data/db.json` (volume پایدار) نوشته میشود. اگر `/data` قابل نوشتن نباشد،
  برنامه به `/tmp/db.json` (موقت) فالبک میکند و در لاگ هشدار میدهد.
- متغیرها: `.env.example` را ببینید. `PORT` توسط `run.port` در `deplexo.yaml` تزریق میشود.
- پلن رایگان فقط 128MB RAM / 0.25 CPU / 10Mbit/s / 100GB ترافیک ماهانه دارد؛
  حالت کمحافظه بهصورت خودکار بافرهای XHTTP را کوچک میکند.

> ⚠️ **هشدار مهم:** قوانین Deplexo اجرای سرور پروکسی/VPN/تانل ترافیک عمومی اینترنت را
> حتی بهصورت خصوصی ممنوع کرده است. این پروژه چنین سرویسی ارائه میدهد؛ رفع مشکلات فنی
> دیپلوی بهمعنای مجاز بودن آن نیست. جزئیات در `DEPLEXO_DEPLOY_GUIDE.md`.

راهنمای کامل عیبیابی و تنظیمات: **[DEPLEXO_DEPLOY_GUIDE.md](DEPLEXO_DEPLOY_GUIDE.md)**
