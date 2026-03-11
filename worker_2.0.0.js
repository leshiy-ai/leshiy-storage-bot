export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 1. ВЕБ-ИНТЕРФЕЙС (Твой оригинал)
    if (request.method === "GET" && url.pathname === "/") {
      return new Response(`
        <!DOCTYPE html>
        <html lang="ru">
          <head><meta charset="utf-8"><title>Хранилка Bot</title></head>
          <body style="font-family:sans-serif; text-align:center; padding-top:100px; background:#f4f4f4;">
            <div style="display:inline-block; background:white; padding:40px; border-radius:20px; box-shadow:0 10px 30px rgba(0,0,0,0.1);">
              <h1 style="margin:0;">🤖 Хранилка Bot</h1>
              <p style="color:green; font-weight:bold;">✅ Система работает штатно</p>
              <hr style="border:0; border-top:1px solid #eee; margin:20px 0;">
              <a href="https://t.me/leshiy_storage_bot" style="display:inline-block; background:#0088cc; color:white; padding:12px 25px; border-radius:50px; text-decoration:none; font-weight:bold;">Открыть бота в Telegram</a>
            </div>
          </body>
        </html>`, { headers: { "Content-Type": "text/html; charset=utf-8" } });
    }

    // 2. ОБРАБОТКА CALLBACKS (Auth)
    if (url.pathname === "/auth/yandex/callback") return await handleYandexCallback(request, env);
    if (url.pathname === "/auth/google/callback") return await handleGoogleCallback(request, env);

    // 3. ТЕЛЕГРАМ (Webhook)
    if (request.method === "POST") {
      try {
        const update = await request.json();
        return await handleTelegramUpdate(update, env, url.hostname);
      } catch (e) {
        return new Response("OK");
      }
    }

    return new Response("Alive", { status: 200 });
  }
};

async function handleTelegramUpdate(update, env, hostname) {
  const msg = update.message || update.edited_message;
  if (!msg) return new Response("OK");

  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text || "";

  // Данные админа и юзера
  const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
  const isAdmin = adminCfg && Number(adminCfg.id) === Number(userId);
  const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });

  // --- КОМАНДА /START ---
  if (text === "/start") {
    const statusText = userData ? `✅ <b>${userData.provider}</b> подключен` : "❌ Диск не подключен";
    const welcome = `👋 <b>Привет! Я твоя личная хранилка.</b>\n\n` +
                    `📁 Просто пришли мне фото или видео, и я закину их на сервер.\n\n` +
                    `⚙️ Статус: ${statusText}`;
    
    const yAuth = `https://oauth.yandex.ru/authorize?response_type=code&client_id=${env.YANDEX_CLIENT_ID}&state=${userId}`;
    const gAuth = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${env.GOOGLE_CLIENT_ID}&redirect_uri=https://${hostname}/auth/google/callback&response_type=code&scope=${encodeURIComponent("https://www.googleapis.com/auth/drive.file")}&state=${userId}&access_type=offline&prompt=consent`;
    
    const kb = {
      inline_keyboard: [
        [{ text: "🔗 Подключить Яндекс.Диск", url: yAuth }],
        [{ text: "🔗 Подключить Google Drive", url: gAuth }]
      ]
    };
    return await sendMessage(chatId, welcome, kb, env);
  }

  // --- КОМАНДА /DEBUG ---
  if (text === "/debug") {
    const debugMsg = `🤖 <b>Бот онлайн</b>\n📦 Версия: 2.1.0 (Stable)\n🔗 Статус: ${userData ? "✅ Соединение активно" : "❌ Ошибка"}\n👤 Твой ID: <code>${userId}</code>\n👑 Админ: ${isAdmin ? "Да" : "Нет"}`;
    return await sendMessage(chatId, debugMsg, null, env);
  }

  // --- КОМАНДА /ADMIN ---
  if (text === "/admin" && isAdmin) {
    const list = await env.USER_DB.list({ prefix: "user:" });
    const adminMsg = `⚙️ <b>Панель администратора</b>\n\n` +
                     `🆔 Авторизованные ID:\n` +
                     list.keys.map(k => `• <code>${k.name.split(":")[1]}</code>`).join("\n") +
                     `\n\n👤 Всего пользователей: ${list.keys.length}`;
    return await sendMessage(chatId, adminMsg, null, env);
  }

  // --- КОМАНДА /ADD ---
  if (text.startsWith("/add") && isAdmin) {
    const targetId = text.split(" ")[1];
    if (!targetId) return await sendMessage(chatId, "⚠️ Формат: <code>/add [ID]</code>", null, env);
    
    let allowed = await env.USER_DB.get("admin:allowed_ids", { type: "json" }) || [];
    if (!allowed.includes(targetId)) {
      allowed.push(targetId);
      await env.USER_DB.put("admin:allowed_ids", JSON.stringify(allowed));
    }
    return await sendMessage(chatId, `✅ Пользователь <code>${targetId}</code> получил доступ.`, null, env);
  }

  // --- ОБРАБОТКА ФАЙЛОВ ---
  const isDoc = !!msg.document;
  const isVideo = !!msg.video;
  const isPhoto = !!msg.photo;

  if (isDoc || isVideo || isPhoto) {
    if (!userData) return await sendMessage(chatId, "❌ Сначала подключи диск через /start", null, env);
    
    await sendMessage(chatId, "⏳ <b>Начинаю загрузку в облако...</b>", null, env);
    
    try {
      // Выбираем объект файла
      const fileObj = msg.document || msg.video || (msg.photo ? msg.photo[msg.photo.length - 1] : null);
      
      // Логика формирования имени
      let fileName = "";
      if (isDoc || isVideo) {
        fileName = fileObj.file_name || `file_${Date.now()}`;
      } else {
        // Для фото делаем красивую дату
        const now = new Date();
        const dateStr = now.toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = `Photo_${dateStr}.jpg`;
      }

      const { stream } = await getFileStream(fileObj.file_id, env);
      
      let success = false;
      if (userData.provider === "yandex") success = await uploadToYandex(stream, fileName, userData.access_token);
      else if (userData.provider === "google") success = await uploadToGoogle(stream, fileName, userData.access_token);
      
      if (success) return await sendMessage(chatId, `✅ Файл <b>${fileName}</b> успешно сохранен в ${userData.provider}!`, null, env);
      else return await sendMessage(chatId, "❌ Ошибка при загрузке.", null, env);
    } catch (e) {
      return await sendMessage(chatId, `❌ Критическая ошибка: ${e.message}`, null, env);
    }
  }

  return new Response("OK");
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async function sendMessage(chatId, text, kb, env) {
  const body = { chat_id: chatId, text: text, parse_mode: "HTML" };
  if (kb) body.reply_markup = kb;
  return await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
  });
}

async function getFileStream(fileId, env) {
  const fRes = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/getFile?file_id=${fileId}`);
  const fData = await fRes.json();
  if (!fData.ok) throw new Error("Telegram API error: " + fData.description);
  
  const res = await fetch(`https://api.telegram.org/file/bot${env.TELEGRAM_TOKEN}/${fData.result.file_path}`);
  return { stream: res.body };
}

async function uploadToYandex(stream, name, token) {
  const path = `Загрузки/${name}`;
  const r = await fetch(`https://cloud-api.yandex.net/v1/disk/resources/upload?path=${encodeURIComponent(path)}&overwrite=true`, {
    headers: { "Authorization": `OAuth ${token}` }
  });
  const d = await r.json();
  if (d.href) { await fetch(d.href, { method: "PUT", body: stream }); return true; }
  return false;
}

async function uploadToGoogle(stream, name, token) {
  const meta = { name: name, parents: ["0B-MvdtMs1jihN2E1MzhiZjEtYjAyMC00ZTBhLWFhYjEtYTdlYzQ0OGE4OGZm"] };
  const fd = new FormData();
  fd.append('metadata', new Blob([JSON.stringify(meta)], { type: 'application/json' }));
  fd.append('file', new Blob([await new Response(stream).arrayBuffer()]));
  const res = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
    method: 'POST', headers: { 'Authorization': `Bearer ${token}` }, body: fd
  });
  return res.ok;
}

// --- CALLBACKS ---

async function handleYandexCallback(req, env) {
  const u = new URL(req.url);
  const code = u.searchParams.get("code");
  const uid = u.searchParams.get("state");
  const res = await fetch("https://oauth.yandex.ru/token", {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "authorization_code", code, client_id: env.YANDEX_CLIENT_ID, client_secret: env.YANDEX_CLIENT_SECRET })
  });
  const d = await res.json();
  if (d.access_token) {
    await env.USER_DB.put(`user:${uid}`, JSON.stringify({ access_token: d.access_token, provider: "yandex" }));
    await sendMessage(uid, "🎉 <b>Яндекс.Диск подключен!</b>", null, env);
    return new Response("Успешно! Возвращайся в Telegram.");
  }
  return new Response("Error");
}

async function handleGoogleCallback(req, env) {
  const u = new URL(req.url);
  const code = u.searchParams.get("code");
  const uid = u.searchParams.get("state");
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ code, client_id: env.GOOGLE_CLIENT_ID, client_secret: env.GOOGLE_CLIENT_SECRET, redirect_uri: `https://${u.hostname}/auth/google/callback`, grant_type: "authorization_code" })
  });
  const d = await res.json();
  if (d.access_token) {
    await env.USER_DB.put(`user:${uid}`, JSON.stringify({ access_token: d.access_token, provider: "google" }));
    await sendMessage(uid, "✅ <b>Google Drive подключен!</b>", null, env);
    return new Response("Успешно! Возвращайся в Telegram.");
  }
  return new Response("Error");
}