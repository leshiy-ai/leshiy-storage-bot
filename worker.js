export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const hostname = url.hostname;

    // 1. ВЕБ-ИНТЕРФЕЙС (Твой оригинал)
    if (request.method === "GET" && url.pathname === "/") {
      return new Response(`
        <!DOCTYPE html>
        <html lang="ru">
          <head><meta charset="utf-8"><title>Telegram Bot "Storage" by Leshiy</title></head>
          <body style="font-family:sans-serif; text-align:center; padding-top:100px; background:#f4f4f4;">
            <div style="display:inline-block; background:white; padding:40px; border-radius:20px; box-shadow:0 10px 30px rgba(0,0,0,0.1);">
              <h1 style="margin:0;">Telegram Storage Bot "Хранилка" by Leshiy</h1>
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

    // 3. ОБРАБОТКА ВЕБХУКОВ ТЕЛЕГРАМА (POST запросы)
    if (request.method === "POST") {
      try {
        const update = await request.json();
    
        // Сначала проверяем кнопки (Callback)
        if (update.callback_query) {
          return await handleCallbackQuery(update.callback_query, env);
        }
    
        // Потом проверяем сообщения и файлы
        if (update.message || update.edited_message) {
          return await handleTelegramUpdate(update, env, hostname);
        }
      } catch (e) {
        console.error("Критическая ошибка:", e);
      }
      return new Response("OK", { status: 200 });
    }

    // Все остальное — 404
    return new Response("Not Found", { status: 404 });
  }
};

async function handleTelegramUpdate(update, env, hostname) {
  // --- ОБРАБОТКА НАЖАТИЙ (CALLBACK) ---
  if (update.callback_query) {
    // Если пришел колбэк, отдаем его специальной функции
    return await handleCallbackQuery(update.callback_query, env);
  }

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
    // Формируем статус с учетом выбранной папки
    let statusText = "❌ Диск не подключен";
    if (userData) {
      const folderInfo = userData.folderId ? ` (папка: <b>${userData.folderId}</b>)` : " (корень)";
      statusText = `✅ <b>${userData.provider}</b> подключен${folderInfo}`;
    }

    const welcome = `👋 <b>Привет! Я твоя личная хранилка.</b>\n\n` +
                    `📁 Просто пришли мне фото или видео, и я закину их на сервер.\n\n` +
                    `⚙️ Статус: ${statusText}\n\n` +
                    `📖 <b>Команды:</b>\n` +
                    `/folder — Выбрать папку для загрузки\n` +
                    `/debug — Техническая информация`;
    
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
    const currentFolder = userData?.folderId || "Не установлена (Root)";
    const debugMsg = `🤖 <b>Бот онлайн</b>\n` +
                     `📦 Версия: 2.1.0 (Stable)\n` +
                     `🔗 Статус: ${userData ? "✅ Соединение активно" : "❌ Ошибка"}\n` +
                     `📁 Текущая папка: <code>${currentFolder}</code>\n` +
                     `👤 Твой ID: <code>${userId}</code>\n` +
                     `👑 Админ: ${isAdmin ? "Да" : "Нет"}`;
    return await sendMessage(chatId, debugMsg, null, env);
  }

  // --- КОМАНДА /ADMIN ---
  if (text === "/admin" && isAdmin) {
    // 1. Получаем тех, кто уже авторизован (есть запись user:ID)
    const list = await env.USER_DB.list({ prefix: "user:" });
    const authIds = list.keys.map(k => k.name.split(":")[1]);

    // 2. Получаем тех, кому просто разрешен доступ через /add
    const allowedIds = await env.USER_DB.get("admin:allowed_ids", { type: "json" }) || [];

    // Объединяем уникальные ID для статистики
    const allUniqueIds = [...new Set([...authIds, ...allowedIds])];

    const adminMsg = `⚙️ <b>Панель администратора</b>\n\n` +
                     `✅ <b>Авторизованы (диск подключен):</b>\n` +
                     (authIds.length > 0 ? authIds.map(id => `• <code>${id}</code>`).join("\n") : "—") +
                     `\n\n🔑 <b>Разрешенные ID (ждут входа):</b>\n` +
                     (allowedIds.length > 0 ? allowedIds.map(id => `• <code>${id}</code>`).join("\n") : "—") +
                     `\n\n👤 <b>Всего охвачено:</b> ${allUniqueIds.length}`;

    return await sendMessage(chatId, adminMsg, null, env);
  }

  if (text.startsWith("/add") && isAdmin) {
    const targetId = text.split(" ")[1];
    if (!targetId) return await sendMessage(chatId, "⚠️ Формат: /add [ID]", null, env);

    const myData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
    if (!myData) return await sendMessage(chatId, "❌ Сначала авторизуй свой диск!", null, env);

    let folders = [];
    try {
      if (myData.provider === "google") {
        folders = await listGoogleFolders(myData.access_token);
      } else {
        folders = await listYandexFolders(myData.access_token);
      }
    } catch (e) {
      console.log("Folder list error", e);
    }

    // Инициализируем запись пользователя (копируем твой провайдер и токен)
    await env.USER_DB.put(`user:${targetId}`, JSON.stringify({
      provider: myData.provider,
      access_token: myData.access_token,
      ownerId: userId
    }));

    // Собираем кнопки
    let buttons = folders.map(f => [
      { text: `📁 ${f.name}`, callback_data: `set_folder:${targetId}:${f.id}` }
    ]);
    
    // Кнопка создания — ВСЕГДА первая
    buttons.unshift([{ text: "➕ Создать новую папку", callback_data: `create_folder:${targetId}` }]);

    const msgText = `👤 Пользователь <code>${targetId}</code> инициализирован.\n` +
                    `☁️ Облако: <b>${myData.provider}</b>\n\n` +
                    `👇 <b>Выбери папку или создай новую:</b>`;

    return await sendMessage(chatId, msgText, { inline_keyboard: buttons }, env);
  }

  // --- КОМАНДА /FOLDER ---
  if (text === "/folder") {
    if (!userData) return await sendMessage(chatId, "❌ Сначала подключи диск", null, env);
  
    let folders = [];
    try {
      if (userData.provider === "google") {
        // Используем listGoogleFolders (которую мы обсуждали выше)
        folders = await listGoogleFolders(userData.access_token);
      } else {
        folders = await listYandexFolders(userData.access_token);
      }
    } catch (e) {
      return await sendMessage(chatId, `❌ Ошибка списка папок: ${e.message}`, null, env);
    }
  
    // Собираем кнопки: для Google кладем в callback_data ID папки, для Яндекса - имя
    const buttons = folders.map(f => [
      { 
        text: `📁 ${f.name}`, 
        callback_data: `set_folder:${userId}:${userData.provider === 'google' ? f.id : f.name}` 
      }
    ]);
  
    // Добавляем кнопку создания папки "Storage" (или с другим именем)
    buttons.unshift([{ 
      text: "➕ Создать 'Storage'", 
      callback_data: `create_folder:${userId}:Storage` 
    }]);
  
    const msgText = `📂 <b>${userData.provider} Drive</b>\nВыбери папку для сохранения файлов:`;
    return await sendMessage(chatId, msgText, { inline_keyboard: buttons }, env);
  }

  // --- ОБРАБОТКА ФАЙЛОВ ---
  const isDoc = !!msg.document;
  const isVideo = !!msg.video;
  const isPhoto = !!msg.photo;

  if (isDoc || isVideo || isPhoto) {
    // 1. ПРОВЕРКА ДОСТУПА (Берем из GitHub логику)
    let allowed = await env.USER_DB.get("admin:allowed_ids", { type: "json" }) || [];
    const isAllowed = isAdmin || allowed.includes(String(userId));

    if (!isAllowed) {
      return await sendMessage(chatId, "🚫 <b>Доступ ограничен.</b>\nОбратитесь к администратору для получения разрешения.", null, env);
    }

    // 2. ПРОВЕРКА ПОДКЛЮЧЕНИЯ ДИСКА
    if (!userData) {
      return await sendMessage(chatId, "❌ <b>Диск не подключен.</b>\nИспользуйте /start для авторизации.", null, env);
    }
    
    await sendMessage(chatId, "⏳ <b>Начинаю загрузку в облако...</b>", null, env);
    
    try {
      const fileObj = msg.document || msg.video || (msg.photo ? msg.photo[msg.photo.length - 1] : null);
      
      // Логика формирования имени (как в 1.5.2)
      let fileName = "";
      if (isDoc || isVideo) {
        fileName = fileObj.file_name || `file_${Date.now()}`;
      } else {
        const now = new Date();
        const dateStr = now.toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = `Photo_${dateStr}.jpg`;
      }

      const { stream } = await getFileStream(fileObj.file_id, env);
      
      let success = false;
      if (userData.provider === "yandex") {
        // Добавляем userData.folderId в аргументы!
        success = await uploadToYandex(stream, fileName, userData.access_token, userData.folderId || "");
      } else if (userData.provider === "google") {
        success = await uploadToGoogle(stream, fileName, userData.access_token, userData.folderId);
      }

      if (success) {
        return await sendMessage(chatId, `✅ Файл <b>${fileName}</b> успешно сохранен в ${userData.provider}!`, null, env);
      } else {
        return await sendMessage(chatId, "❌ Ошибка при загрузке. Проверьте место на диске или токены.", null, env);
      }
    } catch (e) {
      return await sendMessage(chatId, `❌ Критическая ошибка: ${e.message}`, null, env);
    }
  }

  return new Response("OK");
}

async function handleCallbackQuery(query, env) {
  // 1. Сразу гасим часики на кнопке
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/answerCallbackQuery`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ callback_query_id: query.id })
  });

  const data = query.data; // Пример: "set_folder:12345:ID_ПАПКИ"
  const chatId = query.message.chat.id;
  const userId = query.from.id;

  const parts = data.split(":");
  const action = parts[0];
  const targetUserId = parts[1] || userId; // Для команды /add или личного использования
  const folderIdOrName = parts[parts.length - 1]; 

  try {
    const userData = await env.USER_DB.get(`user:${targetUserId}`, { type: "json" });
    if (!userData) return new Response("OK");

    if (action === "create_folder") {
      let finalId;
      if (userData.provider === "google") {
        finalId = await createGoogleFolder(folderIdOrName, userData.access_token);
      } else {
        await createYandexFolder(folderIdOrName, userData.access_token);
        finalId = folderIdOrName;
      }
      userData.folderId = finalId;
      await env.USER_DB.put(`user:${targetUserId}`, JSON.stringify(userData));
      await sendMessage(chatId, `✅ Папка <b>${folderIdOrName}</b> создана и выбрана!`, null, env);
      
    } else if (action === "set_folder") {
      // ВОТ ЭТОГО У ТЕБЯ НЕ ХВАТАЛО: Сохранение существующей папки
      userData.folderId = folderIdOrName;
      await env.USER_DB.put(`user:${targetUserId}`, JSON.stringify(userData));
      await sendMessage(chatId, `📂 Папка выбрана: <b>${folderIdOrName}</b>`, null, env);
    }

  } catch (e) {
    await sendMessage(chatId, `❌ Ошибка: ${e.message}`, null, env);
  }
  return new Response("OK");
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async function sendMessage(chatId, text, kb, env) {
  const payload = {
    chat_id: chatId,
    text: text,
    parse_mode: "HTML"
  };

  if (kb) {
    // Если kb уже строка (JSON), используем её, иначе превращаем объект в строку
    payload.reply_markup = typeof kb === 'string' ? JSON.parse(kb) : kb;
  }

  return await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

async function getFileStream(fileId, env) {
  const fRes = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/getFile?file_id=${fileId}`);
  const fData = await fRes.json();
  if (!fData.ok) throw new Error("Telegram API error: " + fData.description);
  
  const res = await fetch(`https://api.telegram.org/file/bot${env.TELEGRAM_TOKEN}/${fData.result.file_path}`);
  return { stream: res.body };
}

// --- CALLBACKS ---

// РАБОТА С ЯНДЕКС-ДИСКОМ
async function handleYandexCallback(req, env) {
  const u = new URL(req.url);
  const code = u.searchParams.get("code");
  const uid = u.searchParams.get("state");

  const res = await fetch("https://oauth.yandex.ru/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ 
      grant_type: "authorization_code", 
      code, 
      client_id: env.YANDEX_CLIENT_ID, 
      client_secret: env.YANDEX_CLIENT_SECRET 
    })
  });

  const d = await res.json();

  if (d.access_token) {
    const userData = { access_token: d.access_token, provider: "yandex" };
    await env.USER_DB.put(`user:${uid}`, JSON.stringify(userData));

    // СООБЩЕНИЕ 1: Подтверждение (быстро улетает в ТГ)
    await sendMessage(uid, "🎉 <b>Яндекс.Диск подключен!</b>", null, env);

    // СООБЩЕНИЕ 2: Список папок (отдельным вызовом)
    // Используем setTimeout или просто await, так как это JS в Воркере
    await showFolderSelector(uid, userData, env);

    return new Response("Успешно! Возвращайся в Telegram.");
  }
  return new Response("Error", { status: 400 });
}

async function showFolderSelector(chatId, userData, env) {
  try {
    // 1. Запрос к Яндексу (только папки, лимит 50)
    const res = await fetch("https://cloud-api.yandex.net/v1/disk/resources?path=/&limit=50", {
      headers: { "Authorization": `OAuth ${userData.access_token}` }
    });

    if (!res.ok) {
      return await sendMessage(chatId, `❌ Ошибка Яндекса: ${res.status}. Попробуй переподключить диск.`, null, env);
    }

    const data = await res.json();
    const items = data._embedded?.items || [];
    
    let buttons = [];

    // 2. Собираем кнопки из папок
    items.forEach(item => {
      if (item.type === 'dir') {
        buttons.push([{ 
          text: `📁 ${item.name}`, 
          callback_data: `set_folder::${item.name}` 
        }]);
      }
    });

    // 3. Всегда добавляем кнопку создания (в конец)
    buttons.push([{ text: "➕ Создать 'Storage'", callback_data: `create_folder:${chatId}:Storage` }]);

    const text = buttons.length > 1 
      ? "📂 <b>Твои папки на диске:</b>\nВыбери ту, куда сохранять файлы." 
      : "📂 <b>Папок не найдено.</b>\nНажми кнопку ниже, чтобы создать папку для бота.";

    return await sendMessage(chatId, text, { inline_keyboard: buttons }, env);

  } catch (e) {
    return await sendMessage(chatId, `❌ Ошибка: ${e.message}`, null, env);
  }
}

async function uploadToYandex(stream, name, token, folder = "") {
  // Яндекс требует, чтобы путь начинался с /
  let fullPath = folder ? `/${folder}/${name}` : `/${name}`;
  // Убираем двойные слэши, если они вдруг возникли
  fullPath = fullPath.replace(/\/+/g, '/');

  const getUrl = `https://cloud-api.yandex.net/v1/disk/resources/upload?path=${encodeURIComponent(fullPath)}&overwrite=true`;
  
  const r = await fetch(getUrl, {
    headers: { "Authorization": `OAuth ${token}` }
  });
  
  const d = await r.json();
  if (d.href) { 
    await fetch(d.href, { method: "PUT", body: stream }); 
    return true; 
  }
  return false;
}

async function listYandexFolders(token) {
  try {
    const res = await fetch(`https://cloud-api.yandex.net/v1/disk/resources?path=%2F&fields=_embedded.items.name%2C_embedded.items.type&limit=100`, {
      headers: { "Authorization": `OAuth ${token}` }
    });
    
    if (!res.ok) return [];

    const data = await res.json();
    // Фильтруем только папки и возвращаем их имена
    return (data._embedded?.items || [])
      .filter(item => item.type === 'dir')
      .map(item => ({ id: item.name, name: item.name }));
  } catch (e) {
    return [];
  }
}

// Создание папки на Яндексе
async function createYandexFolder(name, token) {
  const path = name.startsWith('/') ? name : '/' + name;
  const res = await fetch(`https://cloud-api.yandex.net/v1/disk/resources?path=${encodeURIComponent(path)}`, {
    method: "PUT",
    headers: { "Authorization": `OAuth ${token}` }
  });

  // 201 - создана, 409 - уже есть. Оба варианта нам подходят.
  return res.status === 201 || res.status === 409;
}

// РАБОТА С GOOGLE-ДИСКОМ
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

async function uploadToGoogle(stream, name, token, folderId = "root") {
  const meta = { 
    name: name, 
    parents: (folderId && folderId !== "root") ? [folderId] : [] 
  };
  
  const fd = new FormData();
  fd.append('metadata', new Blob([JSON.stringify(meta)], { type: 'application/json' }));
  fd.append('file', new Blob([await new Response(stream).arrayBuffer()]));
  
  const res = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
    method: 'POST', 
    headers: { 'Authorization': `Bearer ${token}` }, 
    body: fd
  });
  
  return res.ok;
}

async function listGoogleFolders(token) {
  const url = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.folder' and trashed=false&fields=files(id, name)";
  
  const res = await fetch(url, {
    headers: { "Authorization": `Bearer ${token}` }
  });

  if (!res.ok) {
    const errorBody = await res.text();
    throw new Error(`Google API Error: ${res.status} - ${errorBody}`);
  }

  const data = await res.json();
  // Если файлов нет, вернется пустой массив, а не undefined
  return data.files || [];
}

async function createGoogleFolder(folderName, token, parentId = "root") {
  const meta = {
    name: folderName,
    mimeType: "application/vnd.google-apps.folder",
    parents: [parentId] // По умолчанию в корень или в твою рабочую папку
  };

  const res = await fetch('https://www.googleapis.com/drive/v3/files', {
    method: 'POST',
    headers: { 
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(meta)
  });
  const data = await res.json();
  return data.id; // Возвращает ID созданной папки
}