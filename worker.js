/* 🗄 Telegram Storage Bot "Хранилка" by Leshiy
Личный Telegram-бот для автоматической загрузки фото и видео на домашний FTP/SFTP/WEBDAV-сервер. 
Бот поддерживает сохранение исходных имен файлов и автоматическую сортировку по папкам пользователей.

✨ Основные функции
Универсальность: Поддержка классического FTP, защищенного SFTP и облачного WebDAV (Google, Яндекс.Диск, Облако Mail.Ru и др.).
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео.
Поддержка WEBM: Возможность сохранять видеофайлы в современных форматах без потери качества.
Диагностика: Команда /debug для проверки статуса подключения к хранилищу в реальном времени.
*/
// Глобальные константы
const version = "v2.1.4"; // актуальная версия

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
    if (url.pathname === "/auth/mailru/callback") return await handleMailruCallback(request, env);
    if (url.pathname === "/auth/dropbox/callback") return await handleDropboxCallback(request, env);
  
  // Ответ для Mail.ru, чтобы он нашел файл receiver.html
  if (url.pathname.endsWith("receiver.html")) {
    const receiverHtml = `<html>
  <body>
  <script src="//connect.mail.ru/js/loader.js"></script>
  <script>
  mailru.loader.require('receiver', function(){
    mailru.receiver.init();
  })
  </script>
  </body>
  </html>`;
    
    return new Response(receiverHtml, {
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }

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
    return await handleCallbackQuery(update.callback_query, env);
  }

  const msg = update.message || update.edited_message;
  if (!msg) return new Response("OK");

  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text || "";
  const userKey = `user:${userId}`;

  // Данные админа и базовая загрузка данных пользователя
  const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
  const isAdmin = adminCfg && Number(adminCfg.id) === Number(userId);
  let userData = await env.USER_DB.get(userKey, { type: "json" });

  // --- 1. МОСТ ДЛЯ РЕФЕРАЛА (Приоритет) ---
  // Если у пользователя в базе есть пометка shared_from, подтягиваем данные владельца
  if (userData && userData.shared_from) {
    const ownerId = String(userData.shared_from);
    const ownerData = await env.USER_DB.get(`user:${ownerId}`, { type: "json" });

    if (ownerData) {
      // Сохраняем ID рефа, но для сессии используем токены владельца
      const originalRefId = userId;
      userData = { 
        ...ownerData, 
        is_ref: true, 
        real_user_id: originalRefId, 
        shared_from: ownerId 
      };
    }
  }

  // --- 2. КОМАНДА /START (Доступна ВСЕМ, не блокируется проверками) ---
  if (text.startsWith("/start")) {
    const args = text.split(" ")[1];
    let inviteData = null;

    // Проверка инвайта по ссылке ?start=ref_XXX
    if (args && args.startsWith("ref_")) {
      const token = args.split("_")[1];
      inviteData = await env.USER_DB.get(`invite:${token}`, { type: "json" });

      if (inviteData) {
        const ownerData = await env.USER_DB.get(`user:${inviteData.inviterId}`, { type: "json" });
        if (ownerData) {
          // Создаем связь в базе
          userData = { 
            provider: ownerData.provider, 
            shared_from: String(inviteData.inviterId), 
            connected_at: Date.now() 
          };
          await env.USER_DB.put(userKey, JSON.stringify(userData));
          
          await sendMessage(chatId, `🤝 <b>Готово!</b>\nТы подключился к хранилке пользователя <code>${inviteData.inviterId}</code> (${ownerData.provider}).`, null, env);
          await sendMessage(inviteData.inviterId, `🔔 Твоей хранилкой начал пользоваться ID <code>${userId}</code>`, null, env);
        }
      } else {
        await sendMessage(chatId, "❌ Ссылка недействительна или устарела.", null, env);
      }
    }

    // Уведомление админу о новом юзере (только если это первый заход)
    if (!userData && !isAdmin) {
      const report = `👤 Новый пользователь: ${msg.from.first_name || "ᅠ"}\n` +
                     `🆔 ID: <code>${userId}</code>\n` +
                     `📂 Статус: Ожидает подключения`;
      await logToAdmin(report, env);
    }

    // Формирование текста приветствия
    let statusText = "❌ Диск не подключен";
    if (userData && userData.provider) {
      const folderInfo = userData.folderId ? ` (папка: <b>${userData.folderId}</b>)` : " (корень)";
      const sharedInfo = userData.shared_from ? ` [Общий диск]` : "";
      statusText = `✅ <b>${userData.provider}</b> подключен${folderInfo}${sharedInfo}`;
    }

    let welcome = `👋 <b>Привет! Я твоя личная хранилка.</b>\n\n` +
                  `📁 Просто пришли мне фото или видео, и я закину их на сервер.\n\n` +
                  `⚙️ Статус: ${statusText}\n\n` +
                  `📖 <b>Команды:</b>\n` +
                  `/folder — Выбрать папку для загрузки\n` +
                  `/share — Создать ссылку для друга\n` +
                  `/disconnect — Отключить диск друга\n` +
                  `/debug — Техническая информация`;
    
    if (inviteData && !userData?.shared_from) {
      welcome += `\n\n🎁 <b>Найдено приглашение!</b>\nОт владельца облака <b>${inviteData.provider}</b>.\nНажми кнопку подтверждения в меню ниже.`;
    }
  
    const kb = getStartKeyboard(userId, hostname, env, inviteData);
    return await sendMessage(chatId, welcome, kb, env);
  }

  // --- 3. ОБЩАЯ ПРОВЕРКА ДОСТУПА (Для всех команд ниже и файлов) ---
  const hasAccess = isAdmin || (userData && (userData.access_token || userData.provider === 'mailru-webdav' || userData.shared_from));
  
  if (!hasAccess) {
    const restrictedMsg = `🚫 <b>Доступ ограничен.</b>\nУ тебя не подключено облако и нет активной ссылки от друга.`;
    return await sendMessage(chatId, restrictedMsg, null, env);
  }

  // --- КОМАНДА /SHARE ---
  if (text === "/share") {
    if (userData.is_ref) {
      return await sendMessage(chatId, "⚠️ Ты используешь чужой диск и не можешь создавать свои реф-ссылки.", null, env);
    }
    
    const inviteToken = Math.random().toString(36).substring(2, 12);
    const inviteData = {
      inviterId: userId,
      provider: userData.provider,
      token: inviteToken,
      timestamp: Date.now()
    };
    
    await env.USER_DB.put(`invite:${inviteToken}`, JSON.stringify(inviteData));
    const botName = env.BOT_USERNAME || "leshiy_storage_bot"; 
    const inviteLink = `https://t.me/${botName}?start=ref_${inviteToken}`;
    return await sendMessage(chatId, `🚀 <b>Твоя ссылка для друга:</b>\n<code>${inviteLink}</code>\n\nДруг подключится к твоему облаку <b>${userData.provider}</b>.`, null, env);
  }

  // --- КОМАНДА /DEBUG ---
  if (text === "/debug") {
    const currentFolder = userData?.folderId || "Не установлена (Root)";
    const debugMsg = `🤖 <b>Бот онлайн</b>\n` +
                     `📦 Версия: ${version}\n` +
                     `🔗 Статус: ✅ Соединение активно\n` +
                     `📁 Папка: <code>${currentFolder}</code>\n` +
                     `👤 Твой ID: <code>${userId}</code>\n` +
                     `${isAdmin ? "👑 Админ: Да" : "👑 Админ: Нет"}`;
    return await sendMessage(chatId, debugMsg, null, env);
  }

  // --- КОМАНДА /DISCONNECT ---
  if (text === "/disconnect") {
    const isShared = !!userData.shared_from;
    const provider = userData.provider;

    await env.USER_DB.delete(userKey);
    
    let dMsg = `🔌 <b>Диск отключен.</b>\nТы больше не подключен к ${provider}.`;
    if (isShared) {
      dMsg = `🔌 <b>Ты отключился от хранилки друга.</b>\nТеперь ты можешь подключить своё собственное облако.`;
    }
    return await sendMessage(chatId, dMsg, null, env);
  }

  // --- КОМАНДА /FOLDER ---
  if (text === "/folder") {
    let folders = [];
    try {
      if (userData.provider === "google") {
        folders = await listGoogleFolders(userData.access_token);
      } else if (userData.provider === "dropbox") {
        folders = await listDropboxFolders(userData.access_token);
      } else if (userData.provider === "yandex") {
        folders = await listYandexFolders(userData.access_token);
      }
    } catch (e) {
      return await sendMessage(chatId, `❌ Ошибка списка папок: ${e.message}`, null, env);
    }

    const buttons = folders.map(f => [
      { text: `📁 ${f.name}`, callback_data: `set_folder:${userId}:${userData.provider === 'google' ? f.id : f.name}` }
    ]);
    
    if (userData.provider === "mailru-webdav") {
      buttons.push([{ text: "✏️ Указать папку вручную", callback_data: `manual_folder:${userId}` }]);
    }
    
    buttons.unshift([{ text: "➕ Создать 'Storage'", callback_data: `create_folder:${userId}:Storage` }]);

    const msgText = `📂 <b>${userData.provider} Drive</b>\nВыбери папку:`;
    return await sendMessage(chatId, msgText, { inline_keyboard: buttons }, env);
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

  // --- ОБРАБОТКА ФАЙЛОВ (Документы, Видео, Фото, Аудио, Голосовые) ---
  const isDoc = !!msg.document;
  const isVideo = !!msg.video;
  const isPhoto = !!msg.photo;
  const isAudio = !!msg.audio;
  const isVoice = !!msg.voice;

  if (isDoc || isVideo || isPhoto || isAudio || isVoice) {
    await sendMessage(chatId, "⏳ <b>Начинаю загрузку в облако...</b>", null, env);
    
    try {
      // Собираем объект файла в зависимости от его типа
      const fileObj = msg.document || 
                      msg.video || 
                      msg.audio || 
                      msg.voice || 
                      (msg.photo ? msg.photo[msg.photo.length - 1] : null);
      
      if (!fileObj) throw new Error("Файл не найден");

      let fileName = "";
      
      // Логика формирования имени
      if (isDoc || isVideo || isAudio) {
        // Берем оригинальное имя файла
        fileName = fileObj.file_name || `file_${Date.now()}`;
      } else if (isVoice) {
        // Для голосовых создаем имя с датой
        const dateStr = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = `Voice_${dateStr}.ogg`;
      } else {
        // Для фото создаем имя с датой
        const dateStr = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = `Photo_${dateStr}.jpg`;
      }

      const { stream } = await getFileStream(fileObj.file_id, env);
      let success = false;

      if (userData.provider === "yandex") {
        success = await uploadToYandex(stream, fileName, userData.access_token, userData.folderId || "");
      } else if (userData.provider === "google") {
        success = await uploadToGoogle(stream, fileName, userData.access_token, userData.folderId);
      } else if (userData.provider === "dropbox") {
        success = await uploadToDropbox(stream, fileName, userData.access_token, userData.folderId || "Storage");
      } else if (userData.provider === "mailru-webdav") {
        const fullPath = `${userData.host}/${userData.folderId ? userData.folderId + '/' : ''}${encodeURIComponent(fileName)}`;
        const arrayBuffer = await new Response(stream).arrayBuffer();
        const res = await fetch(fullPath, {
          method: "PUT",
          headers: {
            "Authorization": `Basic ${btoa(userData.user + ":" + userData.pass)}`,
            "Content-Type": "application/octet-stream"
          },
          body: arrayBuffer
        });
        success = res.status === 201 || res.status === 204;
      }

      if (success) {
        return await sendMessage(chatId, `✅ Файл <b>${fileName}</b> сохранен в ${userData.provider}!`, null, env);
      } else {
        return await sendMessage(chatId, "❌ Ошибка при загрузке. Проверьте токены или место на диске.", null, env);
      }
    } catch (e) {
      return await sendMessage(chatId, `❌ Ошибка: ${e.message}`, null, env);
    }
  }

  return new Response("OK");
}

/**
 * Генерирует клавиатуру для команды /start
 */
function getStartKeyboard(userId, hostname, env, inviteData = null) {
  let keyboard = [];

  // 1-я строка: Яндекс
  const yAuth = `https://oauth.yandex.ru/authorize?response_type=code&client_id=${env.YANDEX_CLIENT_ID}&state=${userId}`;
  keyboard.push([{ text: "🔗 Подключить Яндекс.Диск", url: yAuth }]);

  // 2-я строка: Google
  const gAuth = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${env.GOOGLE_CLIENT_ID}&redirect_uri=https://${hostname}/auth/google/callback&response_type=code&scope=${encodeURIComponent("https://www.googleapis.com/auth/drive.file")}&state=${userId}&access_type=offline&prompt=consent`;
  keyboard.push([{ text: "🔗 Подключить Google Drive", url: gAuth }]);

  // 3-я строка: DropBox
  const dbxAuth = `https://www.dropbox.com/oauth2/authorize?client_id=${env.DROPBOX_CLIENT_ID}&response_type=code&redirect_uri=${encodeURIComponent(`https://${hostname}/auth/dropbox/callback`)}&token_access_type=offline&state=${userId}`;
  keyboard.push([{ text: "🔗 Подключить Dropbox", url: dbxAuth }]);

  // 4-я строка: Mail.Ru
  const mailruClientId = env.MAILRU_CLIENT_ID;
  const mailruRedirectUri = `https://${hostname}/auth/mailru/callback`;
  // Старый вариант: scope=cloud.write.all
  const scope = "cloud";
  const mAuth = `https://connect.mail.ru/oauth/authorize?client_id=${mailruClientId}&response_type=code&scope=${encodeURIComponent(scope)}&redirect_uri=${encodeURIComponent(mailruRedirectUri)}&state=${userId}`;

  keyboard.push([{ 
    text: "🔗 Подключить Облако Mail.ru (WebDAV)", callback_data: "ask_mailru_webdav" }]);
  
  // 5-я строка: Свои FTP/SFTP/WebDAV серверы
  keyboard.push([{ text: "🖥️ Подключить свой FTP/SFTP/WebDAV", callback_data: "ask_custom_server_info" }]);

  // 6-я строка: Условие по рефу
  if (inviteData) {
      keyboard.push([{ 
          text: "🤝 Подтвердить подключение к другу", 
          callback_data: `confirm_ref:${inviteData.token}` 
      }]);
  } else {
      keyboard.push([{ 
          text: "🤝 Подключить Хранилку друга", 
          callback_data: "ask_ref_url" 
      }]);
  }

  return { inline_keyboard: keyboard };
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

    if (action === "manual_folder") {
      await env.USER_DB.put(`state:${userId}`, "wait_manual_folder");
      await sendMessage(chatId, "🔤 Напиши название папки (например: <code>Storage</code>):", null, env);
      return new Response("OK");
    }
    if (action === "create_folder") {
      let finalId;
      let success = false;
      if (userData.provider === "google") {
        finalId = await createGoogleFolder(folderIdOrName, userData.access_token);
      } else if (userData.provider === "yandex") {
        success = await createYandexFolder(folderIdOrName, userData.access_token);
      } else if (userData.provider === "mailru") {
        // Вызываем создание папки для Mail.ru
        success = await createMailruFolder(folderIdOrName, userData.access_token, env);
      } else if (userData.provider === "dropbox") {
        success = await createDropboxFolder(folderIdOrName, userData.access_token);
      } else if (userData.provider === "webdav" || userData.provider === "mailru-webdav") {
        success = await createWebDAVFolder(folderIdOrName, userData);
      }
      if (success) {
        userData.folderId = folderIdOrName;
        await env.USER_DB.put(`user:${targetUserId}`, JSON.stringify(userData));
        await sendMessage(chatId, `✅ Папка <b>${folderIdOrName}</b> создана и выбрана!`, null, env);
      } else {
        await sendMessage(chatId, "❌ Не удалось создать папку. Попробуйте позже.", null, env);
      }

    } else if (action === "set_folder") {
      userData.folderId = folderIdOrName;
      await env.USER_DB.put(`user:${targetUserId}`, JSON.stringify(userData));
      await sendMessage(chatId, `📂 Папка выбрана: <b>${folderIdOrName}</b>`, null, env);
    }
    if (action === "ask_ref_url") {
      // Если рефа нет, просто шлем инструкцию и просим прислать ссылку текстом
      const instruction = `📥 <b>Как подключить хранилку друга:</b>\n\n` +
                          `1. Попроси друга прислать тебе реф-ссылку (он может создать её командой /share).\n` +
                          `2. Либо просто скопируй и <b>пришли мне токен</b> (например: <code>${Math.random().toString(36).substring(2, 10)}</code>) прямо в этот чат.`;
      return await sendMessage(chatId, instruction, null, env);
    }
    if (action === "ask_mailru_webdav") {
      await env.USER_DB.put(`state:${userId}`, "wait_mailru_webdav");
      return await sendMessage(chatId, 
        "📧 <b>Облако Mail.ru через WebDAV</b>\n\n" +
        "1. Перейди в Настройки Облака Mail.ru → «Пароли для внешних приложений»\n" +
        "2. Создай пароль для WebDAV\n" +
        "3. Пришли мне ссылку в формате:\n<code>https://ваша-почта@mail.ru:пароль_для_внешнего_приложения@webdav.cloud.mail.ru</code>\n\n" +
        "<i>Я сразу удалю это сообщение из чата!</i>", 
        null, env
      );
    }
    if (action === "ask_custom_server_info") {
      const customServerGuide = 
        `📁 <b>Подключение своего сервера</b>\n\n` +
        `Поддерживаются следующие протоколы:\n` +
        `✅ <b>WebDAV</b> (рекомендуется) — работает в Cloudflare Workers\n` +
        `❌ <b>FTP / SFTP</b> — НЕ работают в Cloudflare Workers (только в Python-версии)\n\n` +
        `🔗 <b>Формат для WebDAV:</b>\n<code>https://user:pass@ваш-сервер.ru</code>\n\n` +
        `❗ После отправки ссылки я удалю ваше сообщение из чата.\n\n` +
        `📘 <b>Для FTP/SFTP</b> используйте <a href="https://github.com/leshiy-ai/leshiy-storage-bot">Python-версию бота</a> (на Render/VPS).\n` +
        `Это полноценный продукт для личного хостинга.`;
    
      return await sendMessage(chatId, customServerGuide, { 
        inline_keyboard: [[
          { text: "🚀 Отправить WebDAV-ссылку", callback_data: "ask_custom_server" }
        ]] 
      }, env);
    }
    if (action === "ask_custom_server") {
      await env.USER_DB.put(`state:${userId}`, "wait_webdav_url");
      return await sendMessage(chatId, "🌐 <b>Подключение своего сервера</b>\n\nПришли ссылку в формате:\n<code>https://user:pass@webdav.yandex.ru</code>\n\n<i>После получения я удалю твое сообщение из чата в целях безопасности.</i>", null, env);
    }
    
    if (action === "confirm_ref") {
      // parts[1] будет содержать токен инвайта: "confirm_ref:TOKEN"
      const token = parts[1];
      const invite = await env.USER_DB.get(`invite:${token}`, { type: "json" });
    
      if (invite) {
        const ownerData = await env.USER_DB.get(`user:${invite.inviterId}`, { type: "json" });
        if (ownerData) {
          const newUserContext = {
            ...ownerData,
            shared_from: invite.inviterId,
            connected_at: Date.now()
          };
          await env.USER_DB.put(`user:${userId}`, JSON.stringify(newUserContext));
          
          await sendMessage(chatId, `🤝 <b>Связь установлена!</b>\nТеперь ты используешь облако друга (${invite.provider}).`, null, env);
          await logToAdmin(`✅ Юзер <code>${userId}</code> подтвердил подключение к <code>${invite.inviterId}</code>`, env);
          
          // Обновляем меню /start
          return await handleTelegramUpdate({ message: { chat: { id: chatId }, from: { id: userId }, text: "/start" } }, env, "hostname_placeholder");
        }
      } else {
        return await sendMessage(chatId, "❌ Ссылка просрочена или неверна.", null, env);
      }
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

// ✅ logToAdmin - Сообщения только АДМИНУ в общем чате
async function logToAdmin(text, env) {
  try {
    const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
    if (adminCfg && adminCfg.id) {
      // Используем parse_mode HTML для красоты (ID в code)
      await sendMessage(adminCfg.id, `🔔 <b>ADMIN LOG:</b>\n${text}`, null, env);
    }
  } catch (e) {
    console.error("Ошибка логирования админу:", e.message);
  }
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

// Работа с Облаком Mail.Ru
async function handleMailruCallback(request, env) {
  const url = new URL(request.url);
  const code = url.searchParams.get("code");
  const userId = url.searchParams.get("state");

  if (!code) return new Response("❌ Ошибка: code не получен");
  const clientId = env.MAILRU_CLIENT_ID.trim();
  const clientSecret = env.MAILRU_CLIENT_SECRET.trim();
  const redirectUri = `https://${url.hostname}/auth/mailru/callback`;

  // Формируем параметры строго по спецификации для Внешних приложений
  const bodyParams = new URLSearchParams();
  bodyParams.append('grant_type', 'authorization_code');
  bodyParams.append('code', code);
  bodyParams.append('redirect_uri', redirectUri);
  bodyParams.append('client_id', clientId);
  bodyParams.append('client_secret', clientSecret);

  try {
    // Пробуем connect.mail.ru, так как статус теперь "Внешнее"
    const res = await fetch("https://connect.mail.ru/oauth/token", {
      method: "POST",
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
      },
      body: bodyParams.toString()
    });

    const data = await res.json();

    if (data.access_token) {
      await env.USER_DB.put(`user:${userId}`, JSON.stringify({
        provider: "mailru",
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        expires_at: Date.now() + (data.expires_in * 1000)
      }));

      await sendMessage(userId, "✅ <b>Облако Mail.ru (Внешнее) подключено!</b>", null, env);
      return new Response("✅ Успешно! Можете вернуться в Telegram.");
    }

    // Если всё еще CLIENT_SECRET_FAIL, выводим детали для отладки
    return new Response(`❌ Ошибка обмена: ${JSON.stringify(data)}`);
  } catch (e) {
    return new Response(`❌ Ошибка сети: ${e.message}`);
  }
}

async function uploadToMailru(stream, fileName, accessToken, folderPath, env) {
  try {
    // 1. Преобразуем поток в ArrayBuffer
    const arrayBuffer = await new Response(stream).arrayBuffer();
    const fileBuffer = new Uint8Array(arrayBuffer);

    // 2. Получаем shard URL
    const dispRes = await fetch(`https://cloud.mail.ru/api/v2/dispatcher?access_token=${accessToken}`);
    const dispData = await dispRes.json();

    if (!dispData.body?.upload?.[0]?.url) {
      await logToAdmin(`❌ Mail.ru: не удалось получить shard: ${JSON.stringify(dispData)}`, env);
      return false;
    }
    const uploadUrl = dispData.body.upload[0].url;

    // 3. Загружаем файл на shard
    const uploadRes = await fetch(`${uploadUrl}?cloud_domain=2&login=me`, {
      method: "POST",
      body: fileBuffer
    });

    const fileHash = await uploadRes.text();
    if (!fileHash.trim()) {
      await logToAdmin(`❌ Mail.ru: пустой hash после загрузки на shard`, env);
      return false;
    }

    // 4. Регистрируем файл в облаке
    const fullPath = `/${folderPath}/${fileName}`.replace(/\/+/g, '/');
    const params = new URLSearchParams({
      home: fullPath,
      hash: fileHash.trim(),
      size: fileBuffer.byteLength.toString(),
      conflict: "rename",
      api: "2"
    });

    const addRes = await fetch(`https://cloud.mail.ru/api/v2/file/add?access_token=${accessToken}`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });

    const addData = await addRes.json();
    await logToAdmin(`📤 Mail.ru upload result: ${JSON.stringify(addData)}`, env);

    return addData.status === 200;
  } catch (e) {
    await logToAdmin(`🔥 Mail.ru upload error: ${e.message}\nStack: ${e.stack}`, env);
    return false;
  }
}

async function createMailruFolder(folderName, accessToken, env) {
  try {
    const params = new URLSearchParams({
      home: `/${folderName}`,
      api: "2"
    });

    const res = await fetch(`https://cloud.mail.ru/api/v2/folder/add?access_token=${accessToken}`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params.toString()
    });

    const data = await res.json();
    // Логируем результат, чтобы понять, пробита ли 403-я
    await logToAdmin(`📁 Mailru Folder Create (${folderName}): ${JSON.stringify(data)}`, env);
    
    return data.status === 200 || data.status === 409; // 409 значит папка уже есть
  } catch (e) {
    await logToAdmin(`❌ Folder Create Error: ${e.message}`, env);
    return false;
  }
}

// Работа с WebDAV - создание папки
async function createWebDAVFolder(folderName, userData) {
  const url = `${userData.host}/${encodeURIComponent(folderName)}/`; // ← Обязательно с /
  const auth = btoa(`${userData.user}:${userData.pass}`);

  const res = await fetch(url, {
    method: "MKCOL", // ← Ключевое изменение!
    headers: {
      "Authorization": `Basic ${auth}`
    }
  });

  // 201 — создано, 405 — уже существует (иногда Mail.ru возвращает 405)
  return res.status === 201 || res.status === 405;
}

// Работа с DropBox
async function handleDropboxCallback(request, env) {
  const url = new URL(request.url);
  const code = url.searchParams.get("code");
  const userId = url.searchParams.get("state"); // Не забудь прокинуть state в ссылку выше

  const res = await fetch("https://api.dropboxapi.com/oauth2/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      code,
      grant_type: "authorization_code",
      client_id: env.DROPBOX_CLIENT_ID,
      client_secret: env.DROPBOX_CLIENT_SECRET,
      redirect_uri: `https://${url.hostname}/auth/dropbox/callback`
    })
  });

  const data = await res.json();
  if (data.access_token) {
    await env.USER_DB.put(`user:${userId}`, JSON.stringify({
      provider: "dropbox",
      access_token: data.access_token,
      refresh_token: data.account_id // У Dropbox свои нюансы с рефрешем, для начала хватит этого
    }));
    await sendMessage(userId, "🎉 <b>Dropbox успешно подключен!</b>", null, env);
    return new Response("OK! Go back to Telegram.");
  }
  return new Response("Error", { status: 400 });
}

async function uploadToDropbox(stream, fileName, accessToken, folderPath = "") {
  const path = `/${folderPath}/${fileName}`.replace(/\/+/g, '/');
  const arg = {
    path: path,
    mode: "add",
    autorename: true,
    mute: false
  };

  const res = await fetch("https://content.dropboxapi.com/2/files/upload", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${accessToken}`,
      "Dropbox-API-Arg": JSON.stringify(arg),
      "Content-Type": "application/octet-stream"
    },
    body: stream // Dropbox отлично принимает поток напрямую!
  });

  return res.ok;
}

async function listDropboxFolders(token) {
  try {
    const res = await fetch("https://api.dropboxapi.com/2/files/list_folder", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        path: "", // Пустое значение для корня Full Dropbox
        recursive: false,
        include_media_info: false,
        include_deleted: false
      })
    });

    if (!res.ok) {
      const errorData = await res.json();
      console.error("Dropbox list error:", errorData);
      return [];
    }

    const data = await res.json();
    // Фильтруем только записи с тегом "folder"
    return (data.entries || [])
      .filter(item => item[".tag"] === "folder")
      .map(item => ({ 
        id: item.name, // Для Dropbox используем имя как ID
        name: item.name 
      }));
  } catch (e) {
    console.error("Dropbox fetch error:", e);
    return [];
  }
}

async function createDropboxFolder(folderName, token) {
  try {
    const res = await fetch("https://api.dropboxapi.com/2/files/create_folder_v2", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        path: `/${folderName}`,
        autorename: false
      })
    });
    
    const data = await res.json();
    // Если папка уже есть, Dropbox вернет ошибку, проверим это
    return res.status === 200 || data?.error_summary?.includes("path_already_exists");
  } catch (e) {
    return false;
  }
}
