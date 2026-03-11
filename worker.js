/* 🗄 Telegram Storage Bot "Хранилка" by Leshiy
Telegram-бот для автоматической загрузки фото и видео в облачное хранилище с реферальной системой доступа. 

✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Google, Яндекс.Диск, Облако Mail.Ru WebDAV и др.) 
прямо через телеграмм. Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку.
Универсальность: Поддержка облачного WebDAV (Google, Яндекс.Диск, Облако Mail.Ru и др.).
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео.
Поддержка WEBM: Возможность сохранять видеофайлы в современных форматах без потери качества.
Диагностика: Команда /debug для проверки статуса подключения к хранилищу в реальном времени.
*/
// Глобальные константы
const version = "v2.1.5"; // актуальная версия

// ----------------------------------------------------
// ГЛАВНЫЙ ОБРАБОТЧИК (WEBHOOK) Fetch
// ----------------------------------------------------
export default {
  async fetch(request, env, ctx) {
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
          await handleCallbackQuery(update.callback_query, env, ctx);
          return new Response("OK"); // ← один раз, в fetch
        }
    
        // Потом проверяем сообщения и файлы
        if (update.message || update.edited_message) {
          return await handleTelegramUpdate(update, env, hostname, ctx);
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

async function handleTelegramUpdate(update, env, hostname, ctx) {
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
      await logDebug(report, env);
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

  // - КОМАНДА /SEARCH - 
  if (text.startsWith("/search")) {
    const query = text.replace(/^\/search\s*/i, '').trim();
    if (!query) {
      return await sendMessage(chatId, "🔎 Напиши, что искать: /search вечеринка 15 декабря", null, env);
    }

    const searchResult = await searchFilesByQuery(userId, query, env);

    if (!searchResult.success) {
      return await sendMessage(chatId, `❌ Ошибка поиска: ${searchResult.message}`, null, env);
    }

    if (searchResult.fileIds.length === 0) {
      return await sendMessage(chatId, "❌ Ничего не найдено.", null, env);
    }

    // Сохраняем результат поиска в KV (для пагинации)
    const searchKey = `search:${userId}:${Date.now()}`;
    await env.USER_DB.put(searchKey, JSON.stringify({
      query,
      fileIds: searchResult.fileIds,
      timestamp: Date.now()
    }), { expirationTtl: 60 * 10 }); // 10 минут

    // Отправляем первые 5 файлов
    const firstFive = searchResult.fileIds.slice(0, 5);
    let fileList = "";
    for (const fileId of firstFive) {
      const fileRow = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(fileId).first();
      if (fileRow) {
        fileList += `📄 ${fileRow.fileName}\n`;
      }
    }

    return await sendMessage(chatId, 
      `🔍 Найдено ${searchResult.fileIds.length} файлов:\n\n${fileList}\n\n👉 Для просмотра всех результатов — нажми «Показать ещё»`,
      { inline_keyboard: [[{ text: "➡️ Показать ещё", callback_data: `show_more_search:${searchKey}` }]] },
      env
    );
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

    const adminKeyboard = {
      inline_keyboard: [
        [{ text: "🧠 Настройки ИИ", callback_data: "open_ai_settings" }]
  ]
};

return await sendMessage(chatId, adminMsg, adminKeyboard, env);
  }

  if (text === "/ai_settings" && isAdmin) {
    const serviceType = "TEXT_TO_TEXT";
    const service = AI_MODEL_MENU_CONFIG[serviceType];
    if (!service) return await sendMessage(chatId, "❌ Настройки ИИ недоступны.", null, env);
  
    const currentModelKey = await env.USER_DB.get(service.kvKey) || Object.keys(service.models)[0];
    const buttons = Object.entries(service.models).map(([key, name]) => [
      { text: (key === currentModelKey ? "✅ " : "") + name, callback_data: `set_ai_model:${serviceType}:${key}` }
    ]);
  
    return await sendMessage(chatId, `🧠 Выберите модель для семантического поиска:`, { inline_keyboard: buttons }, env);
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
      const fileResponse = await getFileStream(fileObj.file_id, env);
      const arrayBuffer = await new Response(fileResponse.stream).arrayBuffer();
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
        // Сразу отвечаем
        const response = await sendMessage(chatId, `✅ Файл <b>${fileName}</b> сохранен!`, null, env);
        // Определяем тип для базы
        const fType = isPhoto ? "photo" : isVideo ? "video" : isAudio ? "audio" : isVoice ? "voice" : "document";
        // А запись — в фоне
        ctx.waitUntil(
          (async () => {
            try {
              await env.FILES_DB.prepare(
                "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
              ).bind(
                String(userId), 
                fileName, 
                fileObj.file_id, 
                fType, 
                userData.provider, 
                userData.folderId || "Root", 
                Date.now()
              ).run();
            } catch (e) {
              await logDebug(`⚠️ D1 write error: ${e.message}`, env);
            }
          })()
          );
        // Фоновая генерация описания через Gemini Vision
        ctx.waitUntil(
          (async () => {
            try {
              // Только для фото/видео, где можно применить Vision
              // Фоновая генерация описания (если файл — фото или видео)
              if (fType === "photo" || fType === "video") {
                ctx.waitUntil(
                  (async () => {
                    try {
                      // Читаем поток в ArrayBuffer
                      const arrayBuffer = await new Response(stream).arrayBuffer();

                      // Генерируем описание
                      const description = await callGeminiVision(
                        AI_MODELS.IMAGE_TO_TEXT_GEMINI,
                        arrayBuffer,
                        env
                      );

                      // Обновляем запись в D1
                      const updateSql = `
                        UPDATE files SET ai_description = ? WHERE userId = ? AND fileName = ?
                      `;
                      await env.FILES_DB.prepare(updateSql)
                        .bind(description, String(userId), fileName)
                        .run();

                      console.log(`✅ Description generated for ${fileName}`);

                    } catch (e) {
                      console.error("AI description error:", e);
                      await logDebug(`⚠️ Ошибка генерации описания: ${e.message}`, env);
                    }
                  })()
                );
              }
            } catch (e) {
              console.error("AI description error:", e);
              await logDebug(`⚠️ Ошибка генерации описания: ${e.message}`, env);
            }
          })()
        );
        return response;
      } else {
        return await sendMessage(chatId, "❌ Ошибка при загрузке. Проверьте токены или место на диске.", null, env);
      }
    } catch (e) {
      return await sendMessage(chatId, `❌ Ошибка: ${e.message}`, null, env);
    }
  }

  if (text.trim() && !text.startsWith("/") && userData) {
    // Пользователь подключён → отвечаем через ИИ
    try {
      const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
      await logDebug(`💬 Запрос к ИИ: "${text.substring(0, 50)}..." через <code>${modelConfig.MODEL}</code>`, env);
  
      const responseText = await modelConfig.FUNCTION(text, modelConfig, env);
      const safeText = responseText.substring(0, 4000);
      await sendMessage(chatId, safeText, null, env);
    } catch (e) {
      await logDebug(`❌ Ошибка ИИ: ${e.message}`, env);
      await sendMessage(chatId, `❌ ИИ не отвечает. Подробности в /debug.`, null, env);
    }
    return new Response("OK");
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

async function handleCallbackQuery(query, env, ctx) {
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

    if (action === "show_more_search") {
      const searchKey = parts[1];
      const searchState = await env.USER_DB.get(searchKey, { type: "json" });
      if (!searchState) {
        return await sendMessage(chatId, "❌ Результат поиска устарел или не найден.", null, env);
      }
    
      const remainingIds = searchState.fileIds.slice(5); // Пропускаем первые 5
      if (remainingIds.length === 0) {
        return await sendMessage(chatId, "✅ Все файлы уже показаны.", null, env);
      }
    
      let fileList = "";
      for (const fileId of remainingIds) {
        const fileRow = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(fileId).first();
        if (fileRow) {
          fileList += `📄 ${fileRow.fileName}\n`;
        }
      }
      return await sendMessage(chatId, `✅ Остальные файлы:\n\n${fileList}`, null, env);
    }

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

    if (action === "set_ai_model") {
      const serviceType = parts[1];
      const modelKey = parts[2];
    
      // Проверяем, что тип сервиса и модель существуют
      if (!SERVICE_TYPE_MAP[serviceType] || !AI_MODELS[modelKey]) {
        await sendMessage(chatId, "❌ Неверный тип или модель.", null, env);
        return new Response("OK");
      }
    
      // Сохраняем выбранную модель в KV
      const kvKey = SERVICE_TYPE_MAP[serviceType].kvKey;
      await env.USER_DB.put(kvKey, modelKey);
      await logDebug(`⚙️ Установлена модель: <code>${kvKey}</code> = <code>${modelKey}</code>`, env);
    
      // Подтверждаем пользователю
      await sendMessage(chatId, `✅ Модель для ${serviceType} успешно изменена на: ${modelKey}`, null, env);
    
      // Обновляем меню
      const updatedService = AI_MODEL_MENU_CONFIG[serviceType];
      const currentModelKey = modelKey;
      const buttons = Object.entries(updatedService.models).map(([key, name]) => [
        {
          text: (key === currentModelKey ? "✅ " : "") + name,
          callback_data: `set_ai_model:${serviceType}:${key}`
        }
      ]);
    
      // Отправляем обновлённое меню
      await sendMessage(chatId, `🧠 Выберите модель для ${serviceType}:`, { inline_keyboard: buttons }, env);
    }

    if (action === "open_ai_settings") {
      // Повтори логику из /ai_settings
      const serviceType = "TEXT_TO_TEXT";
      const service = AI_MODEL_MENU_CONFIG[serviceType];
      if (!service) return await sendMessage(chatId, "❌ Настройки ИИ недоступны.", null, env);
    
      const currentModelKey = await env.USER_DB.get(service.kvKey) || Object.keys(service.models)[0];
      const buttons = Object.entries(service.models).map(([key, name]) => [
        { text: (key === currentModelKey ? "✅ " : "") + name, callback_data: `set_ai_model:${serviceType}:${key}` }
      ]);
    
      return await sendMessage(chatId, `🧠 Выберите модель для семантического поиска:`, { inline_keyboard: buttons }, env);
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
          await logDebug(`✅ Юзер <code>${userId}</code> подтвердил подключение к <code>${invite.inviterId}</code>`, env);
          
          // Обновляем меню /start
          return await handleTelegramUpdate({ message: { chat: { id: chatId }, from: { id: userId }, text: "/start" } }, env, "hostname_placeholder", ctx);
        }
      } else {
        return await sendMessage(chatId, "❌ Ссылка просрочена или неверна.", null, env);
      }
    }
  } catch (e) {
    await sendMessage(chatId, `❌ Ошибка: ${e.message}`, null, env);
  }
  return;
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

// ✅ logDebug - Сообщения только АДМИНУ в общем чате
async function logDebug(text, env) {
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
      await logDebug(`❌ Mail.ru: не удалось получить shard: ${JSON.stringify(dispData)}`, env);
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
      await logDebug(`❌ Mail.ru: пустой hash после загрузки на shard`, env);
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
    await logDebug(`📤 Mail.ru upload result: ${JSON.stringify(addData)}`, env);

    return addData.status === 200;
  } catch (e) {
    await logDebug(`🔥 Mail.ru upload error: ${e.message}\nStack: ${e.stack}`, env);
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
    await logDebug(`📁 Mailru Folder Create (${folderName}): ${JSON.stringify(data)}`, env);
    
    return data.status === 200 || data.status === 409; // 409 значит папка уже есть
  } catch (e) {
    await logDebug(`❌ Folder Create Error: ${e.message}`, env);
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

async function searchFilesByQuery(userId, query, env) {
  try {
    // Получаем файлы
    const filesResult = await env.FILES_DB.prepare(
      "SELECT id, fileName, ai_description FROM files WHERE userId = ? ORDER BY timestamp DESC LIMIT 100"
    ).bind(String(userId)).all();

    if (!filesResult.success || filesResult.results.length === 0) {
      return { success: false, message: "Нет файлов для поиска." };
    }

    const filesList = filesResult.results.map(f => 
      `${f.id}. ${f.fileName} — ${f.ai_description || 'Без описания'}`
    ).join("\n");

    // Выбираем модель через KV
    const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
    
    // Формируем промт
    const prompt = `
Ты — умный ассистант по поиску файлов. Пользователь ищет: "${query}"
Вот список его файлов (ID. Имя — Описание):
${filesList}

Верни ТОЛЬКО номера подходящих файлов через запятую, например: 1,3,7
Не пиши ничего кроме чисел и запятых.
`;

    // Вызываем ИИ
    const responseText = await modelConfig.FUNCTION(prompt, modelConfig, env);
    console.log("Gemini response:", responseText);

    // Парсим
    const relevantIds = responseText
      .split(/[,，\s]+/)
      .map(id => parseInt(id.trim()))
      .filter(id => !isNaN(id) && id > 0);

    return relevantIds.length > 0
      ? { success: true, fileIds: relevantIds }
      : { success: true, fileIds: [], message: "Ничего не найдено." };

  } catch (e) {
    console.error("Search error:", e);
    return { success: false, message: "Ошибка ИИ: " + (e.message || "неизвестно") };
  }
}

/**
 * Преобразует ArrayBuffer в Base64 строку.
 * @param {ArrayBuffer} buffer - Бинарные данные.
 * @returns {string} Base64 строка.
 */
function arrayBufferToBase64(buffer) {
  let binary = '';
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

// ✅ *** Gemini Chat API (для текстового общения) ***
/**
 * Вызывает модель Gemini через Google Generative Language API, используя унифицированную конфигурацию.
 * @param {string} prompt - Текстовый промт.
 * @param {Object} config - Объект активной конфигурации (AI_MODELS.TEXT_TO_TEXT_GEMINI).
 * @param {Object} env - Объект окружения Cloudflare Worker, содержащий ключ.
 * @param {string} userMessageText - Текущее сообщение пользователя. 
 * @returns {Promise<string>} Сгенерированный текстовый ответ.
 */
async function callGeminiChat(prompt, config, env, userMessageText) {
    
  // --- ДИНАМИЧЕСКИЕ ПАРАМЕТРЫ ИЗ КОНФИГУРАЦИИ ---
  const API_KEY_ENV_NAME = config.API_KEY; 
  const API_KEY = env[API_KEY_ENV_NAME]; 
  const BASE_URL = config.BASE_URL; 
  const MODEL = config.MODEL; 
  
  // --- УНИФИЦИРОВАННАЯ СБОРКА URL ---
  // Формат: BASE_URL/models/МОДЕЛЬ:generateContent?key=КЛЮЧ
  const url = `${BASE_URL}/models/${MODEL}:generateContent?key=${API_KEY}`;
  // ------------------------------------

  if (!API_KEY) {
      throw new Error(`GemINI API key is missing. Expected env var: ${API_KEY_ENV_NAME}`);
  }

  // 2. СИСТЕМНАЯ ИНСТРУКЦИЯ 
  const systemInstructionText = `
  🤖 ТЫ — многофункциональный AI-ассистент "Gemini AI" от Leshiy, отвечающий на русском языке.
  Твоя задача — вести диалог, отвечать на вопросы, соблюдая контекст и используя информацию о твоих функциях.
  
  Твои ключевые функции:
  ✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Google, Яндекс.Диск, Облако Mail.Ru WebDAV и др.) прямо через телеграмм. 
  Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку (команда /share) формирует ссылку с токеном.
  Универсальность: Поддержка облачного хранилища с авторизацией OAuth (Google, Яндекс.Диск, DropBox) и WebDAV (Облако Mail.Ru и др.)
  Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео/аудио/документов.
  💬 Чат: Ты ведешь диалог, отвечаешь на вопросы, ❔ помогаешь по менюшкам и окнам и сохраняешь контекст беседы.
  
  Когда пользователь спрашивает, что ты умеешь, обязательно упомяни о своих навыках.
  Ответы должны быть информативными и доброжелательными и по возможности компактными, старайся построить диалог понятно и не сильно рассуждая.
`;
  // 3. ТЕЛО ЗАПРОСА (ВОССТАНОВЛЕННАЯ РАБОЧАЯ ЛОГИКА)
  const body = {
      systemInstruction: {
          parts: [{ text: systemInstructionText }]
      },
      contents: [{ role: "user", parts: [{ text: prompt }] }]
  };

  const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
  });

  if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Gemini Chat API Error: ${response.status} - ${errorText.substring(0, 150)}...`);
  }

  const data = await response.json();
  const textResult = data?.candidates?.[0]?.content?.parts?.[0]?.text;

  if (!textResult) { throw new Error(`Gemini Chat не вернул ответ. Причина: ${JSON.stringify(data.promptFeedback)}`); }

  return textResult.trim();
}

// ✅ *** Gemini Vision (компьютерное зрение) - ИСПРАВЛЕНО ***
/**
 * Генерирует описание файла через Gemini Vision.
 * @param {Object} config - Конфигурация модели (из AI_MODELS).
 * @param {ArrayBuffer} imageBuffer - Буфер изображения.
 * @param {Object} env - Окружение (с API ключом).
 * @returns {Promise<string>} Описание файла.
 */
async function callGeminiVision(config, imageBuffer, env) {
  const API_KEY_ENV_NAME = config.API_KEY;
  const API_KEY = env[API_KEY_ENV_NAME];
  const BASE_URL = config.BASE_URL;
  const MODEL = config.MODEL;

  if (!API_KEY) {
    throw new Error(`Gemini API key is missing. Expected env var: ${API_KEY_ENV_NAME}`);
  }

  const imageBase64 = arrayBufferToBase64(imageBuffer);

  const systemInstructionText = "РОЛЬ И ЯЗЫК: Действуй как 'Фотореставратор'. Общение СТРОГО на РУССКОМ языке. ЦЕЛЬ: Создать максимально детализированный, буквальный промпт для Image-to-Image генерации. Твой ответ должен быть только промптом, без приветствий и объяснений.";

  const body = {
    systemInstruction: { parts: [{ text: systemInstructionText }] },
    contents: [{
      parts: [
        { text: "На основе присланного изображения, сгенерируй ОЧЕНЬ ПОДРОБНЫЙ, но не более 750 символов, точный и буквальный промпт на РУССКОМ языке для нейросети для генерации изображения. ТОЧНО ВОСПРОИЗВЕДИ сцену, но в высоком разрешении и цвете. Используй слово 'ребенок' вместо 'малыш' или 'младенец'. НЕ УПОМИНАЙ 'пустышка', если это возможно, или замени на нейтральный термин вроде 'аксессуар для рта'. Сохрани СТРОГО ту же КОМПОЗИЦИЮ и ракурс. Используй художественный стиль 'фотореалистичная иллюстрация' или 'картина' вместо 'фотография'. Добавь в конец промпта суффиксы для качества: 'высокая детализация, шедевр, студийное освещение'." },
        { inlineData: { mimeType: 'image/jpeg', data: imageBase64 } }
      ]
    }],
  };

  const url = `${BASE_URL}/models/${MODEL}:generateContent?key=${API_KEY}`;
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });

  const data = await response.json();
  if (data.error) {
    throw new Error(`Gemini API Error: ${data.error.message}`);
  }

  const textResult = data?.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!textResult) {
    throw new Error("Gemini не вернул промпт.");
  }

  return textResult.trim();
}

// ✅ *** Gemini Video Vision (видео аналитика) - ИСПРАВЛЕНО ***
/**
* Выполняет анализ видеоконтента (Video Captioning) с помощью Gemini 2.5 Flash.
* @param {Object} config - Объект активной конфигурации (AI_MODELS.VIDEO_TO_ANALYSIS_GEMINI).
* @param {ArrayBuffer} videoBuffer - Буфер видеофайла.
* @param {string} mimeType - MIME-тип видео (напр., 'video/mp4').
* @param {Object} env - Объект окружения, содержащий ключ.
* @returns {Promise<string>} Сгенерированный текстовый анализ.
*/
async function callGeminiVideoVision(config, videoBuffer, mimeType, env) { 
  
  // --- ДИНАМИЧЕСКИЕ ПАРАМЕТРЫ ИЗ КОНФИГУРАЦИИ ---
  const API_KEY_ENV_NAME = config.API_KEY; 
  const API_KEY = env[API_KEY_ENV_NAME]; 
  const BASE_URL = config.BASE_URL; 
  const MODEL = config.MODEL; 
  
  const url = `${BASE_URL}/models/${MODEL}:generateContent`; 
  // ------------------------------------

  if (!API_KEY) {
      throw new Error(`Gemini API key is missing. Expected env var: ${API_KEY_ENV_NAME}`);
  }
  
  // ТРЕБУЕТСЯ КОНВЕРТАЦИЯ: Base64 для Gemini.
  // P.S. Убедитесь, что arrayBufferToBase64 доступна
  const videoBase64 = arrayBufferToBase64(videoBuffer); 

  const systemInstructionText = "РОЛЬ: Действуй как 'Видеоаналитик'. Общение СТРОГО на РУССКОМ языке. ЦЕЛЬ: Предоставить подробный и точный анализ видеоконтента, включая ключевые действия, объекты и события. Твой ответ должен быть только анализом, без приветствий и объяснений.";
  
  const promptText = "Проанализируй видеоролик покадрово и аудиодорожку. Предоставь полное описание происходящего, включая распознавание действий, ключевых объектов и хронометраж. Отдельно опиши содержание аудиодорожки, включая точную транскрипцию и возможный контекст (цитаты, источники). Ответь только текстом анализа, используя четкую структуру";

  const body = {
      systemInstruction: { parts: [{ text: systemInstructionText }] }, 
      contents: [{
          parts: [
              { text: promptText },
              { inlineData: { mimeType: mimeType, data: videoBase64 } } // <-- Используем mimeType видео
          ]
      }],
  };
  
  const response = await fetch(`${url}?key=${API_KEY}`, { 
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
  });
  
  const data = await response.json();
  if (data.error) { throw new Error(`Gemini API Error: ${data.error.message}`); }
  const textResult = data?.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!textResult) { throw new Error("Gemini не вернул результат анализа видео."); }
  return textResult.trim();
}

// ✅ *** Workers AI Chat API (для текстового общения с историей) ***
async function callWorkersAIChat(prompt, config, env, userMessageText) {
    const { AI } = env;
    if (!AI) {
        throw new Error("Workers AI binding 'AI' не настроен. Проверьте Cloudflare Dashboard.");
    }

    const MODEL_NAME = config.MODEL;
    // --- УДАЛЯЕМ ЛИШНИЕ ПЕРЕМЕННЫЕ ИЗ-ЗА НОВОЙ ЛОГИКИ ПЕРЕХВАТА ---

    // 1. ОПРЕДЕЛЕНИЕ СИСТЕМНОГО КОНТЕКСТА
    // УДАЛЯЕМ ЛОГИКУ, КОТОРАЯ СТИМУЛИРУЕТ ТЕГИ <think>
    const systemPromptText = `🤖 ТЫ — многофункциональный AI-ассистент "Gemini AI" от Leshiy, отвечающий на русском языке.
Твоя задача — вести диалог, отвечать на вопросы, соблюдая контекст и используя информацию о твоих функциях.

СТРОГОЕ ПРАВИЛО: НИКОГДА НЕ УПОМИНАЙ LLaMA, Meta AI или Austin.

Твои ключевые функции:
✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Google, Яндекс.Диск, Облако Mail.Ru WebDAV и др.) прямо через телеграмм. 
Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку (команда /share) формирует ссылку с токеном.
Универсальность: Поддержка облачного хранилища с авторизацией OAuth (Google, Яндекс.Диск, DropBox) и WebDAV (Облако Mail.Ru и др.)
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео/аудио/документов.
💬 Чат: Ты ведешь диалог, отвечаешь на вопросы, ❔ помогаешь по менюшкам и окнам и сохраняешь контекст беседы.

Когда пользователь спрашивает, что ты умеешь, обязательно упомяни о своих навыках.
Ответы должны быть информативными и доброжелательными и по возможности компактными, старайся построить диалог понятно и не сильно рассуждая.
`.trim();

    // 2. ФОРМИРОВАНИЕ ИСТОРИИ (messages) (Оставляем как есть, но используем 'system' для основного промпта)

    // Инициализация массива с СИСТЕМНЫМ КОНТЕКСТОМ.
    // Используем роль 'system' если модель её поддерживает (Qwen должна),  иначе оставим 'user'.
    const messages = [
        { role: 'system', content: systemPromptText },
    ];

    try {
        // *** ДОБАВЛЯЕМ ЛИМИТ ТОКЕНОВ И ТЕМПЕРАТУРУ ***
        const response = await AI.run(MODEL_NAME, { 
            messages: messages,
            stream: false, // Отключаем стриминг, чтобы избежать обрезки
            max_tokens: 1024, // Увеличиваем лимит токенов для безопасности
            temperature: 0.7 // Умеренная температура
        });

        if (!response || !response.response) {
            throw new Error(`Workers AI не вернул ожидаемый ответ. Response: ${JSON.stringify(response)}`);
        }

        return response.response.trim(); // Возвращаем сырой, но полный ответ
    } catch (e) {
        console.error("Workers AI call failed:", e);
        throw new Error(`Ошибка Workers AI: ${e.message}`);
    }
}

// ✅ *** Workers AI Vision (Uform-Gen2 для генерации промпта из фото) - УНИФИЦИРОВАНО ***
/**
 * Генерирует детальный промпт для Stable Diffusion, используя изображение и текстовую инструкцию, через Workers AI (Uform).
 * @param {Object} config - Объект активной конфигурации (AI_MODELS.IMAGE_TO_TEXT_WORKERS_AI).
 * @param {ArrayBuffer} imageBuffer - Буфер изображения.
 * @param {Object} env - Объект окружения, содержащий привязку AI.
 * @returns {Promise<string>} Сгенерированный текстовый промпт.
 */
async function callWorkersAIVision(config, imageBuffer, env) { // <-- ИЗМЕНЕНА ПОДПИСЬ
  const { AI } = env;
  // --- УНИФИКАЦИЯ: Используем модель из конфигурации ---
  const VISION_MODEL = config.MODEL; 
  // ---------------------------------------------------

  if (!AI) {
      throw new Error("Workers AI binding 'AI' не настроен.");
  }

  // Здесь audioBuffer стал вторым аргументом, а promptText - третьим.
  const imageBytes = [...new Uint8Array(imageBuffer)];

  // Uform-Gen2 требует простого промпта. Мы используем эффективную инструкцию на английском.
  const simplifiedPrompt = `Describe the attached image in full detail as a high-quality, atmospheric, long prompt (max 750 characters) for an image generation AI like Stable Diffusion or Midjourney. Focus on subject, style, lighting, and composition. The response must be ONLY in RUSSIAN, without any added commentary.`;

  try {
      const aiResponse = await AI.run(
          VISION_MODEL,
          {
              prompt: simplifiedPrompt,
              image: imageBytes
          }
      );

      if (!aiResponse || !aiResponse.description) { // <-- Uform возвращает 'description'
          throw new Error(`Vision API не вернул ожидаемый ответ. Response: ${JSON.stringify(aiResponse)}`);
      }

      return aiResponse.description.trim();
  } catch (e) {
      console.error("Workers AI Vision call failed:", e);
      throw new Error(`VISION_FAIL: Ошибка Workers AI Vision: ${e.message}`);
  }
}

// ✅ *** callBotHubTextChat - Обработчик для текстовых чат-запросов BotHub
/**
 * @description Отправляет запрос на генерацию текста через BotHub API.
 * @param {string} prompt - Текстовый промт.
 * @param {Object} config - Объект конфигурации модели (TEXT_TO_TEXT_BOTHUB).
 * @param {string} messageText - Новое сообщение от пользователя.
 * @param {Object} env - Объект окружения (включает DEBUG_ENABLED и ctx).
 * @returns {Promise<string>} Сгенерированный текстовый ответ.
 */
async function callBotHubTextChat(prompt, config, env, messageText) {
  // 1. ОПРЕДЕЛЕНИЕ СИСТЕМНОГО КОНТЕКСТА (ГЛОБАЛЬНАЯ КОНСТАНТА)
  const SYSTEM_PROMPT = `
🤖 ТЫ — многофункциональный AI-ассистент "Gemini AI" от Leshiy, отвечающий на русском языке.
Твоя задача — вести диалог, отвечать на вопросы, соблюдая контекст и используя информацию о твоих функциях.

Твои ключевые функции:
✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Google, Яндекс.Диск, Облако Mail.Ru WebDAV и др.) прямо через телеграмм. 
Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку (команда /share) формирует ссылку с токеном.
Универсальность: Поддержка облачного хранилища с авторизацией OAuth (Google, Яндекс.Диск, DropBox) и WebDAV (Облако Mail.Ru и др.)
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео/аудио/документов.
💬 Чат: Ты ведешь диалог, отвечаешь на вопросы, ❔ помогаешь по менюшкам и окнам и сохраняешь контекст беседы.
  
Когда пользователь спрашивает, что ты умеешь, обязательно упомяни о своих навыках.
Ответы должны быть информативными и доброжелательными и по возможности компактными, старайся построить диалог понятно и не сильно рассуждая.
`.trim();
  
  const apiKey = env[config.API_KEY];
  const baseUrl = config.BASE_URL;
  const model = config.MODEL;
  
  // ПРОВЕРКА КЛЮЧА
  if (!apiKey) {
      throw new Error(`API Key для ${config.SERVICE} не настроен.`);
  }

  // 1. Формирование истории и промпта
  const apiMessages = [];
  
  // Используем ГЛОБАЛЬНЫЙ ПРОМПТ для обучения бота
  apiMessages.push({ "role": "system", "content": SYSTEM_PROMPT }); 
  
  apiMessages.push({
      role: 'user',
      content: messageText
  });
  
  // 2. Формирование тела запроса
  const body = {
      model: model,
      messages: apiMessages,
      stream: false,
      temperature: 0.7,
      max_tokens: 4096,
  };

  const url = `${baseUrl}/chat/completions`;

  // 3. Отправка запроса
  const response = await fetch(url, {
      method: 'POST',
      headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify(body),
  });

  if (!response.ok) {
      const errorText = await response.text();
      
      throw new Error(`BOTHUB API error (Status ${response.status}): ${errorText}`);
  }

  // 4. Обработка ответа
  const data = await response.json();
  let responseText = '';

  if (data.choices && data.choices.length > 0) {
      responseText = data.choices[0].message.content.trim();
  } 
  
  if (responseText) {
      return responseText;
  } else {
      throw new Error(`BOTHUB API response error: Received empty content from model.`);
  }
}


// --- ГЛОБАЛЬНАЯ КОНФИГУРАЦИЯ AI-СЕРВИСОВ (AI_MODELS) ---
const AI_MODELS = {
  // --- WORKERS AI (БЕСПЛАТНЫЕ, РАБОЧИЕ) ---

  // ✅ [Текст в Текст]
  TEXT_TO_TEXT_WORKERS_AI: { 
      SERVICE: 'WORKERS_AI', 
      FUNCTION: callWorkersAIChat, 
      //MODEL: '@cf/google/gemma-2b-it-lora', // тупой ЛЛама
      MODEL: '@cf/qwen/qwen2.5-coder-32b-instruct', // программерская
      //MODEL: '@cf/deepseek-ai/deepseek-r1-distill-qwen-32b', // думающая
      //MODEL: '@cf/qwen/qwq-32b', // думающая
      API_KEY: 'CLOUDFLARE_API_TOKEN', 
      BASE_URL: 'AI_RUN' // Вызов через env.AI.run
  },
  // ✅ [Изображение в Текст (Видение)]
  IMAGE_TO_TEXT_WORKERS_AI: { 
    SERVICE: 'WORKERS_AI', 
    FUNCTION: callWorkersAIVision,
    MODEL: '@cf/unum/uform-gen2-qwen-500m', 
    API_KEY: 'CLOUDFLARE_API_TOKEN', 
    BASE_URL: 'AI_RUN'
  },

  // --- СЕРВИСЫ GOOGLE ---

  // --- GEMINI ---
  // ✅ Прекрасно работает текстовый чат
  TEXT_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiChat, 
    MODEL: 'gemini-2.5-flash', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание голоса
  AUDIO_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    //FUNCTION: callGeminiSpeechToText,
    MODEL: 'gemini-2.5-flash', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание голоса
  VIDEO_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    //FUNCTION: callGeminiSpeechToText,
    MODEL: 'gemini-2.5-flash', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание фото
  IMAGE_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiVision, 
    //MODEL: 'gemini-2.0-flash', 
    MODEL: 'gemini-2.5-flash', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание видео
  VIDEO_TO_ANALYSIS_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiVideoVision, 
    MODEL: 'gemini-2.5-flash', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },

  // --- BOTHUB (ПЛАТНЫЕ, ТЕСТОВЫЕ) ---

  // --- BOTHUB TEXT --- (БЕСПЛАТНО)
  TEXT_TO_TEXT_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    FUNCTION: callBotHubTextChat, 
    //MODEL: 'deepseek-chat-v3-0324:free', 
    //MODEL: 'gpt-oss-20b:free',   
    MODEL: 'gemini-2.5-flash',       
    API_KEY: 'BOTHUB_API_KEY', 
    //BASE_URL: 'https://bothub.chat/api/v2/openai/v1/chat/completions'
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
},
  // --- BOTHUB WHISPER-1 --- (ПЛАТНО)
  AUDIO_TO_TEXT_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    //FUNCTION: callBotHubAudioToText,
    MODEL: 'whisper-1', 
    API_KEY: 'BOTHUB_API_KEY', 
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
  },
  // --- BOTHUB VISION --- (ПЛАТНО и нестабильно)
  IMAGE_TO_TEXT_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    //FUNCTION: callBotHubVisionChat, 
    //MODEL: 'gemini-2.0-flash-exp:free', 
    //MODEL: 'gpt-4o',   
    MODEL: 'gemini-2.5-flash',         
    API_KEY: 'BOTHUB_API_KEY', 
    //BASE_URL: 'https://bothub.chat/api/v2/openai/v1/chat/completions'
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
  },
  // --- BOTHUB WHISPER-1 --- (ПЛАТНО)
  VIDEO_TO_TEXT_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    //FUNCTION: callBotHubAudioToText,
    MODEL: 'whisper-1', 
    API_KEY: 'BOTHUB_API_KEY', 
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
  },
  // --- BOTHUB VIDEO VISION --- (ПЛАТНО)
  VIDEO_TO_ANALYSIS_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    //FUNCTION: callBothubVideoVision, 
    MODEL: 'gemini-2.5-flash',         
    API_KEY: 'BOTHUB_API_KEY', 
    //BASE_URL: 'https://bothub.chat/api/v2/openai/v1/chat/completions'
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
  },
  /* --- DEEPSEEK --- (ПЛАТНО $0.028 минимум)
    TEXT_TO_TEXT_DEEPSEEK: { 
        SERVICE: 'DEEPSEEK', 
        FUNCTION: callDeepSeekChat, 
        MODEL: 'deepseek-chat', 
        API_KEY: 'DEEPSEEK_API_KEY', 
        BASE_URL: 'https://api.deepseek.com/v1'
    },  */  
};
// --- КАРТА СЕРВИСОВ ДЛЯ АДМИН-МЕНЮ ---
const SERVICE_TYPE_MAP = {
  'TEXT_TO_TEXT': { name: '✍️ Text → Text', kvKey: 'ACTIVE_MODEL_TEXT_TO_TEXT' },
  'AUDIO_TO_TEXT': { name: '🎤 Audio → Text', kvKey: 'ACTIVE_MODEL_AUDIO_TO_TEXT' },
  'VIDEO_TO_TEXT': { name: '🎧 Video → Text', kvKey: 'ACTIVE_MODEL_VIDEO_TO_TEXT' },
  'IMAGE_TO_TEXT': { name: '👁️ Image → Text', kvKey: 'ACTIVE_MODEL_IMAGE_TO_TEXT' },
  'VIDEO_TO_ANALYSIS' : {name: '👀 Video → Analysis', kvKey: 'ACTIVE_MODEL_VIDEO_TO_ANALYSIS' }
};
// !!! ВАЖНО: Определите эту константу после AI_MODELS !!!
const AI_MODEL_MENU_CONFIG = generateModelMenuConfig(AI_MODELS);

/**
 * @description Генерирует полную карту AI-сервисов для меню, группируя модели по типу.
 * @param {Object} AI_MODELS - Глобальный объект AI-моделей.
 * @returns {Object} Структура для меню.
 */
function generateModelMenuConfig(AI_MODELS) {
  const config = {};

  for (const [modelKey, modelDetails] of Object.entries(AI_MODELS)) {
      // Извлекаем тип сервиса (например, 'TEXT_TO_TEXT')
      const parts = modelKey.split('_');
      // Собираем первые три части: TEXT_TO_TEXT, IMAGE_TO_IMAGE и т.д.
      const serviceType = parts.slice(0, 3).join('_');

      if (!SERVICE_TYPE_MAP[serviceType]) continue; // Пропускаем неизвестные типы

      if (!config[serviceType]) {
          config[serviceType] = {
              name: SERVICE_TYPE_MAP[serviceType].name,
              kvKey: SERVICE_TYPE_MAP[serviceType].kvKey,
              models: {}
          };
      }

      // Формируем пользовательское название модели
      let friendlyName = `${modelDetails.SERVICE}: ${modelDetails.MODEL}`;
      
      config[serviceType].models[modelKey] = friendlyName;
  }
  return config;
  // Можно хранить выбор в KV, но пока — жёстко зададим Gemini как основной
  //return AI_MODELS.TEXT_TO_TEXT_GEMINI;
}

/**
 * Загружает активную AI-модель из KV по типу сервиса.
 * @param {string} serviceType - Тип сервиса (например, 'TEXT_TO_TEXT').
 * @param {Object} env - Окружение с доступом к KV (`USER_DB`).
 * @returns {Promise<Object>} Конфигурация модели из AI_MODELS.
 */
async function loadActiveConfig(serviceType, env) {
  const serviceConfig = SERVICE_TYPE_MAP[serviceType];
  if (!serviceConfig) {
    throw new Error(`Неизвестный тип сервиса: ${serviceType}`);
  }

  const kvKey = serviceConfig.kvKey;
  const defaultModelKey = Object.keys(AI_MODEL_MENU_CONFIG[serviceType]?.models || AI_MODELS).find(
    key => key.startsWith(serviceType)
  ) || Object.keys(AI_MODELS).find(key => key.startsWith(serviceType));

  const activeModelKey = await env.USER_DB.get(kvKey) || defaultModelKey;
  const modelConfig = AI_MODELS[activeModelKey];
  if (!modelConfig) {
    throw new Error(`Модель ${activeModelKey} не найдена в AI_MODELS`);
  }

  await logDebug(`🧠 Активная модель для ${serviceType}: <code>${activeModelKey}</code>`, env);
  return modelConfig;
}