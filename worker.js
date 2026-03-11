/* 🗄 Telegram Storage Bot "Хранилка" by Leshiy
Telegram-бот для автоматической загрузки фото и видео в облачное хранилище с реферальной системой доступа. 

✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Google, Яндекс.Диск, Облако Mail.Ru WebDAV и др.) 
прямо через телеграмм. Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку.
Универсальность: Поддержка облачного WebDAV (Google, Яндекс.Диск, Облако Mail.Ru и др.).
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео.
Поддержка WEBM: Возможность сохранять видеофайлы в современных форматах без потери качества.
Диагностика: Команда /debug для проверки статуса подключения к хранилищу в реальном времени.
Скрытая функция: Команда /search для поиска и возможность достать файлы с хранилки.
*/
// Глобальные константы
const version = "v2.3.0 от 09.01.2026"; // актуальная версия

// ----------------------------------------------------
// ГЛАВНЫЙ ОБРАБОТЧИК (WEBHOOK) Fetch
// ----------------------------------------------------
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const hostname = url.hostname;

    // 1. ВЕБ-ИНТЕРФЕЙС
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

    // 3. ОБРАБОТКА ВЕБХУКОВ (POST запросы)
    if (request.method === "POST") {
      try {
        const url = new URL(request.url);
        const body = await request.json(); // ← Читаем ОДИН РАЗ

        if (url.pathname === "/vk") {
          // Передаём уже распаршенное тело
          return await handleVK(body, env, hostname, ctx);
        } else {
          // Telegram
          if (body.callback_query) {
            await handleCallbackQuery(body.callback_query, env, ctx);
            return new Response("OK");
          }
          if (body.message || body.edited_message) {
            return await handleTelegramUpdate({ ...body }, env, hostname, ctx);
          }
        }
      } catch (e) {
        console.error("Критическая ошибка:", e);
      }
      return new Response("OK", { status: 200 });
    }

    // Все остальное — 404
    return new Response("Not Found", { status: 404 });
  }
}

/**
 * Отправляет ТЕКСТОВОЕ сообщение во ВКонтакте (без клавиатуры).
 * @param {number} peerId - ID чата (peer_id).
 * @param {string} text - Текст сообщения.
 * @param {Object} env - Окружение.
 */
async function sendVKMessage(peerId, text, env) {
  const url = `https://api.vk.com/method/messages.send?v=5.199&access_token=${env.VK_GROUP_TOKEN}&peer_id=${peerId}&message=${encodeURIComponent(text)}&random_id=${Date.now()}`;
  const response = await fetch(url, { method: "GET" });
  return response;
}

/**
 * Отправляет сообщение во ВКонтакте С КЛАВИАТУРОЙ.
 * Работает ТОЛЬКО в беседах (peer_id < 0).
 * @param {number} peerId - ID чата (должен быть < 0).
 * @param {string} text - Текст сообщения.
 * @param {Object} kb - Клавиатура (объект inline_keyboard).
 * @param {Object} env - Окружение.
 */
async function sendVKMessageWithKeyboard(peerId, text, kb, env) {
  const url = "https://api.vk.com/method/messages.send";
  const payload = {
    v: "5.199",
    access_token: env.VK_GROUP_TOKEN,
    peer_id: Number(peerId),
    message: text,
    random_id: Date.now(),
    dont_parse_links: true
  };

  if (kb) {
    // VK требует строку JSON для keyboard
    payload.keyboard = JSON.stringify(kb);
  }

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  return response;
}

function getStartKeyboardVK(userId, hostname, env, inviteData = null) {
  let rows = [];
  // Все кнопки — через payload
  rows.push([{ text: "🔗 Яндекс.Диск", payload: JSON.stringify({ cmd: "auth", provider: "yandex" }) }]);
  rows.push([{ text: "🔗 Google Drive", payload: JSON.stringify({ cmd: "auth", provider: "google" }) }]);
  rows.push([{ text: "🔗 Dropbox", payload: JSON.stringify({ cmd: "auth", provider: "dropbox" }) }]);
  rows.push([{ text: "🔗 Mail.ru (WebDAV)", payload: JSON.stringify({ cmd: "auth_mailru" }) }]);
  rows.push([{ text: "🖥️ Свой WebDAV", payload: JSON.stringify({ cmd: "auth_webdav" }) }]);
  if (inviteData) {
    rows.push([{ text: "🤝 Подтвердить", payload: JSON.stringify({ cmd: "confirm_ref", token: inviteData.token }) }]);
  } else {
    rows.push([{ text: "🤝 Подключить друга", payload: JSON.stringify({ cmd: "ask_ref" }) }]);
  }
  return { inline_keyboard: rows };
}

/**
* Генерирует интерфейс поиска во ВКонтакте.
* @param {string} searchKey - Ключ поиска.
* @param {number} offset - Смещение.
* @param {Object} env - Окружение.
* @param {string} userId - ID пользователя.
*/
async function renderSearchPageVK(searchKey, offset, env, userId) {
  const dataRaw = await env.USER_DB.get(searchKey);
  if (!dataRaw) return { text: "❌ Поиск устарел или не найден.", kb: null };
  const searchData = JSON.parse(dataRaw);
  const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
  const total = searchData.ids.length;
  const pageIds = searchData.ids.slice(offset, offset + 5);
  let list = `🔍 <b>Найдено всего: ${total}</b> (Страница ${Math.floor(offset/5) + 1})
`;
  const userFolder = userData?.folderId || "/";
  for (const id of pageIds) {
    const f = await env.FILES_DB.prepare("SELECT fileName, provider, remotePath FROM files WHERE id = ?").bind(id).first();
    const isProviderOk = userData && f?.provider === userData.provider;
    const isPathOk = f?.remotePath ? f.remotePath === userFolder : false;
    const status = (isProviderOk && isPathOk) ? '🟢' : '🔴';
    list += `${status} <code>${f?.fileName || 'Файл'}</code>
`;
  }
  list += `
Активное подключение:`;
  list += `
<b>🔌 Провайдер: ${userData?.provider}</b> 📁 Папка: ${userData?.folderId}`;
  list += `
<b>🟢 доступно</b> | <b>🔴 не доступно</b> для выгрузки`;

  // Формируем клавиатуру
  const kb = { inline_keyboard: [
    [{ text: "📥 Выгрузить эти файлы", callback_data: `dl:${searchKey}:${offset}` },
    { text: "🔎 Изменить поиск", callback_data: "search_retry" }],
    []
  ]};

  if (offset > 0) {
    kb.inline_keyboard[1].push({ text: `⬅️ стр. ${Math.floor(offset/5) + 0}`, callback_data: `pg:${searchKey}:${offset - 5}` });
  }
  if (offset + 5 < total) {
    kb.inline_keyboard[1].push({ text: `⬆️ стр. ${Math.floor(offset/5) + 1}`, callback_data: "dummy_ignore" });
  }
  if (offset + 5 < total) {
    kb.inline_keyboard[1].push({ text: `стр. ${Math.floor(offset/5) + 2} ➡️`, callback_data: `pg:${searchKey}:${offset + 5}` });
  }

  return { text: list, kb };
}

async function handleTelegramUpdate(update, env, hostname, ctx) {
  const msg = update.message || update.edited_message;
  if (!msg) return new Response("OK");

  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text || "";
  const userKey = `user:${userId}`;

  // Данные админа и базовая загрузка данных пользователя
  const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
  const isAdmin = adminCfg?.admins?.includes(String(userId));
  let userData = await env.USER_DB.get(userKey, { type: "json" });

  // --- 1. МОСТ ДЛЯ РЕФЕРАЛА (Приоритет) ---
  if (userData && userData.shared_from) {
    const ownerId = String(userData.shared_from);
    const ownerData = await env.USER_DB.get(`user:${ownerId}`, { type: "json" });

    if (ownerData) {
      // Сохраняем папку, которая прописана у реферала в его ключе user:ID
      const refFolder = userData.folderId || "/"; 

      userData = { 
        ...ownerData, // Берем токены владельца
        is_ref: true, 
        shared_from: ownerId,
        folderId: refFolder // <--- ОСТАВЛЯЕМ ПАПКУ РЕФЕРАЛА, А НЕ ВЛАДЕЛЬЦА
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
            connected_at: Date.now(),
            folderId: ownerData.folderId, // <--- ПАПКА ТЕПЕРЬ ТУТ
          };
          await env.USER_DB.put(userKey, JSON.stringify(userData));          
          // Добавляем инфо о папке в сообщение, чтобы сразу видеть результат
          await sendMessage(chatId, `🤝 <b>Готово!</b>\nТы подключился к хранилке пользователя <code>${inviteData.inviterId}</code> (${ownerData.provider}).\n📁 Папка: <code>${ownerData.folderId}</code>`, null, env);
          await sendMessage(inviteData.inviterId, `🔔 Твоей хранилкой начал пользоваться ID <code>${userId}</code> (папка: ${userData.folderId})`, null, env);
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
                  `/search — Поиск файлов по хранилке\n` +
                  `/disconnect — Отключить диск друга\n` +
                  `/debug — Техническая информация`;
    
    if (inviteData && !userData?.shared_from) {
      welcome += `\n\n🎁 <b>Найдено приглашение!</b>\nОт владельца облака <b>${inviteData.provider}</b>.\nНажми кнопку подтверждения в меню ниже.`;
    }
  
    const kb = getStartKeyboard(userId, hostname, env, inviteData);
    return await sendMessage(chatId, welcome, kb, env);
  }

  // --- 3. ОБЩАЯ ПРОВЕРКА ДОСТУПА (Для всех команд ниже и файлов) ---
  const hasAccess = isAdmin || (userData && (userData.access_token || userData.provider === 'webdav' || userData.shared_from));
  
  if (!hasAccess) {
    const restrictedMsg = `🚫 <b>Доступ ограничен.</b>\nУ тебя не подключено облако и нет активной ссылки от друга.`;
    return await sendMessage(chatId, restrictedMsg, null, env);
  }

  // --- КОМАНДА /SHARE ---
  if (text.startsWith("/share")) {
    if (userData.is_ref) {
      return await sendMessage(chatId, "⚠️ Ты используешь чужой диск и не можешь создавать свои реф-ссылки.", null, env);
    }
    
    // Берем папку из команды (напр. /share STORAGE) или текущую выбранную
    const currentFolder = userData?.folderId || "Не установлена (Root)";
    
    const inviteToken = Math.random().toString(36).substring(2, 12);
    const inviteData = {
      inviterId: userId,
      provider: userData.provider,
      token: inviteToken,
      folderId: currentFolder, // КЛЮЧЕВОЕ: Добавили папку в объект инвайта
      timestamp: Date.now()
    };
    
    await env.USER_DB.put(`invite:${inviteToken}`, JSON.stringify(inviteData));
    
    const botName = env.BOT_USERNAME || "leshiy_storage_bot"; 
    const inviteLink = `https://t.me/${botName}?start=ref_${inviteToken}`;
    
    return await sendMessage(chatId, 
      `🚀 <b>Твоя ссылка для друга:</b>\n<code>${inviteLink}</code>\n\n` +
      `📁 Папка: <b>${currentFolder}</b>\n` +
      `Облако: <b>${userData.provider}</b>`, 
      null, env
    );
  }

  // --- КОМАНДА /DEBUG ---
  if (text === "/debug") {
    // Используем ?. чтобы бот не падал, если userData пустой
    const currentProvider = userData?.provider || "Не определён";
    const currentFolder = userData?.folderId || "Не установлена";
    
    // Статус меняется в зависимости от наличия провайдера
    const statusIcon = userData?.provider ? "✅ Соединение активно" : "❌ Не подключен";
    const debugMsg = `🤖 <b>Бот онлайн</b>\n` +
                     `📦 Версия: ${version}\n` +
                     `🔗 Статус: ${statusIcon}\n` +
                     `🔌 Провайдер: ${currentProvider}\n` +
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

  // --- КОМАНДА /SEARCH ---
  if (text.startsWith("/search")) {
    // ПРОВЕРКА: Если диска нет, то и искать не в чем
    if (!userData || (!userData.provider && !userData.shared_from)) {
      const noDiscMsg = `⚠️ <b>Поиск недоступен</b>\n\n` +
                        `Твоё хранилище не подключено. Сначала авторизуйся или подключись к другу, чтобы я мог просканировать файлы.`;
      return await sendMessage(chatId, noDiscMsg, null, env);
    }
    const query = text.replace(/^\/search\s*/i, '').trim();

    // Если запрос пустой — выдаем инструкцию и ставим стейт
    if (!query) {
      // Ставим стейт ожидания поиска на 5 минут
      await env.USER_DB.put(`state:${userId}`, "waiting_for_search", { expirationTtl: 300 });

      const helpMsg = `🔎 <b>Поиск по архиву</b>\n\n` +
                      `Пришли мне название файла или его часть.\n` +
                      `<i>Примеры: "сейф", "jpg", "2025"</i>\n\n` +
                      `🔹 Ищу только по именам файлов.\n` +
                      `🔹 Поиск не чувствителен к регистру.\n\n` +
                      `👇 <b>Просто напиши, что искать:</b>`;
      
      return await sendMessage(chatId, helpMsg, null, env);
    }

    // Если запрос есть — выполняем поиск
    await sendMessage(chatId, "⏳ <b>Выполняю поиск файлов...</b>", null, env);
    //const searchResult = await searchFilesByQuery(userId, isAdmin, query, env);
    // Определяем тип поиска: обычный или интеллектуальный
    const isAIQuery = query.includes(" ") && isAdmin; // Только у админа и только если есть пробел

    let searchResult;
    if (isAIQuery) {
      // Интеллектуальный поиск (только по своим файлам, даже для админа)
      searchResult = await searchAIFilesByQuery(userId, isAdmin, query, env);
    } else {
      // Обычный поиск (админ — по всем, пользователь — по своим)
      searchResult = await searchFilesByQuery(userId, isAdmin, query, env);
    }

    if (!searchResult.success || searchResult.fileIds.length === 0) {
      return await sendMessage(chatId, `❌ По запросу "<b>${query}</b>" ничего не найдено.`, null, env);
    }

    const shortId = Math.random().toString(36).substring(2, 8);
    const searchKey = `s:${userId}:${shortId}`;
    
    await env.USER_DB.put(searchKey, JSON.stringify({
      ids: searchResult.fileIds,
      q: query
    }), { expirationTtl: 3600 });

    const { text: msgText, kb } = await renderSearchPage(searchKey, 0, env, userId);
    return await sendMessage(chatId, msgText, kb, env);
  }

  // - КОМАНДА /AI_SEARCH - 
  if (text.startsWith("/ai_search") && isAdmin) {
    const query = text.replace(/^\/ai_search\s*/i, '').trim();
    if (!query) return await sendMessage(chatId, "🔎 Что ищем с помощью ИИ?", null, env);
  
    await sendMessage(chatId, "⏳ <b>Выполняю интеллектуальный поиск...</b>", null, env);
    const searchResult = await searchAIFilesByQuery(userId, isAdmin, query, env);
  
    if (!searchResult.success) {
      return await sendMessage(chatId, "❌ Ошибка поиска.", null, env);
    }
    if (searchResult.fileIds.length === 0) {
      return await sendMessage(chatId, "🔍 По вашему запросу ничего не найдено.", null, env);
    }
  
    // Ключ теперь короткий, чтобы влезть в лимиты кнопок TG (64 байта)
    const shortId = Math.random().toString(36).substring(2, 8); // Очень короткий ID
    const searchKey = `s:${userId}:${shortId}`;
    await env.USER_DB.put(searchKey, JSON.stringify({
      ids: searchResult.fileIds,
      q: query
    }), { expirationTtl: 3600 }); // 1 час
  
    // Генерируем интерфейс (функцию создадим ниже)
    // Вызываем рендер
    const { text: msgText, kb } = await renderSearchPage(searchKey, 0, env, userId);
    return await sendMessage(chatId, msgText, kb, env);
  }

  // --- КОМАНДА /ADMIN ---
  if (text === "/admin" && isAdmin) {
    const list = await env.USER_DB.list({ prefix: "user:" });
    const authIds = list.keys.map(k => k.name.split(":")[1]);
    const allowedIds = await env.USER_DB.get("admin:allowed_ids", { type: "json" }) || [];
    const allUniqueIds = [...new Set([...authIds, ...allowedIds])];
  
    const adminMsg = `⚙️ <b>Панель администратора</b>\n\n` +
      `🆔 Админ ID: <code>${userId}</code>\n\n` +
      `✅ <b>Авторизованы (диск подключен):</b>\n` +
      (authIds.length > 0 ? authIds.map(id => `• <code>${id}</code>`).join("\n") : "—") +
  
      `\n\n🚀 Версия: ${version}`;
    const adminKeyboard = {
      inline_keyboard: [
        [{ text: "🧠 Настройки ИИ", callback_data: "ai_menu_main" }],
        [{ text: "🚪 Выход из режима админа", callback_data: "admin_exit" }]
      ]
    };
    return await sendMessage(chatId, adminMsg, adminKeyboard, env);
  }

  // --- КОМАНДА /ai_settings ---
  if (text === "/ai_settings" && isAdmin) {
    const buttons = Object.entries(SERVICE_TYPE_MAP).map(([type, info]) => [
      { text: info.name, callback_data: `ai_menu:${type}` }
    ]);
    return await sendMessage(
      chatId,
      `🧠 <b>Выберите тип ИИ-сервиса:</b>`,
      { inline_keyboard: buttons },
      env
    );
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
  const isVideo = !!msg.video || !!msg.video_note;
  const isPhoto = !!msg.photo;
  const isAudio = !!msg.audio;
  const isVoice = !!msg.voice;

  // --- ОБРАБОТКА ФАЙЛОВ ---
  if (isDoc || isVideo || isPhoto || isAudio || isVoice) {
    await sendMessage(chatId, "⏳ <b>Начинаю загрузку в облако...</b>", null, env);
    try {
      const fileObj = msg.document || msg.video || msg.video_note || msg.audio || msg.voice || (msg.photo ? msg.photo[msg.photo.length - 1] : null);
      if (!fileObj == null) throw new Error("Файл не найден");

      let fileName = "";
      //const fType = isPhoto ? "photo" : isVideo ? "video" : isAudio ? "audio" : isVoice ? "voice" : "document";
      // --- ОПРЕДЕЛЕНИЕ ТИПА ФАЙЛА ---
      let fType = "document";

      if (isPhoto) {
        fType = "photo";
      } else if (isVideo) {
        fType = "video";
      } else if (isAudio) {
        fType = "audio";
      } else if (isVoice) {
        fType = "voice";
      } else if (isDoc) {
        // Проверяем MIME-тип
        if (msg.document.mime_type && msg.document.mime_type.startsWith('image/')) {
          fType = "photo";
        } else {
          // Проверяем расширение файла
          const fileName = msg.document.file_name || "";
          const ext = fileName.toLowerCase().split('.').pop();
          if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'tiff'].includes(ext)) {
            fType = "photo";
          } else if (['mp4', 'avi', 'mov', 'mkv', 'webm'].includes(ext)) {
            fType = "video";
          } else if (['mp3', 'wav', 'ogg', 'flac'].includes(ext)) {
            fType = "audio";
          }
        }
      }

      if (isDoc || isVideo || isAudio) {
        const dateStr = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = fileObj.file_name || `file_${dateStr}.mp4`;
      } else if (isVoice) {
        const dateStr = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = `Voice_${dateStr}.ogg`;
      } else {
        const dateStr = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-');
        fileName = `Photo_${dateStr}.jpg`;
      }

      // 🔑 ЕДИНСТВЕННОЕ СКАЧИВАНИЕ ФАЙЛА
      const arrayBuffer = await getFileStream(fileObj.file_id, env);

      // Загрузка в облако
      let success = false;
      if (userData.provider === "google") {
        success = await uploadToGoogleFromArrayBuffer(arrayBuffer, fileName, userData.access_token, userData.folderId || "root");
      } else if (userData.provider === "yandex") {
        success = await uploadToYandexFromArrayBuffer(arrayBuffer, fileName, userData.access_token, userData.folderId || "");
      } else if (userData.provider === "dropbox") {
        success = await uploadToDropboxFromArrayBuffer(arrayBuffer, fileName, userData.access_token, userData.folderId || "Storage");
      } else if (userData.provider === "webdav") {
        success = await uploadWebDAVFromArrayBuffer(arrayBuffer, fileName, userData, env);
      }
      if (success) {
        // ✅ Сохраняем метаданные
        await env.FILES_DB.prepare(
          "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
        ).bind(
          String(userId), fileName, fileObj.file_id, fType, userData.provider,
          userData.folderId || "Root", Date.now()
        ).run();

        // ✅ Фоновая генерация описания (если фото/видео/аудио/голос)
        if (fType === "photo" || fType === "video" || fType === "audio" || fType === "voice" || fType === "document") {
          ctx.waitUntil(
            (async () => {
              try {
                let description;
                if (fType === "photo") {
                  const modelConfig = await loadActiveConfig('IMAGE_TO_TEXT', env);
                  description = await modelConfig.FUNCTION(modelConfig, arrayBuffer, env);
                } else if (fType === "video") {
                  const mimeType = "video/mp4";
                  //const modelConfig = await loadActiveConfig('VIDEO_TO_TEXT', env);
                  const modelConfig = await loadActiveConfig('VIDEO_TO_ANALYSIS', env);
                  description = await modelConfig.FUNCTION(modelConfig, arrayBuffer, env, mimeType);
                } else if (fType === "audio" || fType === "voice") {
                  const modelConfig = await loadActiveConfig('AUDIO_TO_TEXT', env);
                  description = await modelConfig.FUNCTION(modelConfig, arrayBuffer, env);
                } else if (fType === "document") {
                  const mimeType = msg.document?.mime_type || getMimeTypeFromExtension(msg.document?.file_name);
                  //const mimeType = "application/pdf";
                  const modelConfig = await loadActiveConfig('DOCUMENT_TO_TEXT', env);
                  description = await modelConfig.FUNCTION(modelConfig, arrayBuffer, env, mimeType);
                }
                await env.FILES_DB.prepare(
                  "UPDATE files SET ai_description = ? WHERE userId = ? AND fileName = ?"
                ).bind(description, String(userId), fileName).run();
              } catch (e) {
                await logDebug(`⚠️ Ошибка генерации описания: ${e.message}`, env);
              }
            })()
          );
        }

        await sendMessage(chatId, `✅ Файл <b>${fileName}</b> сохранен в ${userData.provider}!`, null, env);
        return new Response("OK");
      } else {
        await sendMessage(chatId, "❌ Ошибка при загрузке. Проверьте токены или место на диске.", null, env);
        return new Response("OK");
      }
    } catch (e) {
      await sendMessage(chatId, `❌ Ошибка: ${e.message}`, null, env);
      return new Response("OK");
    }
  }

  // 1. ПРИОРИТЕТ: Обработка состояний (WebDAV и папки)
  const userState = await env.USER_DB.get(`state:${userId}`);
  // 1. ПРИОРИТЕТ: Обработка состояний (Ввод данных)
  if (userState === "wait_webdav_url") {
    try {
      // Если userData еще не существует (после disconnect), создаем объект
      if (!userData) {
        userData = { id: userId, username: msg.from.username || "User" };
      }

      let rawText = text.trim();
      
      // Отсекаем протокол
      const protocolMatch = rawText.match(/^(https?:\/\/)/);
      if (!protocolMatch) throw new Error("Ссылка должна начинаться с https://");
      const protocol = protocolMatch[1];
      let linkWithoutProtocol = rawText.replace(protocol, "");

      // Ищем ПОСЛЕДНЮЮ собаку (отделяет почту:пароль от сервера)
      const lastAtIndex = linkWithoutProtocol.lastIndexOf("@");
      if (lastAtIndex === -1) throw new Error("Неверный формат. Используй логин:пароль@сервер");

      const authPart = linkWithoutProtocol.substring(0, lastAtIndex); 
      const hostPart = linkWithoutProtocol.substring(lastAtIndex + 1); 

      // Ищем ПЕРВОЕ двоеточие в authPart (отделяет почту от пароля)
      const colonIndex = authPart.indexOf(":");
      if (colonIndex === -1) throw new Error("Не найден пароль (двоеточие)");

      const user = authPart.substring(0, colonIndex);
      const pass = authPart.substring(colonIndex + 1);

      // ПИШЕМ ДАННЫЕ (Используем общий провайдер 'webdav')
      userData.provider = "webdav"; 
      userData.user = user;
      userData.pass = pass;
      userData.host = `${protocol}${hostPart}`;
      userData.webdav_url = rawText; // Полная ссылка для совместимости
      userData.folderId = "Storage"; 

      // Сохраняем пользователя в KV
      await env.USER_DB.put(`user:${userId}`, JSON.stringify(userData));
      // Чистим состояние
      await env.USER_DB.delete(`state:${userId}`);

      // УДАЛЕНИЕ СООБЩЕНИЯ С ПАРОЛЕМ
      try {
        await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/deleteMessage`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ chat_id: chatId, message_id: msg.message_id })
        });
      } catch (e) {}

      await sendMessage(chatId, "✅ <b>WebDAV успешно настроен!</b>\nПровайдер: <code>webdav</code>\nПапка: <code>Storage</code>", null, env);

      // Пробуем создать папку (твоя функция)
      await createWebDAVFolder("Storage", userData);

      return new Response("OK");
    } catch (e) {
      await sendMessage(chatId, `❌ <b>Ошибка настройки:</b>\n${e.message}`, null, env);
      console.error("WebDAV Parse Error:", e);
      return new Response("OK");
    }
  }

  // Проверка активного стейта поиска
  if (userState === "waiting_for_search" && !text.startsWith("/")) {
    // Сбрасываем стейт, чтобы не зациклиться
    await env.USER_DB.delete(`state:${userId}`);
    
    // Перенаправляем текст в логику поиска, как будто ввели /search ТЕКСТ
    const searchQuery = text.trim();
    // Вызываем поиск. Для простоты здесь просто повторяем вызов sendMessage с поиском:
    await sendMessage(chatId, `🔍 Ищу: <b>${searchQuery}</b>...`, null, env);
    const searchResult = await searchFilesByQuery(userId, isAdmin, searchQuery, env);

    if (!searchResult.success || searchResult.fileIds.length === 0) {
      return await sendMessage(chatId, `❌ По запросу "<b>${searchQuery}</b>" ничего не найдено.`, null, env);
    }

    const shortId = Math.random().toString(36).substring(2, 8);
    const searchKey = `s:${userId}:${shortId}`;
    
    await env.USER_DB.put(searchKey, JSON.stringify({
      ids: searchResult.fileIds,
      q: searchQuery
    }), { expirationTtl: 3600 });

    const { text: msgText, kb } = await renderSearchPage(searchKey, 0, env, userId);
    return await sendMessage(chatId, msgText, kb, env);
  }

  if (userState === "wait_manual_folder") {
    userData.folderId = text.trim();
    await env.USER_DB.put(`user:${userId}`, JSON.stringify(userData));
    await env.USER_DB.delete(`state:${userId}`);
    await sendMessage(chatId, `✅ Папка установлена: <code>${userData.folderId}</code>`, null, env);
    return new Response("OK");
  }

  // 3. РЕФЕРАЛЫ (Ловим и токен, и целую ссылку)
  const refMatch = text.match(/ref_([a-zA-Z0-9]+)/) || text.match(/^([a-zA-Z0-9]{8,12})$/);
  if (refMatch) {
    const token = refMatch[1];
    const invite = await env.USER_DB.get(`invite:${token}`, { type: "json" });
    if (invite) {
      const ownerData = await env.USER_DB.get(`user:${invite.inviterId}`, { type: "json" });
      if (ownerData) {
        userData = { ...ownerData, shared_from: invite.inviterId, is_ref: true, connected_at: Date.now() };
        await env.USER_DB.put(`user:${userId}`, JSON.stringify(userData));
        await sendMessage(chatId, `🤝 Вы подключились к облаку друга (${userData.provider})`, null, env);
        return new Response("OK");
      }
    }
    return new Response("OK");
  }

  // --- ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ ---
  if (text && userData) {
    try {
      const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
      //const responseText = await modelConfig.FUNCTION(text, modelConfig, env);
      const responseText = await handleChatRequest(text, modelConfig, env);
      await sendMessage(chatId, responseText.substring(0, 4000), null, env);
    } catch (e) {
      console.error("AI Error:", e);
    }
    return new Response("OK");
  }
}

/**
 * Обрабатывает входящие запросы от VK API.
 * @param {Object} body - Уже распаршенное тело запроса.
 * @param {Object} env - Окружение Cloudflare Worker.
 * @param {string} hostname - Хост (например, leshiy-storage-bot.leshiyalex.workers.dev)
 * @param {Object} ctx - Контекст выполнения (для waitUntil)
 * @returns {Promise<Response>} Ответ для VK.
 */
async function handleVK(body, env, hostname, ctx) {
  let chatId = null;
  try {
    // --- 1. Подтверждение сервера ---
    if (body.type === "confirmation") {
      return new Response("87f0c4ac");
    }

    // --- 2. Обработка сообщений ---
    if (body.type === "message_new") {
      const message = body.object.message;
      chatId = message.peer_id;
      const userId = message.from_id;
      const text = (message.text || "").trim();
      const userKey = `user:${userId}`;

      let userData = await env.USER_DB.get(userKey, { type: "json" });
      const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
      const isAdmin = adminCfg?.admins?.includes(String(userId));

      // --- КОМАНДА /START ---
      if (text.startsWith("/start")) {
        const args = text.split(" ")[1];
        let inviteData = null;

        if (args && args.startsWith("ref_")) {
          const token = args.split("_")[1];
          inviteData = await env.USER_DB.get(`invite:${token}`, { type: "json" });
          if (inviteData) {
            const ownerData = await env.USER_DB.get(`user:${inviteData.inviterId}`, { type: "json" });
            if (ownerData) {
              userData = {
                provider: ownerData.provider,
                shared_from: String(inviteData.inviterId),
                connected_at: Date.now(),
                folderId: ownerData.folderId,
              };
              await env.USER_DB.put(userKey, JSON.stringify(userData));
              await sendVKMessage(chatId, `🤝 Готово!\nТы подключился к хранилке пользователя ${inviteData.inviterId} (${ownerData.provider}).\n📁 Папка: ${ownerData.folderId}`, env);
              await sendVKMessage(inviteData.inviterId, `🔔 Твоей хранилкой начал пользоваться ID ${userId} (папка: ${userData.folderId})`, env);
            }
          } else {
            await sendVKMessage(chatId, "❌ Ссылка недействительна или устарела.", env);
          }
        }

        let statusText = "❌ Диск не подключен";
        if (userData && userData.provider) {
          const folderInfo = userData.folderId ? ` (папка: ${userData.folderId})` : " (корень)";
          const sharedInfo = userData.shared_from ? ` [Общий диск]` : "";
          statusText = `✅ ${userData.provider} подключен${folderInfo}${sharedInfo}`;
        }

        let welcome = `👋 Привет! Я твоя личная хранилка.\n📁 Просто пришли мне фото или видео, и я закину их на сервер.\n⚙️ Статус: ${statusText}\n📖 Команды:\n/folder — Выбрать папку\n/share — Создать ссылку для друга\n/search — Поиск файлов\n/disconnect — Отключить диск\n/debug — Техническая информация`;

        if (inviteData && !userData?.shared_from) {
          welcome += `\n\n🎁 Найдено приглашение!\nОт владельца облака ${inviteData.provider}.`;
        }

        await sendVKMessage(chatId, welcome, env);

        // Отправляем ссылки авторизации как текст (кнопки не работают в ЛС)
        if (!userData || !userData.provider) {
          const yAuth = `https://oauth.yandex.ru/authorize?response_type=code&client_id=${env.YANDEX_CLIENT_ID}&state=${userId}`;
          const gAuth = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${env.GOOGLE_CLIENT_ID}&redirect_uri=https://${hostname}/auth/google/callback&response_type=code&scope=${encodeURIComponent("https://www.googleapis.com/auth/drive.file")}&state=${userId}&access_type=offline&prompt=consent`;
          const dbxAuth = `https://www.dropbox.com/oauth2/authorize?client_id=${env.DROPBOX_CLIENT_ID}&response_type=code&redirect_uri=${encodeURIComponent(`https://${hostname}/auth/dropbox/callback`)}&token_access_type=offline&state=${userId}`;
          await sendVKMessage(chatId, `🔗 Подключить Яндекс.Диск:\n${yAuth}\n\n🔗 Подключить Google Drive:\n${gAuth}\n\n🔗 Подключить Dropbox:\n${dbxAuth}`, env);
        }

        return new Response("OK");
      }

      // --- КОМАНДА /ADMIN ---
      if (text === "/admin" && isAdmin) {
        const list = await env.USER_DB.list({ prefix: "user:" });
        const authIds = list.keys.map(k => k.name.split(":")[1]);
        const adminMsg = `⚙️ Панель администратора\n
🆔 Админ ID: ${userId}\n
✅ Авторизованы (диск подключен):\n${authIds.length > 0 ? authIds.join("\n") : "—"}
\n🚀 Версия: ${version}`;
        await sendVKMessage(chatId, adminMsg, env);
        return new Response("OK");
      }

      // --- КОМАНДА /AI_SETTINGS ---
      if (text === "/ai_settings" && isAdmin) {
        let msg = "🧠 Настройки ИИ-моделей\n";
        for (const [type, info] of Object.entries(SERVICE_TYPE_MAP)) {
          const modelKey = await env.USER_DB.get(info.kvKey) || Object.keys(AI_MODELS).find(k => k.startsWith(type)) || "—";
          const modelName = AI_MODELS[modelKey]?.MODEL || "—";
          msg += `\n${info.name}: ${modelName}`;
        }
        await sendVKMessage(chatId, msg, env);
        return new Response("OK");
      }

      // --- КОМАНДА /ADD ---
      if (text.startsWith("/add") && isAdmin) {
        const targetId = text.split(" ")[1];
        if (!targetId) {
          await sendVKMessage(chatId, "⚠️ Формат: /add [ID]", env);
          return new Response("OK");
        }
        const myData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
        if (!myData) {
          await sendVKMessage(chatId, "❌ Сначала авторизуй свой диск!", env);
          return new Response("OK");
        }
        // Копируем твои данные целевому пользователю
        await env.USER_DB.put(`user:${targetId}`, JSON.stringify({
          provider: myData.provider,
          access_token: myData.access_token,
          folderId: "Storage"
        }));
        await sendVKMessage(chatId, `✅ Пользователь ${targetId} инициализирован на ${myData.provider}`, env);
        return new Response("OK");
      }

      // --- КОМАНДА /DEBUG ---
      if (text === "/debug") {
        let debugInfo = `🔧 DEBUG INFO\nUser ID: ${userId}\nIs Admin: ${isAdmin}\nHostname: ${hostname}`;
        if (userData) {
          debugInfo += `\nProvider: ${userData.provider || '—'}\nFolder: ${userData.folderId || 'Root'}\nShared from: ${userData.shared_from || '—'}`;
        } else {
          debugInfo += "\nUser data: not found";
        }
        await sendVKMessage(chatId, debugInfo, env);
        return new Response("OK");
      }

      // --- Общая проверка доступа ---
      const hasAccess = isAdmin || (userData && (userData.access_token || userData.provider === 'webdav' || userData.shared_from));
      if (!hasAccess && !text.startsWith("/")) {
        await sendVKMessage(chatId, "🚫 Доступ ограничен.\nУ тебя не подключено облако и нет активной ссылки от друга.", env);
        return new Response("OK");
      }

      // --- КОМАНДА /SHARE ---
      if (text.startsWith("/share")) {
        if (userData.is_ref) {
          await sendVKMessage(chatId, "⚠️ Ты используешь чужой диск и не можешь создавать свои реф-ссылки.", env);
          return new Response("OK");
        }
        const currentFolder = userData?.folderId || "Root";
        const inviteToken = Math.random().toString(36).substring(2, 12);
        const inviteData = {
          inviterId: userId,
          provider: userData.provider,
          token: inviteToken,
          folderId: currentFolder,
          timestamp: Date.now()
        };
        await env.USER_DB.put(`invite:${inviteToken}`, JSON.stringify(inviteData));
        const botName = env.BOT_USERNAME || "leshiy_storage_bot";
        const inviteLink = `https://t.me/${botName}?start=ref_${inviteToken}`;
        await sendVKMessage(chatId, `🚀 Твоя ссылка для друга:\n${inviteLink}\n📁 Папка: ${currentFolder}\nОблако: ${userData.provider}`, env);
        return new Response("OK");
      }

      // --- КОМАНДА /DISCONNECT ---
      if (text === "/disconnect") {
        await env.USER_DB.delete(userKey);
        await sendVKMessage(chatId, "🔌 Диск отключен.", env);
        return new Response("OK");
      }

      // --- КОМАНДА /FOLDER ---
      if (text === "/folder") {
        if (!userData || !userData.provider) {
          await sendVKMessage(chatId, "❌ Сначала подключи облако (/start).", env);
          return new Response("OK");
        }

        await sendVKMessage(chatId, "📂 Получаю список папок...", env);
        let folders = [];
        try {
          if (userData.provider === "google") {
            folders = await listGoogleFolders(userData.access_token);
          } else if (userData.provider === "yandex") {
            folders = await listYandexFolders(userData.access_token);
          } else if (userData.provider === "dropbox") {
            folders = await listDropboxFolders(userData.access_token);
          }
        } catch (e) {
          await sendVKMessage(chatId, `❌ Ошибка: ${e.message}`, env);
          return new Response("OK");
        }

        if (folders.length === 0) {
          await sendVKMessage(chatId, "📁 Папок не найдено. Отправь:\n`/create Storage` — чтобы создать папку 'Storage'", env);
          await env.USER_DB.put(`state:${userId}`, "wait_create_folder");
          return new Response("OK");
        }

        let list = "📂 Доступные папки:\n";
        folders.slice(0, 20).forEach((f, i) => {
          list += `${i + 1}. ${f.name}\n`;
        });
        list += "\nОтветь номером папки (например: `2`) или:\n`/create Имя` — создать новую папку";
        await env.USER_DB.put(`state:${userId}`, "wait_folder_choice");
        await env.USER_DB.put(`temp_folders:${userId}`, JSON.stringify(folders));
        await sendVKMessage(chatId, list, env);
        return new Response("OK");
      }

      // --- КОМАНДА /SEARCH ---
      if (text.startsWith("/search")) {
        if (!userData || (!userData.provider && !userData.shared_from)) {
          await sendVKMessage(chatId, "⚠️ Поиск недоступен.\nТвоё хранилище не подключено.", env);
          return new Response("OK");
        }
        const query = text.replace(/^\/search\s*/i, '').trim();
        if (!query) {
          await env.USER_DB.put(`state:${userId}`, "waiting_for_search", { expirationTtl: 300 });
          await sendVKMessage(chatId, "🔎 Поиск по архиву\nПришли мне название файла или его часть.\nПримеры: \"сейф\", \"jpg\", \"2025\"\n🔹 Ищу только по именам файлов.\n🔹 Поиск не чувствителен к регистру.\n👇 Просто напиши, что искать:", env);
          return new Response("OK");
        }

        await sendVKMessage(chatId, "⏳ Выполняю поиск файлов...", env);
        const isAIQuery = query.includes(" ") && isAdmin;
        let searchResult;
        if (isAIQuery) {
          searchResult = await searchAIFilesByQuery(userId, isAdmin, query, env);
        } else {
          searchResult = await searchFilesByQuery(userId, isAdmin, query, env);
        }

        if (!searchResult.success || searchResult.fileIds.length === 0) {
          await sendVKMessage(chatId, `❌ По запросу "${query}" ничего не найдено.`, env);
          return new Response("OK");
        }

        let list = `🔍 Найдено всего: ${searchResult.fileIds.length}\n`;
        for (const id of searchResult.fileIds.slice(0, 5)) {
          const f = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(id).first();
          list += `• ${f?.fileName || 'Файл'}\n`;
        }
        await sendVKMessage(chatId, list, env);
        return new Response("OK");
      }

      // --- КОМАНДА /AI_SEARCH ---
      if (text.startsWith("/ai_search") && isAdmin) {
        const query = text.replace(/^\/ai_search\s*/i, '').trim();
        if (!query) {
          await sendVKMessage(chatId, "🔎 Что ищем с помощью ИИ?", env);
          return new Response("OK");
        }
        await sendVKMessage(chatId, "⏳ Выполняю интеллектуальный поиск...", env);
        const searchResult = await searchAIFilesByQuery(userId, isAdmin, query, env);
        if (!searchResult.success || searchResult.fileIds.length === 0) {
          await sendVKMessage(chatId, "🔍 По вашему запросу ничего не найдено.", env);
          return new Response("OK");
        }
        let list = `🧠 Найдено (ИИ): ${searchResult.fileIds.length}\n`;
        for (const id of searchResult.fileIds.slice(0, 5)) {
          const f = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(id).first();
          list += `• ${f?.fileName || 'Файл'}\n`;
        }
        await sendVKMessage(chatId, list, env);
        return new Response("OK");
      }

      // --- ОБРАБОТКА ФАЙЛОВ ---
      if (message.attachments && message.attachments.length > 0) {
        await sendVKMessage(chatId, "⏳ Начинаю загрузку в облако...", env);
        try {
          for (const attachment of message.attachments) {
            let fileObj = null;
            let fileName = "";
            let fType = "document";

            if (attachment.type === "photo") {
              fileObj = attachment.photo;
              fType = "photo";
              fileName = `Photo_${Date.now()}.jpg`;
            } else if (attachment.type === "video") {
              // Видео из VK нельзя скачать напрямую → пропускаем
              await sendVKMessage(chatId, "⚠️ Загрузка видео из VK временно недоступна. Используй Telegram.", env);
              continue;
            } else if (attachment.type === "audio") {
              fileObj = attachment.audio;
              fType = "audio";
              fileName = attachment.audio.title || `Audio_${Date.now()}.mp3`;
            } else if (attachment.type === "doc") {
              fileObj = attachment.doc;
              fType = "document";
              fileName = fileObj.title || `file_${Date.now()}.pdf`;
            }

            if (!fileObj || !fileObj.url) continue;

            const arrayBuffer = await fetch(fileObj.url).then(res => res.arrayBuffer());

            let success = false;
            if (userData.provider === "google") {
              success = await uploadToGoogleFromArrayBuffer(arrayBuffer, fileName, userData.access_token, userData.folderId || "root");
            } else if (userData.provider === "yandex") {
              success = await uploadToYandexFromArrayBuffer(arrayBuffer, fileName, userData.access_token, userData.folderId || "");
            } else if (userData.provider === "dropbox") {
              success = await uploadToDropboxFromArrayBuffer(arrayBuffer, fileName, userData.access_token, userData.folderId || "Storage");
            } else if (userData.provider === "webdav") {
              success = await uploadWebDAVFromArrayBuffer(arrayBuffer, fileName, userData, env);
            }

            if (success) {
              await env.FILES_DB.prepare(
                "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
              ).bind(String(userId), fileName, fileObj.id, fType, userData.provider, userData.folderId || "Root", Date.now()).run();

              if (["photo", "audio", "document"].includes(fType)) {
                ctx.waitUntil((async () => {
                  try {
                    let description;
                    const modelConfig = await loadActiveConfig(`${fType.toUpperCase()}_TO_TEXT`, env);
                    description = await modelConfig.FUNCTION(modelConfig, arrayBuffer, env);
                    await env.FILES_DB.prepare("UPDATE files SET ai_description = ? WHERE userId = ? AND fileName = ?")
                      .bind(description, String(userId), fileName).run();
                  } catch (e) {
                    await logDebug(`⚠️ Ошибка генерации описания: ${e.message}`, env);
                  }
                })());
              }

              await sendVKMessage(chatId, `✅ Файл ${fileName} сохранен в ${userData.provider}!`, env);
              return new Response("OK");
            } else {
              await sendVKMessage(chatId, "❌ Ошибка при загрузке. Проверьте токены или место на диске.", env);
              return new Response("OK");
            }
          }
        } catch (e) {
          await sendVKMessage(chatId, `❌ Ошибка: ${e.message}`, env);
          return new Response("OK");
        }
      }

      // Обработка выбора папки
      const userState = await env.USER_DB.get(`state:${userId}`);
      if (userState === "wait_folder_choice") {
        const folders = await env.USER_DB.get(`temp_folders:${userId}`, { type: "json" });
        if (!folders) {
          await sendVKMessage(chatId, "❌ Сессия выбора папки устарела. Повтори /folder.", env);
          return new Response("OK");
        }

        const input = text.trim();
        if (input.startsWith("/create ")) {
          const folderName = input.replace("/create ", "").trim();
          if (!folderName) {
            await sendVKMessage(chatId, "🔤 Укажи имя папки: `/create Имя`", env);
            return new Response("OK");
          }
          // Создаём папку
          let success = false;
          if (userData.provider === "google") {
            success = await createGoogleFolder(folderName, userData.access_token);
          } else if (userData.provider === "yandex") {
            success = await createYandexFolder(folderName, userData.access_token);
          } else if (userData.provider === "dropbox") {
            success = await createDropboxFolder(folderName, userData.access_token);
          }

          if (success) {
            userData.folderId = folderName;
            await env.USER_DB.put(`user:${userId}`, JSON.stringify(userData));
            await sendVKMessage(chatId, `✅ Папка "${folderName}" создана и выбрана!`, env);
          } else {
            await sendVKMessage(chatId, "❌ Не удалось создать папку.", env);
          }
          await env.USER_DB.delete(`temp_folders:${userId}`);
          await env.USER_DB.delete(`state:${userId}`);
          return new Response("OK");
        }

        const num = parseInt(input);
        if (isNaN(num) || num < 1 || num > folders.length) {
          await sendVKMessage(chatId, "🔢 Введи корректный номер папки или `/create Имя`", env);
          return new Response("OK");
        }

        const selected = folders[num - 1];
        userData.folderId = userData.provider === "google" ? selected.id : selected.name;
        await env.USER_DB.put(`user:${userId}`, JSON.stringify(userData));
        await sendVKMessage(chatId, `📂 Выбрана папка: ${selected.name}`, env);
        await env.USER_DB.delete(`temp_folders:${userId}`);
        await env.USER_DB.delete(`state:${userId}`);
        return new Response("OK");
      }

      // Состояние: ожидание текста для поиска
    if (userState === "waiting_for_search" && !text.startsWith("/")) {
      await env.USER_DB.delete(`state:${userId}`);
      const query = text.trim();
      await sendVKMessage(chatId, `🔍 Ищу: "${query}"...`, env);

      const isAIQuery = query.includes(" ") && isAdmin;
      let searchResult;
      if (isAIQuery) {
        searchResult = await searchAIFilesByQuery(userId, isAdmin, query, env);
      } else {
        searchResult = await searchFilesByQuery(userId, isAdmin, query, env);
      }

      if (!searchResult.success || searchResult.fileIds.length === 0) {
        await sendVKMessage(chatId, `❌ Ничего не найдено по запросу "${query}".`, env);
        return new Response("OK");
      }

      // Отображаем результаты (как в Telegram)
      let list = `🔍 Найдено: ${searchResult.fileIds.length}\n`;
      for (const id of searchResult.fileIds.slice(0, 5)) {
        const f = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(id).first();
        list += `• ${f?.fileName || 'Файл'}\n`;
      }
      await sendVKMessage(chatId, list, env);
      return new Response("OK");
    }

      // --- ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ (чат с ИИ) ---
      if (text && userData) {
        try {
          const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
          const responseText = await handleChatRequest(text, modelConfig, env);
          await sendVKMessage(chatId, responseText.substring(0, 4000), env);
        } catch (e) {
          console.error("AI Error:", e);
        }
        return new Response("OK");
      }
    }
  } catch (e) {
    console.error("VK Error:", e);
    if (chatId) {
      await sendVKMessage(chatId, `❌ Критическая ошибка: ${e.message}`, env);
    }
  }
  return new Response("OK");
}

/**
 * Универсальная обёртка для обработки чат-запросов.
 * Применяет чатовую инструкцию и вызывает соответствующую функцию модели.
 * @param {string} userPrompt - Сообщение пользователя.
 * @param {Object} modelConfig - Конфигурация активной модели (из AI_MODELS).
 * @param {Object} env - Окружение.
 * @returns {Promise<string>} Ответ от ИИ.
 */
async function handleChatRequest(userPrompt, modelConfig, env) {
  // --- 1. ФОРМИРУЕМ ЧАТОВУЮ ИНСТРУКЦИЮ (та же, что и в функциях) ---
  const CHAT_INSTRUCTION = `🤖 ТЫ — многофункциональный AI-ассистент "Gemini AI" от Leshiy, отвечающий на русском языке.
Твоя задача — вести диалог, отвечать на вопросы, соблюдая контекст и используя информацию о твоих функциях.

СТРОГОЕ ПРАВИЛО: НИКОГДА НЕ УПОМИНАЙ LLaMA, Meta AI или Austin.

Твои ключевые функции:
✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Google, Яндекс.Диск, Облако Mail.Ru WebDAV и др.) прямо через телеграмм. 
Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку (команда /share) формирует ссылку с токеном.
Универсальность: Поддержка облачного хранилища с авторизацией OAuth (Google, Яндекс.Диск, DropBox) и WebDAV (Облако Mail.Ru и др.)
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео/аудио/документов.
💬 Чат: Ты ведешь диалог, отвечаешь на вопросы, ❔ помогаешь по менюшкам и окнам и сохраняешь контекст беседы.

Когда пользователь спрашивает, что ты умеешь, обязательно упомяни о своих навыках.
Ответы должны быть информативными и доброжелательными и по возможности компактными, старайся построить диалог понятно и не сильно рассуждая.`.trim();

  // --- 2. ФОРМИРУЕМ ФИНАЛЬНЫЙ ПРОМПТ ---
  // Workers AI и Bothub используют историю в формате messages, но Gemini — нет.
  // Для простоты и совместимости используем один общий формат промпта.
  const finalPrompt = `${CHAT_INSTRUCTION}\n\nВопрос пользователя: ${userPrompt}`;

  // --- 3. ВЫЗЫВАЕМ СООТВЕТСТВУЮЩУЮ ФУНКЦИЮ ---
  // Все существующие функции принимают (prompt, config, env, userMessageText)
  // Мы передаём userPrompt как 4-й аргумент для совместимости.
  //return await modelConfig.FUNCTION(finalPrompt, modelConfig, env, userPrompt);
  if (modelConfig.SERVICE === 'WORKERS_AI') {
    // Для Workers AI передаём отдельно system и user
    return await modelConfig.FUNCTION(CHAT_INSTRUCTION, modelConfig, env, userPrompt);
  } else {
    // Для Gemini/Bothub — один общий промпт
    const finalPrompt = `${CHAT_INSTRUCTION}\n\nВопрос пользователя: ${userPrompt}`;
    return await modelConfig.FUNCTION(finalPrompt, modelConfig, env, userPrompt);
  }
}

/**
 * Универсальная обёртка для обработки запросов ИНТЕЛЛЕКТУАЛЬНОГО ПОИСКА.
 * Применяет ПОИСКОВУЮ инструкцию и вызывает соответствующую функцию модели.
 * @param {string} searchTask - Задача для ИИ (список кандидатов, запрос пользователя).
 * @param {Object} modelConfig - Конфигурация активной модели (из AI_MODELS).
 * @param {Object} env - Окружение.
 * @returns {Promise<string>} Ответ от ИИ (должен содержать ID файлов).
 */
/**
 * Универсальная обёртка для обработки запросов ИНТЕЛЛЕКТУАЛЬНОГО ПОИСКА.
 * Применяет ПОИСКОВУЮ инструкцию и вызывает соответствующую функцию модели.
 * @param {string} searchTask - Задача для ИИ (список кандидатов, запрос пользователя).
 * @param {Object} modelConfig - Конфигурация активной модели (из AI_MODELS).
 * @param {Object} env - Окружение.
 * @returns {Promise<string>} Ответ от ИИ (должен содержать ID файлов).
 */
async function handleSearchRequest(searchTask, modelConfig, env) {
  // --- ЕДИНАЯ ПОИСКОВАЯ ИНСТРУКЦИЯ ДЛЯ ВСЕХ МОДЕЛЕЙ ---
  const SEARCH_INSTRUCTION = `Ты — эксперт по релевантности файлов.
Твоя задача — проанализировать список кандидатов и выбрать самые подходящие под запрос пользователя.
ИНСТРУКЦИЯ: Верни ТОЛЬКО список ID подходящих файлов через запятую (например: 1,5,12). 
НЕ пиши приветствий, пояснений, комментариев, скобок, кода или любого другого текста.
Если ничего не подходит, верни "0".`;

  const finalPrompt = `${SEARCH_INSTRUCTION}\n\n${searchTask}`;
  
  // --- ВЫЗЫВАЕМ ФУНКЦИЮ МОДЕЛИ ---
  return await modelConfig.FUNCTION(finalPrompt, modelConfig, env, searchTask);
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

/**
 * Генерирует клавиатуру для выбора модели.
 * @param {Object} env - Окружение.
 * @param {string} serviceType - Тип сервиса.
 * @returns {Promise<Array>} Массив кнопок.
 */
async function getModelMenuKeyboard(env, serviceType) {
  const service = AI_MODEL_MENU_CONFIG[serviceType];
  if (!service) return [];

  const currentModelKey = await env.USER_DB.get(service.kvKey) || Object.keys(service.models)[0];
  const buttons = Object.entries(service.models).map(([key, name]) => [
    {
      text: (key === currentModelKey ? "✅ " : "") + name,
      callback_data: `admin_model_set_${serviceType};${key}`
    }
  ]);

  // Кнопки переключения сервиса
  const switchButtons = Object.entries(SERVICE_TYPE_MAP).map(([type, info]) => ({
    text: type === serviceType ? `● ${info.name}` : `○ ${info.name}`,
    callback_data: `admin_model_show_${type}`
  }));

  // Группируем по 2 кнопки в строке
  const groupedSwitch = [];
  for (let i = 0; i < switchButtons.length; i += 2) {
    groupedSwitch.push(switchButtons.slice(i, i + 2));
  }

  return [...groupedSwitch, ...buttons, [{ text: "⬅️ Назад", callback_data: "ai_menu_main" }]];
}

/**
 * Генерирует клавиатуру главного меню — выбор типа сервиса.
 * @returns {Array} Кнопки выбора сервиса.
 */
function getAIServiceMenuKeyboard() {
  return Object.entries(SERVICE_TYPE_MAP).map(([type, info]) => [
    { text: info.name, callback_data: `ai_menu:${type}` }
  ]);
}

async function renderSearchPage(searchKey, offset, env, userId) {
  const dataRaw = await env.USER_DB.get(searchKey);
  if (!dataRaw) return { text: "❌ Поиск устарел или не найден.", kb: null };
  
  const searchData = JSON.parse(dataRaw);
  const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });

  const total = searchData.ids.length;
  const pageIds = searchData.ids.slice(offset, offset + 5);
  
  let list = `🔍 <b>Найдено всего: ${total}</b> (Страница ${Math.floor(offset/5) + 1})\n\n`;
  const userFolder = userData?.folderId || "/";

  for (const id of pageIds) {
    const f = await env.FILES_DB.prepare("SELECT fileName, provider, remotePath FROM files WHERE id = ?").bind(id).first();
    // ПРОВЕРКА: провайдер совпадает И путь файла соответствует папке юзера
    const isProviderOk = userData && f?.provider === userData.provider;
    const isPathOk = f?.remotePath ? f.remotePath === userFolder : false;
    
    const status = (isProviderOk && isPathOk) ? '🟢' : '🔴';
    list += `${status} <code>${f?.fileName || 'Файл'}</code>\n`;
  }

  list += `\nАктивное подключение:`;
  list += `\n<b>🔌 Провайдер: ${userData?.provider}</b> 📁 Папка: ${userData?.folderId}`;
  list += `\n<b>🟢 доступно</b> | <b>🔴 не доступно</b> для выгрузки`;
  //list += `\n<b>🔴 не доступно</b>, смените диск`;

  // Формат кнопок сокращаем до предела: pg:KEY:OFFSET и dl:KEY:OFFSET
  const kb = { inline_keyboard: [
    [{ text: "📥 Выгрузить эти файлы", callback_data: `dl:${searchKey}:${offset}` },
    { text: "🔎 Изменить поиск", callback_data: "search_retry" }],
    [] 
  ]};

  if (offset > 0) {
    kb.inline_keyboard[1].push({ text: `⬅️ стр. ${Math.floor(offset/5) + 0}`, callback_data: `pg:${searchKey}:${offset - 5}` });
  }
  if (offset + 5 < total) {
    kb.inline_keyboard[1].push({ text: `⬆️ стр. ${Math.floor(offset/5) + 1}`, callback_data: `dummy_ignore` });
  }
  if (offset + 5 < total) {
    kb.inline_keyboard[1].push({ text: `стр. ${Math.floor(offset/5) + 2} ➡️`, callback_data: `pg:${searchKey}:${offset + 5}` });
  }
  return { text: list, kb };
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
  const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
  const parts = data.split(":");
  const action = parts[0];
  //const targetUserId = parts[1] || userId; // Для команды /add или личного использования
  //const folderIdOrName = parts[parts.length - 1]; 

  try {
    // --- ПАГИНАЦИЯ (Стрелочки) ---
    if (data.startsWith("pg:")) {
      const [_, prefix, uId, sId, offset] = data.split(":");
      const key = `${prefix}:${uId}:${sId}`; // Собираем s:userId:shortId
      
      const { text: newText, kb } = await renderSearchPage(key, parseInt(offset), env, userId);

      await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/editMessageText`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          message_id: query.message.message_id,
          text: newText,
          parse_mode: "HTML",
          reply_markup: kb
        })
      });
    }

    // --- ВЫГРУЗКА (dl:) ---
    if (data.startsWith("dl:")) {
      const parts = data.split(":");
      // Собираем ключ обратно: s : userId : shortId
      const key = `${parts[1]}:${parts[2]}:${parts[3]}`;
      const offset = parts[4] || "0";

      const dataRaw = await env.USER_DB.get(key);
      const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });

      if (!dataRaw || !userData) {
        return await sendMessage(chatId, "❌ Ошибка: данные поиска не найдены.", null, env);
      }

      const searchData = JSON.parse(dataRaw);
      const toDl = searchData.ids.slice(parseInt(offset), parseInt(offset) + 5);

      await sendMessage(chatId, `⏳ Начинаю выгрузку ${toDl.length} файл(ов)...`, null, env);

      for (const fileId of toDl) {
        try {
          const file = await env.FILES_DB.prepare("SELECT * FROM files WHERE id = ?").bind(fileId).first();
          
          // Проверка провайдера (чтобы не пытаться качать с яндекса, если выбран дропбокс)
          if (!file || file.provider !== userData.provider) continue;

          let downloadUrl = "";
          
          const headers = new Headers();

          if (file.provider === 'webdav') {
            downloadUrl = `${userData.host}/${file.remotePath}/${file.fileName}`.replace(/([^:])\/\//g, '$1/');
            headers.set("Authorization", "Basic " + btoa(userData.user + ":" + userData.pass));
          } else if (file.provider === 'yandex') {
            const yaRes = await fetch(`https://cloud-api.yandex.net/v1/disk/resources/download?path=${encodeURIComponent(file.remotePath + "/" + file.fileName)}`, {
              headers: { "Authorization": "OAuth " + userData.access_token }
            });
            const yaData = await yaRes.json();
            downloadUrl = yaData.href;
          } else if (file.provider === 'dropbox') {
            try {
              // 1. Формируем правильный путь (Dropbox требует / в начале)
              const fullPath = (file.remotePath + "/" + file.fileName).replace(/\/+/g, '/');
              const path = fullPath.startsWith('/') ? fullPath : '/' + fullPath;

              // 2. Получаем временную ссылку
              const dbxRes = await fetch("https://api.dropboxapi.com/2/files/get_temporary_link", {
                method: "POST",
                headers: { 
                  "Authorization": "Bearer " + userData.access_token, 
                  "Content-Type": "application/json" 
                },
                body: JSON.stringify({ path: path })
              });

              const dbxData = await dbxRes.json();

              if (dbxData.link) {
                // 3. Качаем файл по ссылке
                const fileResp = await fetch(dbxData.link);
                const dbxBuffer = await fileResp.arrayBuffer();

                if (dbxBuffer && dbxBuffer.byteLength > 0) {
                  const formData = new FormData();
                  formData.append('chat_id', String(chatId));
                  
                  // Шлем как документ (самый надежный вариант)
                  formData.append('document', new Blob([dbxBuffer]), file.fileName);
                  
                  const tgRes = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendDocument`, {
                    method: 'POST',
                    body: formData
                  });

                  if (tgRes.ok) {
                    await logDebug(`✅ Dropbox отправил: ${file.fileName}`, env);
                  } else {
                    await logDebug(`❌ Ошибка ТГ (Dropbox): ${await tgRes.text()}`, env);
                  }
                }
              } else {
                await logDebug(`❌ Dropbox не дал ссылку. Ошибка: ${JSON.stringify(dbxData)}`, env);
              }
            } catch (e) {
              await logDebug(`❌ Крит. ошибка Dropbox: ${e.message}`, env);
            }
            continue; // Важно: уходим на следующий файл

          } else if (file.provider === 'google') {
            const gBuffer = await downloadFromGoogle(file.remotePath, file.fileName, userData.access_token, env);
            
            if (gBuffer && gBuffer.byteLength > 0) {
              const formData = new FormData();
              formData.append('chat_id', String(chatId));
          
              const ext = file.fileName.toLowerCase().split('.').pop();
              let method = 'sendDocument';
              let typeKey = 'document';
          
              // Мапим расширения на методы Telegram
              if (['jpg', 'jpeg', 'png'].includes(ext)) { method = 'sendPhoto'; typeKey = 'photo'; }
              else if (['mp4', 'mov'].includes(ext)) { method = 'sendVideo'; typeKey = 'video'; }
              else if (['ogg', 'opus'].includes(ext)) { method = 'sendVoice'; typeKey = 'voice'; } 
              else if (['mp3', 'wav'].includes(ext)) { method = 'sendAudio'; typeKey = 'audio'; }
          
              formData.append(typeKey, new Blob([gBuffer]), file.fileName);
              
              await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/${method}`, {
                method: 'POST',
                body: formData
              });
              
              await logDebug(`✅ Отправлен: ${file.fileName}`, env);
            }
            continue; 
          }

          if (!downloadUrl) continue;

          // --- СКАЧИВАНИЕ И ОТПРАВКА В TG ---
          const fileResp = await fetch(downloadUrl, { headers: headers });
          if (!fileResp.ok) continue;
          
          const fileBuffer = await fileResp.arrayBuffer();
          const formData = new FormData();
          formData.append('chat_id', String(chatId));

          let method = 'sendDocument';
          let typeKey = 'document';
          if (file.fileType === 'photo') { method = 'sendPhoto'; typeKey = 'photo'; }
          else if (file.fileType === 'video') { method = 'sendVideo'; typeKey = 'video'; }

          formData.append(typeKey, new Blob([fileBuffer]), file.fileName);
          
          await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/${method}`, {
            method: 'POST',
            body: formData
          });

        } catch (e) {
          console.error("Ошибка при выгрузке конкретного файла:", e);
        }
      }

      await sendMessage(chatId, "✅ Готово!", null, env);
      return new Response("OK");
    }
    // --- 1. ВЫГРУЗКА (deliver_files) ---
    if (data.startsWith("deliver_files:")) {
      const parts = data.split(":");
      // ПРОВЕРКА: Если в ключе 4 части (старый формат) или 5 (новый)
      // Собираем ключ: это части с индексами 1, 2, 3 (search:userId:timestamp)
      const searchKey = `${parts[1]}:${parts[2]}:${parts[3]}`;
      const mode = parts[4] || "0"; 

      const searchData = await env.USER_DB.get(searchKey, { type: "json" });
      const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });

      if (!searchData) return await sendMessage(chatId, `❌ Ошибка: Ключ ${searchKey} не найден в базе.`, null, env);
      if (!userData) return await sendMessage(chatId, "❌ Ошибка: Профиль пользователя не найден.", null, env);
        
      let filesToDownload = (mode === "all") 
      ? searchData.fileIds 
      : searchData.fileIds.slice(parseInt(mode), parseInt(mode) + 5);

      if (mode === "all") {
        filesToDownload = searchData.fileIds;
      } else {
        const start = parseInt(mode);
        filesToDownload = searchData.fileIds.slice(start, start + 5);
      }
  
      await sendMessage(chatId, `⏳ Начинаю выгрузку файлов (${filesToDownload.length})...`, null, env); 
  
      let successCount = 0;
      let failCount = 0;
  
      for (const fileId of filesToDownload) {
        try {
          const file = await env.FILES_DB.prepare("SELECT * FROM files WHERE id = ?").bind(fileId).first();
          if (!file || file.provider !== userData.provider) {
            failCount++;
            continue;
          }
  
          let downloadUrl = "";
          const headers = new Headers();
  
          // Формирование ссылки
          if (file.provider === 'webdav') {
            downloadUrl = `${userData.host}/${file.remotePath}/${file.fileName}`.replace(/([^:])\/\//g, '$1/');
            headers.set("Authorization", "Basic " + btoa(userData.user + ":" + userData.pass));
          } else if (file.provider === 'yandex') {
            const yaRes = await fetch(`https://cloud-api.yandex.net/v1/disk/resources/download?path=${encodeURIComponent(file.remotePath + "/" + file.fileName)}`, {
              headers: { "Authorization": "OAuth " + userData.access_token }
            });
            const yaData = await yaRes.json();
            downloadUrl = yaData.href;
          } else if (file.provider === 'google') {
            const gBuffer = await downloadFromGoogle(file.remotePath, file.fileName, userData.access_token, env);
            
            if (gBuffer && gBuffer.byteLength > 0) {
              const formData = new FormData();
              formData.append('chat_id', String(chatId));

              formData.append('document', new Blob([gBuffer]), file.fileName);
              
              await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendDocument`, {
                method: 'POST',
                body: formData
              });
              await logDebug(`✅ Отправлен: ${file.fileName}`, env);
            }
            continue; 
          }
  
          if (!downloadUrl) {
            failCount++;
            continue;
          }
  
          // Скачиваем файл во временный буфер
          const fileResp = await fetch(downloadUrl, { headers: headers });
          if (!fileResp.ok) {
            failCount++;
            continue;
          }
          
          const fileBuffer = await fileResp.arrayBuffer();
          const formData = new FormData();
          formData.append('chat_id', String(chatId));
  
          let method = 'sendDocument';
          let typeKey = 'document';
          if (file.fileType === 'photo') { method = 'sendPhoto'; typeKey = 'photo'; }
          else if (file.fileType === 'video') { method = 'sendVideo'; typeKey = 'video'; }
  
          // Ключевой момент: создаем Blob с правильным типом
          formData.append(typeKey, new Blob([fileBuffer]), file.fileName);
  
          // Отправка в Telegram с проверкой ответа
          const tgRes = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/${method}`, {
            method: 'POST',
            body: formData
          });
  
          if (tgRes.ok) {
            successCount++;
          } else {
            const errText = await tgRes.text();
            console.error("TG Error:", errText);
            failCount++;
          }
  
        } catch (e) { 
          console.error("Critical Error:", e); 
          failCount++;
        }
      }
  
      const finalMsg = successCount > 0 
        ? `✅ Успешно выгружено: ${successCount}\n❌ Ошибок: ${failCount}` 
        : `❌ Не удалось выгрузить ни одного файла. Ошибок: ${failCount}`;
      
      await sendMessage(chatId, finalMsg, null, env);
      return new Response("OK");
    }

  // --- 2. ПОКАЗАТЬ ЕЩЁ (show_more_search) ---
  if (data.startsWith("show_more_search:")) {
    const parts = data.split(":");
    // формат: show_more_search : search : userId : timestamp : offset
    const searchKey = `${parts[1]}:${parts[2]}:${parts[3]}`;
    const offset = parseInt(parts[4] || "5");

    const searchData = await env.USER_DB.get(searchKey, { type: "json" });
    const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });

    if (!searchData) return await sendMessage(chatId, "❌ Поиск устарел.", null, env);

    const nextBatch = searchData.fileIds.slice(offset, offset + 5);
    if (nextBatch.length === 0) return sendMessage(chatId, "🏁 Больше файлов нет.", null, env);

    let list = "";
    for (const id of nextBatch) {
      const f = await env.FILES_DB.prepare("SELECT fileName, provider FROM files WHERE id = ?").bind(id).first();
      if (f) {
        const status = (userData && f.provider === userData.provider) ? '🟢' : '🔴';
        list += `${status} <code>${f.fileName}</code>\n`;
      }
    }

    const nextOffset = offset + 5;
    const kb = { 
      inline_keyboard: [
        // Кнопка выгрузки именно ЭТОЙ пачки (теперь передаем смещение)
        [{ text: `📥 Выгрузить эти 5 файлов`, callback_data: `deliver_files:${searchKey}:${offset}` }]
      ] 
    };
    
    if (searchData.fileIds.length > nextOffset) {
      kb.inline_keyboard.push([{ text: "➡️ Еще 5", callback_data: `show_more_search:${searchKey}:${nextOffset}` }]);
    }

    await sendMessage(chatId, `🔍 <b>Результаты ${offset + 1}-${offset + nextBatch.length}:</b>\n\n${list}`, kb, env);
    return new Response("OK");
  }

  // --- 3. ПОКАЗАТЬ ВСЁ (show_all_search) ---
  if (data.startsWith("show_all_search:")) {
    const parts = data.split(":");
    const searchKey = `${parts[1]}:${parts[2]}:${parts[3]}`;
    
    const searchData = await env.USER_DB.get(searchKey, { type: "json" });
    const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });

    if (!searchData) return await sendMessage(chatId, "❌ Поиск устарел.", null, env);
  
    let allList = "📑 <b>Полный список результатов:</b>\n\n";
    for (const id of searchData.fileIds.slice(0, 50)) { // Лимит 50 чтобы не упасть
      const f = await env.FILES_DB.prepare("SELECT fileName, provider FROM files WHERE id = ?").bind(id).first();
      if (f) {
        const status = (userData && f.provider === userData.provider) ? '🟢' : '🔴';
        allList += `${status} <code>${f.fileName}</code>\n`;
      }
    }
    
    const kb = {
      inline_keyboard: [[{ text: "📥 Выгрузить ВООБЩЕ ВСЁ", callback_data: `deliver_files:${searchKey}:all` }]]
    };

    await sendMessage(chatId, allList, kb, env);
    return new Response("OK");
  }

  if (data === "search_retry") {
    // Ставим стейт ожидания заново
    await env.USER_DB.put(`state:${userId}`, "waiting_for_search", { expirationTtl: 300 });
  
    const retryMsg = `🔎 <b>Новый поиск</b>\n\nВведите название файла или тег:`;
    
    // Отвечаем на колбэк и отправляем новое сообщение (или редактируем старое)
    await sendMessage(chatId, retryMsg, null, env);
    return new Response("OK");
  }

    if (action === "manual_folder") {
      await env.USER_DB.put(`state:${userId}`, "wait_manual_folder");
      await sendMessage(chatId, "🔤 Напиши название папки (например: <code>Storage</code>):", null, env);
      return new Response("OK");
    }

    if (action === "create_folder") {
      let finalId;
      let success = false;
      const targetUserId = parts[1] || userId; // Для команды /add или личного использования
      const folderIdOrName = parts[parts.length - 1]; 
      const userData = await env.USER_DB.get(`user:${targetUserId}`, { type: "json" });
      if (!userData) return new Response("OK");
  
      if (userData.provider === "google") {
        finalId = await createGoogleFolder(folderIdOrName, userData.access_token);
      } else if (userData.provider === "yandex") {
        success = await createYandexFolder(folderIdOrName, userData.access_token);
      } else if (userData.provider === "mailru") {
        // Вызываем создание папки для Mail.ru
        success = await createMailruFolder(folderIdOrName, userData.access_token, env);
      } else if (userData.provider === "dropbox") {
        success = await createDropboxFolder(folderIdOrName, userData.access_token);
      } else if (userData.provider === "webdav") {
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
      const folderIdOrName = parts[parts.length - 1]; 
      const targetUserId = parts[1] || userId; // Для команды /add или личного использования
      const userData = await env.USER_DB.get(`user:${targetUserId}`, { type: "json" });
      if (!userData) return new Response("OK");
      userData.folderId = folderIdOrName;
      
      await env.USER_DB.put(`user:${targetUserId}`, JSON.stringify(userData));
      await sendMessage(chatId, `📂 Папка выбрана: <b>${folderIdOrName}</b>`, null, env);
    }
    
    if (action === "admin_exit") {
      return await sendMessage(chatId, `🚪 <b>Вы вышли из режима администратора.</b>\n\nНажмите /admin для возврата.`, null, env);
    }

    // Обработка переключения сервиса в продвинутом меню
    if (data.startsWith("admin_model_show_")) {
      const serviceType = data.substring("admin_model_show_".length);
      
      if (!SERVICE_TYPE_MAP[serviceType]) {
        await sendMessage(chatId, "❌ Сервис не найден.", null, env);
        return;
      }

      const statusTable = await generateModelStatusTable(env);
      const buttons = await getModelMenuKeyboard(env, serviceType);
      const serviceName = SERVICE_TYPE_MAP[serviceType].name;

      await editMessageWithKeyboard(
        chatId,
        query.message.message_id,
        `🧠 <b>НАСТРОЙКА AI-МОДЕЛЕЙ</b>\n\n${statusTable}\n---\nВыберите модель для: ${serviceName}`,
        env,
        buttons
      );
      return;
    }
    if (data.startsWith("admin_model_set_")) {
      const payload = data.substring("admin_model_set_".length);
      const separatorIndex = payload.indexOf(";");
      if (separatorIndex === -1) {
        await logDebug("❌ Ошибка парсинга callback_data: нет разделителя ';'", env);
        return;
      }
      const serviceType = payload.substring(0, separatorIndex);
      const modelKey = payload.substring(separatorIndex + 1);
    
      if (!SERVICE_TYPE_MAP[serviceType] || !AI_MODELS[modelKey]) {
        await logDebug(`❌ Неверная модель: ${serviceType} / ${modelKey}`, env);
        return;
      }
    
      const kvKey = SERVICE_TYPE_MAP[serviceType].kvKey;
      await env.USER_DB.put(kvKey, modelKey);
    
      // Обновляем меню
      const statusTable = await generateModelStatusTable(env);
      const buttons = await getModelMenuKeyboard(env, serviceType);
      const modelName = AI_MODEL_MENU_CONFIG[serviceType]?.models[modelKey] || modelKey;
    
      await editMessageWithKeyboard(
        chatId,
        query.message.message_id,
        `🧠 <b>НАСТРОЙКА AI-МОДЕЛЕЙ</b>\n\n${statusTable}\n---\n✅ Установлена модель: <code>${modelName}</code>`,
        env,
        buttons
      );
      return;
    }
    if (action === "ai_menu") {
      const serviceType = parts[1];
      if (!SERVICE_TYPE_MAP[serviceType]) {
        await sendMessage(chatId, "❌ Сервис не найден.", null, env);
        return;
      }
      const statusTable = await generateModelStatusTable(env);
      const buttons = await getModelMenuKeyboard(env, serviceType);
      const serviceName = SERVICE_TYPE_MAP[serviceType].name;
      await editMessageWithKeyboard(
        chatId,
        query.message.message_id,
        `🧠 <b>НАСТРОЙКА AI-МОДЕЛЕЙ</b>\n\n${statusTable}\n---\nВыберите модель для: ${serviceName}`,
        env,
        buttons
      );
      return;
    }
    if (action === "ai_menu_main") {
      const statusTable = await generateModelStatusTable(env);
      const buttons = getAIServiceMenuKeyboard(); // ← это список сервисов
      await editMessageWithKeyboard(
        chatId,
        query.message.message_id,
        `🧠 <b>НАСТРОЙКА AI-МОДЕЛЕЙ</b>\n\n${statusTable}\n---\nВыберите сервис:`,
        env,
        buttons
      );
      return;
    }
    if (action === "ai_menu_back") {
      const buttons = Object.entries(SERVICE_TYPE_MAP).map(([type, info]) => [
        { text: info.name, callback_data: `ai_menu:${type}` }
      ]);
      return await editMessageWithKeyboard(
        chatId,
        query.message.message_id,
        `🧠 <b>Выберите тип ИИ-сервиса:</b>`,
        env,
        buttons
      );
    }

    if (action === "ask_ref_url") {
      // Если рефа нет, просто шлем инструкцию и просим прислать ссылку текстом
      const instruction = `📥 <b>Как подключить хранилку друга:</b>\n\n` +
                          `1. Попроси друга прислать тебе реф-ссылку (он может создать её командой /share).\n` +
                          `2. Либо просто скопируй и <b>пришли мне токен</b> (например: <code>${Math.random().toString(36).substring(2, 10)}</code>) прямо в этот чат.`;
      return await sendMessage(chatId, instruction, null, env);
    }
    if (action === "ask_mailru_webdav") {
      await env.USER_DB.put(`state:${userId}`, "wait_webdav_url");
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
      return await sendMessage(chatId, `🌐 <b>Подключение своего сервера</b>\n
    Пришли ссылку в формате:\n
    <code>https://user:pass@webdav.yandex.ru</code>\n
    <i>После получения я удалю твое сообщение из чата в целях безопасности.</i>`, null, env);
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

/**
 * Редактирует сообщение и устанавливает новую инлайн-клавиатуру.
 * @param {number} chatId - ID чата.
 * @param {number} messageId - ID сообщения.
 * @param {string} text - Текст сообщения.
 * @param {Object} env - Окружение.
 * @param {Array} keyboard - Массив кнопок (inline_keyboard).
 */
async function editMessageWithKeyboard(chatId, messageId, text, env, keyboard) {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/editMessageText`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      message_id: messageId,
      text: text,
      parse_mode: "HTML",
      reply_markup: { inline_keyboard: keyboard }
    })
  });

  if (!response.ok) {
    console.error("Ошибка редактирования сообщения:", await response.text());
  }
}

/**
 * Проверяет, является ли текст реф-токеном или ссылкой.
 * @param {string} text - Входящее сообщение.
 * @returns {boolean} true, если это реф-токен или ссылка.
 */
function isRefTokenOrLink(text) {
  // Реф-токен: короткая строка из букв и цифр (например, p1u6ast5)
  const refTokenRegex = /^[a-zA-Z0-9]{6,12}$/;
  // Ссылка на бота с ref (например, https://t.me/...?start=ref_...)
  const refLinkRegex = /https?:\/\/t\.me\/[a-zA-Z0-9_]+(\?start=ref_[a-zA-Z0-9]+)?/;
  
  return refTokenRegex.test(text) || refLinkRegex.test(text);
}

// --- Вспомогательные функции для работы с буферами (необходимы для multipart/form-data) ---
/**
 * Объединяет два ArrayBuffer/Uint8Array.
 * @param {Uint8Array} buffer1
 * @param {Uint8Array} buffer2
 * @returns {Uint8Array}
 */
function concatBuffers(buffer1, buffer2) {
  const tmp = new Uint8Array(buffer1.byteLength + buffer2.byteLength);
  tmp.set(buffer1, 0);
  tmp.set(buffer2, buffer1.byteLength);
  return tmp;
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

// bufferToBase64 - Вспомогательная функция
function bufferToBase64(buffer) {
  let binary = '';
  const bytes = new Uint8Array(buffer);
  const len = bytes.byteLength;
  for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

/**
 * Скачивает файл из Telegram и возвращает его как ArrayBuffer.
 * @param {string} fileId - ID файла в Telegram.
 * @param {Object} env - Окружение (содержит TELEGRAM_TOKEN).
 * @returns {Promise<ArrayBuffer>} Бинарные данные файла.
 */
async function getFileStream(fileId, env) {
  // 1. Получаем file_path
  const getFileUrl = `https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/getFile?file_id=${fileId}`;
  const fileRes = await fetch(getFileUrl);
  const fileData = await fileRes.json();

  if (!fileData.ok) {
    throw new Error("Telegram API error: " + fileData.description);
  }

  // 2. Скачиваем сам файл как ArrayBuffer
  const fileUrl = `https://api.telegram.org/file/bot${env.TELEGRAM_TOKEN}/${fileData.result.file_path}`;
  const response = await fetch(fileUrl);
  if (!response.ok) {
    throw new Error(`Failed to download file: ${response.status} ${response.statusText}`);
  }

  return await response.arrayBuffer(); // ← ВСЁ! Только ArrayBuffer
}

function getFileType(fileName) {
  const ext = fileName.toLowerCase().split('.').pop();
  if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'tiff'].includes(ext)) {
    return "photo";
  } else if (['mp4', 'avi', 'mov', 'mkv', 'webm'].includes(ext)) {
    return "video";
  } else if (['mp3', 'wav', 'ogg', 'flac'].includes(ext)) {
    return "audio";
  } else {
    return "document";
  }
}

/**
 * Возвращает правильный MIME-тип для файла на основе его расширения.
 * Поддерживает изображения, видео, аудио, документы, текст и архивы.
 * @param {string} fileName - Имя файла (например, "report.pdf", "photo.jpg", "song.mp3")
 * @returns {string} MIME-тип. По умолчанию — "application/octet-stream"
 */
function getMimeTypeFromExtension(fileName) {
  if (!fileName) return "application/octet-stream";
  
  const ext = fileName.toLowerCase().split('.').pop();

  const mimeTypes = {
    // === Текст и код ===
    'txt': 'text/plain',
    'html': 'text/html',
    'htm': 'text/html',
    'css': 'text/css',
    'js': 'application/javascript',
    'json': 'application/json',
    'xml': 'application/xml',
    'csv': 'text/csv',
    'md': 'text/markdown',
    'log': 'text/plain',

    // === Изображения ===
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'png': 'image/png',
    'gif': 'image/gif',
    'bmp': 'image/bmp',
    'webp': 'image/webp',
    'svg': 'image/svg+xml',
    'ico': 'image/x-icon',
    'tiff': 'image/tiff',
    'tif': 'image/tiff',

    // === Аудио ===
    'mp3': 'audio/mpeg',
    'wav': 'audio/wav',
    'ogg': 'audio/ogg',
    'oga': 'audio/ogg',
    'flac': 'audio/flac',
    'aac': 'audio/aac',
    'm4a': 'audio/mp4',
    'opus': 'audio/opus',

    // === Видео ===
    'mp4': 'video/mp4',
    'mov': 'video/quicktime',
    'avi': 'video/x-msvideo',
    'mkv': 'video/x-matroska',
    'webm': 'video/webm',
    'flv': 'video/x-flv',
    'wmv': 'video/x-ms-wmv',
    'mpeg': 'video/mpeg',
    'mpg': 'video/mpeg',
    'm4v': 'video/mp4',

    // === Документы (PDF, Office) ===
    'pdf': 'application/pdf',
    'doc': 'application/msword',
    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'xls': 'application/vnd.ms-excel',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'ppt': 'application/vnd.ms-powerpoint',
    'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'odt': 'application/vnd.oasis.opendocument.text',
    'ods': 'application/vnd.oasis.opendocument.spreadsheet',
    'odp': 'application/vnd.oasis.opendocument.presentation',

    // === Архивы ===
    'zip': 'application/zip',
    'rar': 'application/vnd.rar',
    '7z': 'application/x-7z-compressed',
    'tar': 'application/x-tar',
    'gz': 'application/gzip',

    // === Прочее ===
    'epub': 'application/epub+zip',
    'mobi': 'application/x-mobipocket-ebook',
    'ics': 'text/calendar',
    'rtf': 'application/rtf'
  };

  return mimeTypes[ext] || "application/octet-stream";
}

/**
 * Генерирует таблицу текущих активных моделей.
 * @param {Object} env - Окружение.
 * @returns {Promise<string>} HTML-таблица.
 */
async function generateModelStatusTable(env) {
  let table = "📊 <b>Текущие модели:</b>\n";
  for (const [type, config] of Object.entries(SERVICE_TYPE_MAP)) {
    const modelKey = await env.USER_DB.get(config.kvKey) || Object.keys(AI_MODEL_MENU_CONFIG[type]?.models || {})[0];
    const modelName = AI_MODEL_MENU_CONFIG[type]?.models[modelKey] || "—";
    table += `• ${config.name}: <code>${modelName}</code>\n`;
  }
  return table;
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

async function uploadToYandexFromArrayBuffer(arrayBuffer, name, token, folder = "") {
  let fullPath = folder ? `/${folder}/${name}` : `/${name}`;
  fullPath = fullPath.replace(/\/+/g, '/');
  const getUrl = `https://cloud-api.yandex.net/v1/disk/resources/upload?path=${encodeURIComponent(fullPath)}&overwrite=true`;
  const r = await fetch(getUrl, {
    headers: { "Authorization": `OAuth ${token}` }
  });
  const d = await r.json();
  if (d.href) {
    const uploadRes = await fetch(d.href, {
      method: "PUT",
      body: arrayBuffer
    });
    return uploadRes.ok;
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
    const userToSave = {
        access_token: d.access_token,
        refresh_token: d.refresh_token, // Сохраняем рефреш!
        provider: "google",
        expires_at: Date.now() + (d.expires_in * 1000)
    };
    await env.USER_DB.put(`user:${uid}`, JSON.stringify(userToSave));
    await sendMessage(uid, "✅ <b>Google Drive подключен!</b>", null, env);
    return new Response("Успешно! Возвращайся в Telegram.");
  }
  return new Response("Error");
}

async function downloadFromGoogle(folderId, fileName, token, env) {
  try {
    // Ищем файлы в облаке
    const res = await fetch(`https://www.googleapis.com/drive/v3/files?pageSize=100&fields=files(id,name,mimeType,parents)`, {
      headers: { "Authorization": `Bearer ${token}` }
    });
    const data = await res.json();
    const file = data.files?.find(f => f.name.trim() === fileName.trim());

    if (!file) return null;

    let url = `https://www.googleapis.com/drive/v3/files/${file.id}?alt=media`;
    if (file.mimeType.includes('vnd.google-apps')) {
      url = `https://www.googleapis.com/drive/v3/files/${file.id}/export?mimeType=application/pdf`;
    }

    const fileRes = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    return fileRes.ok ? await fileRes.arrayBuffer() : null;
  } catch (e) { return null; }
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

async function uploadToGoogleFromArrayBuffer(arrayBuffer, name, token, folderId = "root") {
  const meta = {
    name: name,
    parents: (folderId && folderId !== "root") ? [folderId] : []
  };
  const fd = new FormData();
  fd.append('metadata', new Blob([JSON.stringify(meta)], { type: 'application/json' }));
  fd.append('file', new Blob([arrayBuffer]));
  const res = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${token}` },
    body: fd
  });
  return res.ok;
}

async function listGoogleFolders(token) {
  // Добавляем encodeURIComponent для безопасности запроса
  const query = encodeURIComponent("mimeType='application/vnd.google-apps.folder' and trashed=false");
  const url = `https://www.googleapis.com/drive/v3/files?q=${query}&fields=files(id, name)`;
  
  const res = await fetch(url, {
    headers: { "Authorization": `Bearer ${token}` }
  });

  if (res.status === 401) {
      throw new Error("TOKEN_EXPIRED"); // Сигнал для системы обновить токен
  }

  const data = await res.json();
  return data.files || []; // Возвращает массив объектов [{id, name}, ...]
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

// Работа с WebDAV
async function uploadWebDAVFromArrayBuffer(arrayBuffer, fileName, userData, env) {
  let fullPath;
  let headers = { "Content-Type": "application/octet-stream" };

  if (userData.provider === "webdav") {
    // Для Mail.ru используем host, user, pass
    fullPath = `${userData.host}/${userData.folderId ? userData.folderId + '/' : ''}${encodeURIComponent(fileName)}`;
    headers["Authorization"] = `Basic ${btoa(userData.user + ":" + userData.pass)}`;
  } else {
    // Для остальных WebDAV используем сохранённый URL
    let baseUrl = userData.webdav_url;
    if (!baseUrl.endsWith("/")) baseUrl += "/";
    fullPath = `${baseUrl}${userData.folderId ? userData.folderId + "/" : ""}${encodeURIComponent(fileName)}`;
  }

  const res = await fetch(fullPath, {
    method: "PUT",
    headers: headers,
    body: arrayBuffer
  });
  return res.status === 201 || res.status === 204;
}

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

async function uploadToDropboxFromArrayBuffer(arrayBuffer, fileName, accessToken, folderPath = "") {
  const path = `/${folderPath}/${fileName}`.replace(/\/+/g, '/');
  const arg = { path, mode: "add", autorename: true, mute: false };
  const res = await fetch("https://content.dropboxapi.com/2/files/upload", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${accessToken}`,
      "Dropbox-API-Arg": JSON.stringify(arg),
      "Content-Type": "application/octet-stream"
    },
    body: arrayBuffer
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

/**
 * Выполняет поиск файлов по имени или тегам.
 * Если вызвана админом — ищет по всем файлам.
 * Если вызвана пользователем — ищет только по своим файлам.
 * @param {string} userId - ID пользователя (для обычного поиска).
 * @param {boolean} isAdmin - Флаг администратора.
 * @param {string} query - Поисковый запрос.
 * @param {Object} env - Окружение.
 */
async function searchFilesByQuery(userId, isAdmin, query, env) {
  try {
    // Очищаем запрос от лишних символов для безопасности
    const searchTerm = `%${query.trim()}%`;
    let filesResult;
    if (isAdmin) {
      // Админ: ищем по ВСЕМ файлам
      filesResult = await env.FILES_DB.prepare(
        `SELECT id FROM files 
         WHERE (fileName LIKE ? OR tags LIKE ?) 
         ORDER BY timestamp DESC LIMIT 100`
      ).bind(searchTerm, searchTerm).all();
    } else {
      // Обычный пользователь: только свои файлы
      filesResult = await env.FILES_DB.prepare(
        `SELECT id FROM files 
         WHERE userId = ? 
         AND (fileName LIKE ? OR tags LIKE ?) 
         ORDER BY timestamp DESC LIMIT 50`
      ).bind(String(userId), searchTerm, searchTerm).all();
    }

    if (!filesResult.success || !filesResult.results || filesResult.results.length === 0) {
      return { success: false, message: `По запросу "${query}" ничего не найдено.` };
    }

    // 3. Собираем только ID
    const relevantIds = filesResult.results.map(f => f.id);

    return { 
      success: true, 
      fileIds: relevantIds 
    };

  } catch (e) {
    console.error("Search error:", e);
    return { success: false, message: "Ошибка поиска: " + e.message };
  }
}

/**
 * Простой и надёжный интеллектуальный поиск.
 * - Ищет по ai_description с фильтрацией по fileType.
 * - Использует логику ИЛИ для слов (как раньше).
 * - Сортировка: сначала НОВЫЕ файлы (DESC по timestamp).
 * - Лимит: 50 последних файлов (достаточно для поиска).
 */
async function searchAIFilesByQuery(userId, isAdmin, query, env) {
  let candidates = [];

  try {
    await logDebug(`🔍 [AI Search] Запуск для userID=${userId}, isAdmin=${isAdmin}, запрос: "${query}"`, env);

    // --- Определяем тип файла ---
    let fileTypeFilter = null;
    const qLower = query.toLowerCase();
    if (qLower.includes('фото') || qLower.includes('photo') || qLower.includes('изображение')) {
      fileTypeFilter = 'photo';
    } else if (qLower.includes('видео') || qLower.includes('video')) {
      fileTypeFilter = 'video';
    } else if (qLower.includes('голос') || qLower.includes('voice')) {
      fileTypeFilter = 'voice';
    } else if (qLower.includes('аудио') || qLower.includes('audio') || qLower.includes('mp3') || qLower.includes('ogg')) {
      fileTypeFilter = 'audio';
    }

    // --- Извлекаем слова ---
    const queryWords = query
      .toLowerCase()
      .split(/\s+/)
      .filter(w => w.length >= 3 && !['для', 'про', 'с', 'на', 'в', 'и', 'или', 'из', 'все'].includes(w));

    await logDebug(`🔍 [AI Search] Тип файла: ${fileTypeFilter}, Слова: [${queryWords.join(', ')}]`, env);

    // --- Формируем SQL ---
    let sql = `SELECT id, fileName, ai_description FROM files WHERE ai_description IS NOT NULL`;
    let binds = [];

    if (!isAdmin) {
      sql += ` AND userId = ?`;
      binds.push(String(userId));
    }

    if (fileTypeFilter) {
      sql += ` AND fileType = ?`;
      binds.push(fileTypeFilter);
    }

    // Используем ИЛИ для слов (как в самом начале)
    if (queryWords.length > 0) {
      const conditions = queryWords.map(() => `(ai_description LIKE ?)`).join(' OR ');
      sql += ` AND (${conditions})`;
      binds.push(...queryWords.map(w => `%${w}%`));
    }

    // ✅ Сортировка: СНАЧАЛА НОВЫЕ файлы (DESC по timestamp)
    // ✅ Лимит: проверяем последние 50 файлов (достаточно для релевантности)
    sql += ` ORDER BY timestamp DESC LIMIT 50`;

    await logDebug(`🔍 [AI Search] SQL: ${sql} | Параметры: ${JSON.stringify(binds)}`, env);

    const candidatesResult = await env.FILES_DB.prepare(sql).bind(...binds).all();
    candidates = candidatesResult.results || [];

    await logDebug(`🔍 [AI Search] Найдено кандидатов: ${candidates.length}`, env);

    if (candidates.length === 0) {
      return { success: true, fileIds: [] };
    }

  } catch (e) {
    await logDebug(`⚠️ [AI Search] Ошибка SQL: ${e.message}`, env);
    return { success: true, fileIds: [] };
  }

  // --- Вызов ИИ ---
  try {
    const candidatesList = candidates.map(f =>
      `${f.id}. [${f.fileName}] ${f.ai_description.substring(0, 200).replace(/\n/g, ' ')}...`
    ).join("\n");

    const prompt = `Ты — эксперт по релевантности.
Запрос: "${query}"
Кандидаты:
${candidatesList}
ИНСТРУКЦИЯ: Верни ТОЛЬКО ID через запятую. Ничего больше.`;

    const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
    const responseText = await handleSearchRequest(prompt, modelConfig, env);

    const relevantIds = [...responseText.matchAll(/\b\d+\b/g)]
      .map(match => parseInt(match[0]))
      .filter(id => id > 0);

    if (relevantIds.length > 0) {
      return { success: true, fileIds: relevantIds };
    }
    throw new Error("ИИ не вернул ID");

  } catch (e) {
    await logDebug(`❌ [AI Search] Сбой ИИ. Используем всех кандидатов.`, env);
    return { success: true, fileIds: candidates.map(f => f.id) };
  }
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

// ✅ *** Gemini Speech-to-Text (STT - голосовое сообщение) - УНИФИЦИРОВАНО ***
/**
 * Транскрибирует аудиофайл (ArrayBuffer) через Gemini API.
 * @param {Object} config - Объект активной конфигурации (AI_MODELS.AUDIO_TO_TEXT_GEMINI).
 * @param {ArrayBuffer} audioBuffer - Буфер аудиофайла.
 * @param {Object} env - Объект окружения, содержащий ключ.
 * @returns {Promise<string>} Транскрибированный текст.
 */
async function callGeminiSpeechToText(config, audioBuffer, env) { // <-- УНИФИЦИРОВАННАЯ ПОДПИСЬ
    
  // --- ДИНАМИЧЕСКИЕ ПАРАМЕТРЫ ИЗ КОНФИГУРАЦИИ ---
  const API_KEY_ENV_NAME = config.API_KEY; 
  const API_KEY = env[API_KEY_ENV_NAME]; 
  const BASE_URL = config.BASE_URL; 
  const MODEL = config.MODEL; 
  
  // Сборка универсального URL
  const url = `${BASE_URL}/models/${MODEL}:generateContent`; 
  // ------------------------------------

  if (!API_KEY) {
      throw new Error(`Gemini API key is missing. Expected env var: ${API_KEY_ENV_NAME}`);
  }
  
  // 1. КОНВЕРТАЦИЯ: ArrayBuffer в Base64
  const audioBase64 = arrayBufferToBase64(audioBuffer); 
  
  // 2. ОПРЕДЕЛЕНИЕ ТИПА: Для Telegram голосовые сообщения обычно OGG/opus.
  //const mimeType = 'audio/ogg'; 

  const systemInstructionText = "РОЛЬ: Ты эксперт по распознаванию речи. ТВОЯ ЦЕЛЬ: Транскрибировать аудиофайл СТРОГО на РУССКОМ языке, возвращая ТОЛЬКО распознанный текст, без приветствий и объяснений.";

  const body = {
      system_instruction: { parts: [{ text: systemInstructionText }] },
      contents: [{
          parts: [
              { text: "Транскрибируй аудиозапись в текст. Верни только текст." },
              { inlineData: { data: audioBase64 } } // Используем конвертированный Base64 и mimeType
          ]
      }]
  };

  const response = await fetch(`${url}?key=${API_KEY}`, { // <-- УНИФИЦИРОВАННЫЙ URL И КЛЮЧ
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
  });

  const data = await response.json();
  if (data.error) { throw new Error(`Gemini STT API Error: ${data.error.message}`); }
  const textResult = data?.candidates?.[0]?.content?.parts?.[0]?.text;

  if (!textResult) {
      throw new Error(`Gemini не вернул транскрипцию. Причина: ${JSON.stringify(data.promptFeedback)}`);
  }
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

// ✅ *** Исправленная функция для Gemini Document Analysis ***
/**
 * Анализирует документ (PDF, изображение) с помощью Gemini API.
 * @param {Object} config - Конфигурация модели из AI_MODELS.
 * @param {ArrayBuffer} arrayBuffer - Данные файла.
 * @param {Object} env - Окружение с API-ключом.
 * @param {string} mimeType - MIME-тип документа (e.g., 'application/pdf'). 
* @returns {Promise<string>} Описание документа.
 */
async function callGeminiDocument(config, arrayBuffer, env, mimeType) {
  const API_KEY_ENV_NAME = config.API_KEY;
  const API_KEY = env[API_KEY_ENV_NAME];
  const BASE_URL = config.BASE_URL;
  const MODEL = config.MODEL;

  if (!API_KEY) throw new Error(`API key ${config.API_KEY} не задан`);

  // Конвертируем ArrayBuffer в Base64
  const base64Data = arrayBufferToBase64(arrayBuffer);

  const url = `${BASE_URL}/models/${MODEL}:generateContent?key=${API_KEY}`;
  
  const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
          contents: [{
              parts: [
                  { text: "Ты — эксперт по анализу документов. Дай краткое и информативное описание этого документа на русском языке. Сосредоточься на сути." },
                  {
                    inlineData: {
                          mimeType: mimeType,
                          data: base64Data // ← Тут будет base64
                      }
                  }
              ]
          }]
      })
  });

  const data = await response.json();
  if (data.error) throw new Error(`Gemini error: ${data.error.message}`);
  return data.candidates?.[0]?.content?.parts?.[0]?.text?.trim() || "";
}

// ✅ *** Gemini Video Vision (видео аналитика) - ИСПРАВЛЕНО ***
/**
* Выполняет анализ видеоконтента (Video Captioning) с помощью Gemini 2.5 Flash.
* @param {Object} config - Объект активной конфигурации (AI_MODELS.VIDEO_TO_ANALYSIS_GEMINI).
* @param {ArrayBuffer} videoBuffer - Буфер видеофайла.
* @param {Object} env - Объект окружения, содержащий ключ.
* @param {string} mimeType - MIME-тип видео (напр., 'video/mp4').
* @returns {Promise<string>} Сгенерированный текстовый анализ.
*/
async function callGeminiVideoVision(config, videoBuffer, env, mimeType) { 
  
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
async function callWorkersAIChat(systemPrompt, config, env, userPrompt) {
  const { AI } = env;
  if (!AI) {
      throw new Error("Workers AI binding 'AI' не настроен.");
  }

  const MODEL_NAME = config.MODEL;
  const messages = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt }
  ];

  try {
      const response = await AI.run(MODEL_NAME, { 
          messages: messages,
          stream: false,
          max_tokens: 500,
          temperature: 0.7
      });

      if (!response?.response) {
          throw new Error(`Workers AI не вернул ответ. Response: ${JSON.stringify(response)}`);
      }

      return response.response.trim();
  } catch (e) {
      console.error("Workers AI call failed:", e);
      throw new Error(`Ошибка Workers AI: ${e.message}`);
  }
}

// ✅ *** Workers AI Speech-to-Text (Whisper - голосовые сообщения) ***
/**
 * Транскрибирует аудиофайл (ArrayBuffer), используя Workers AI (Whisper).
 * @param {Object} config - Объект активной конфигурации (AI_MODELS.AUDIO_TO_TEXT_WORKERS_AI).
 * @param {ArrayBuffer} audioBuffer - Буфер аудиофайла.
 * @param {Object} env - Объект окружения, содержащий привязку AI.
 * @returns {Promise<string>} Транскрибированный текст.
 */
async function callWorkersAISpeechToText(config, audioBuffer, env) {
  const { AI } = env;
  // --- УНИФИКАЦИЯ: Используем модель из конфигурации ---
  const WHISPER_MODEL = config.MODEL; 
  // ---------------------------------------------------

  if (!AI) {
      throw new Error("Workers AI binding 'AI' не настроен.");
  }

  // Workers AI ожидает массив байтов (Array of numbers)
  // Функция теперь принимает audioBuffer вторым аргументом, согласно новой подписи.
  const audioData = [...new Uint8Array(audioBuffer)]; 

  try {
      const aiResponse = await AI.run(
          WHISPER_MODEL,
          {
              audio: audioData
          }
      );

      if (!aiResponse || !aiResponse.text) {
          throw new Error(`Whisper API не вернул ожидаемый текст. Response: ${JSON.stringify(aiResponse)}`);
      }

      // Возвращаем транскрибированный текст
      return aiResponse.text.trim();
  } catch (e) {
      console.error("Workers AI Whisper call failed:", e);
      // Перебрасываем ошибку с префиксом ASR, который вы используете в logDebug
      throw new Error(`ASR_FAIL: Ошибка Workers AI Whisper: ${e.message}`);
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

// ✅ *** callBotHubAudioToText - Транскрипция речи (BotHub/Whisper) ***
/**
 * Преобразует аудиофайл в текст через BotHub (Whisper).
 * Требует multipart/form-data.
 * @param {Object} config - Объект активной конфигурации (AI_MODELS.AUDIO_TO_TEXT_BOTHUB).
 * @param {ArrayBuffer} audioData - Аудиофайл в виде ArrayBuffer.
 * @param {Object} env - Объект окружения.
 * @returns {Promise<string>} Распознанный текст.
 */
async function callBotHubAudioToText(config, audioData, env) { // МЕНЬШЕ АРГУМЕНТОВ
  const endpoint = '/audio/transcriptions';
  const apiUrl = `${config.BASE_URL}${endpoint}`;
  
  const tokenKey = config.API_KEY;
  const token = env[tokenKey]; 
  const mimeType = 'audio/ogg'; // <-- ФИКСИРОВАННЫЙ ТИП для Telegram VOICEMESSAGE

  if (!token) {
      throw new Error(`API Token (${tokenKey}) не настроен в переменных окружения.`);
  }

  // 1. Формирование тела запроса (Multipart/form-data)
  const boundary = '----BothubWhisperBoundary' + Math.random().toString(16).slice(2);
  const contentType = `multipart/form-data; boundary=${boundary}`;
  const encoder = new TextEncoder();
  
  let body = new Uint8Array(0);
  const audioBuffer = new Uint8Array(audioData); 

  // Функция для добавления строковой части
  const addPart = (part) => {
      body = concatBuffers(body, encoder.encode(part));
  };

  // 1.1. Добавление Модели ('model')
  addPart(`--${boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\n${config.MODEL}\r\n`); 

  // 1.2. Добавление Файла ('file')
  const fileExtension = 'ogg'; // Фиксируем расширение
  const filename = `audio_file.${fileExtension}`;
  
  // Заголовок файла
  const fileHeader = `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${filename}"\r\nContent-Type: ${mimeType}\r\n\r\n`;
  addPart(fileHeader);
  
  // Добавляем сам аудиофайл
  body = concatBuffers(body, audioBuffer);
  
  // 1.3. Добавление Финальной Границы
  addPart(`\r\n--${boundary}--\r\n`);

  // 2. Отправка запроса (остается без изменений)
  const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': contentType 
      },
      body: body,
      signal: AbortSignal.timeout(30000) 
  });

  if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`BotHub Whisper API Error (${response.status}): ${errorText.substring(0, 500)}`);
  }

  // 3. Обработка ответа (остается без изменений)
  const data = await response.json();
  
  if (data.text) {
      return data.text.trim();
  } else {
      throw new Error(`BotHub Whisper API Error: Response did not contain 'text' field. Full response: ${JSON.stringify(data).substring(0, 200)}...`);
  }
}

// ✅ *** callBotHubVisionChat - Обработчик для Vision API (BotHub)
/**
 * @description Отправляет запрос на анализ изображения через Vision API (BotHub).
 * @param {Object} config - Объект активной конфигурации (AI_MODELS.IMAGE_TO_TEXT_BOTHUB).
 * @param {ArrayBuffer} imageData - Изображение в виде ArrayBuffer.
 * @param {Object} env - Объект окружения.
 * @returns {Promise<string>} Сгенерированный промпт на английском.
 */
async function callBotHubVisionChat(config, imageData, env) {
  //const config = IMAGE_TO_TEXT_CONFIG; // <--- ИСПОЛЬЗУЕМ НОВУЮ ГЛОБАЛЬНУЮ КОНСТАНТУ
  const apiKey = env[config.API_KEY];
  const baseUrl = config.BASE_URL;
  const model = config.MODEL;

  if (!apiKey) {
      throw new Error(`API Key для Vision (на BotHub) не настроен.`);
  }

  // 1. Кодирование изображения в Base64.
  // Предполагается, что функция bufferToBase64 находится выше.
  const base64Image = bufferToBase64(imageData);
  const systemMessage = "РОЛЬ И ЯЗЫК: Действуй как 'Фотореставратор'. Общение СТРОГО на РУССКОМ языке. ЦЕЛЬ: Создать максимально детализированный, буквальный промпт для Image-to-Image генерации. Твой ответ должен быть только промптом, без приветствий и объяснений.";
  // 2. Формирование тела запроса (мультимодальный формат)
  const messages = [
      { "role": "system",
        "content": systemMessage },
      { 
          "role": "user", 
          "content": [
              { "type": "text", "text": "Describe this image as a Stable Diffusion prompt." },
              // data:image/jpeg;base64,${base64Image} - стандартный формат для передачи Base64
              { "type": "image_url", "image_url": { "url": `data:image/jpeg;base64,${base64Image}` } }
          ]
      }
  ];
  
  const body = {
      model: model,
      messages: messages,
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
      throw new Error(`BOTHUB VISION API error (Status ${response.status}): ${errorText}`);
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
      throw new Error(`BOTHUB VISION API response error: Received empty content from model.`);
  }
}

// ✅ *** callBothubVideoVision - Обработчик для Video Analysis (BotHub/Gemini)
/**
 * @description Отправляет запрос на анализ видеоконтента (Video Captioning) через Bothub (Gemini 2.5 Flash).
 * @param {Object} config - Объект активной конфигурации (напр., AI_MODELS.VIDEO_TO_ANALYSIS_BOTHUB).
 * @param {ArrayBuffer} videoData - Видеофайл в виде ArrayBuffer.
 * @param {Object} env - Объект окружения.
 * @param {string} videoMimeType - MIME-тип видео (напр., 'video/mp4').
* @returns {Promise<string>} Сгенерированный текстовый анализ.
 */
async function callBothubVideoVision(config, videoData, env, videoMimeType) {
  const apiKey = env[config.API_KEY];
  const baseUrl = config.BASE_URL;
  const model = config.MODEL;

  if (!apiKey) {
      throw new Error(`API Key для Video Analysis (на BotHub) не настроен.`);
  }
  if (!videoMimeType || !videoMimeType.startsWith('video/')) {
      throw new Error(`Некорректный или отсутствующий MIME-тип видео: ${videoMimeType}`);
  }

  // 1. Кодирование видео в Base64.
  // Если функция в глобальной области видимости называется arrayBufferToBase64, используйте ее:
  // const base64Video = arrayBufferToBase64(videoData); 
  const base64Video = bufferToBase64(videoData); // Предполагаем, что эта функция доступна
  
  // --- ПЕРЕФОРМУЛИРОВАННАЯ СИСТЕМНАЯ ИНСТРУКЦИЯ (для Видеоаналитика) ---
  const systemMessage = "РОЛЬ И ЯЗЫК: Действуй как 'Мультимодальный Видеоаналитик'. Общение СТРОГО на РУССКОМ языке. ЦЕЛЬ: Предоставить подробный и структурированный анализ видеоконтента, включая визуальные и звуковые данные. Твой ответ должен быть только анализом, без приветствий и объяснений.";
  
  // 2. Формирование тела запроса (мультимодальный формат для видео)
  const userPrompt = "Проанализируй видеоролик. Предоставь полное и детализированное описание: 1) Визуальный анализ (ключевые кадры, объекты, действия). 2) Анализ аудиодорожки (транскрипция, контекст). 3) Общее резюме. Ответь только текстом анализа, используя четкую структуру.";

  const messages = [
      { "role": "system",
        "content": systemMessage }, 
      { 
          "role": "user", 
          "content": [
              { "type": "text", "text": userPrompt },
              // ВАЖНО: Формат для видео/изображения в OpenAI/BotHub API
              { "type": "image_url", "image_url": { "url": `data:${videoMimeType};base64,${base64Video}` } }
          ]
      }
  ];
  
  const body = {
      model: model,
      messages: messages,
      stream: false,
      temperature: 0.7,
      max_tokens: 4096,
  };

  const url = `${baseUrl}/chat/completions`;

  // 3. Отправка запроса (Остается без изменений)
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
      // Уточнено сообщение об ошибке для BotHub
      throw new Error(`BOTHUB VIDEO ANALYSIS API error (Status ${response.status}): ${errorText}. Это может быть связано с тем, что BotHub не пропускает видео в формате 'image_url', даже для Gemini.`);
  }

  // 4. Обработка ответа (Остается без изменений)
  const data = await response.json();
  let responseText = '';
  
  if (data.choices && data.choices.length > 0) {
      responseText = data.choices[0].message.content.trim();
  } 
  
  if (responseText) {
      return responseText;
  } else {
      throw new Error(`BOTHUB VIDEO ANALYSIS API response error: Received empty content from model.`);
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
      //MODEL: '@cf/qwen/qwen3-30b-a3b-fp8', // думающая
      MODEL: '@cf/qwen/qwen2.5-coder-32b-instruct', // программерская
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
  // ✅ [Аудио в Текст]
  AUDIO_TO_TEXT_WORKERS_AI: { 
    SERVICE: 'WORKERS_AI', 
    FUNCTION: callWorkersAISpeechToText, 
    MODEL: '@cf/openai/whisper', 
    API_KEY: 'CLOUDFLARE_API_TOKEN', 
    BASE_URL: 'AI_RUN' // Исправлено для консистентности
  },
  // ✅ [Видео в Текст]
  VIDEO_TO_TEXT_WORKERS_AI: { 
    SERVICE: 'WORKERS_AI', 
    FUNCTION: callWorkersAISpeechToText, 
    MODEL: '@cf/openai/whisper', 
    API_KEY: 'CLOUDFLARE_API_TOKEN', 
    BASE_URL: 'AI_RUN' // Исправлено для консистентности
  },

  // --- СЕРВИСЫ GOOGLE ---

  // --- GEMINI ---
  // ✅ Прекрасно работает текстовый чат
  TEXT_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiChat, 
    MODEL: 'gemini-2.5-flash',
    //MODEL: 'gemini-2.5-flash-lite', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание голоса
  AUDIO_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiSpeechToText,
    //MODEL: 'gemini-2.5-flash',
    MODEL: 'gemini-2.5-flash-lite', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание голоса
  VIDEO_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiSpeechToText,
    //MODEL: 'gemini-2.5-flash',
    MODEL: 'gemini-2.5-flash-lite', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание фото
  IMAGE_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiVision, 
    //MODEL: 'gemini-2.5-flash',
    MODEL: 'gemini-2.5-flash-lite', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // ✅ Работает распознавание видео
  VIDEO_TO_ANALYSIS_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiVideoVision, 
    //MODEL: 'gemini-2.5-flash',
    MODEL: 'gemini-2.5-flash-lite', 
    API_KEY: 'GEMINI_API_KEY', 
    BASE_URL: 'https://generativelanguage.googleapis.com/v1beta'
  },
  // Новая модель для документов
  DOCUMENT_TO_TEXT_GEMINI: { 
    SERVICE: 'GEMINI', 
    FUNCTION: callGeminiDocument, // Новая функция
    //MODEL: 'gemini-2.5-flash',
    MODEL: 'gemini-2.5-flash-lite', 
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
    FUNCTION: callBotHubAudioToText,
    MODEL: 'whisper-1', 
    API_KEY: 'BOTHUB_API_KEY', 
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
  },
  // --- BOTHUB VISION --- (ПЛАТНО и нестабильно)
  IMAGE_TO_TEXT_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    FUNCTION: callBotHubVisionChat, 
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
    FUNCTION: callBotHubAudioToText,
    MODEL: 'whisper-1', 
    API_KEY: 'BOTHUB_API_KEY', 
    BASE_URL: 'https://bothub.chat/api/v2/openai/v1'
  },
  // --- BOTHUB VIDEO VISION --- (ПЛАТНО)
  VIDEO_TO_ANALYSIS_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    FUNCTION: callBothubVideoVision, 
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
  'TEXT_TO_TEXT': { name: '✍️ Text → Text', kvKey: 'ai_config:ACTIVE_MODEL_TEXT_TO_TEXT' },
  'AUDIO_TO_TEXT': { name: '🎤 Audio → Text', kvKey: 'ai_config:ACTIVE_MODEL_AUDIO_TO_TEXT' },
  'VIDEO_TO_TEXT': { name: '🎧 Video → Text', kvKey: 'ai_config:ACTIVE_MODEL_VIDEO_TO_TEXT' },
  'IMAGE_TO_TEXT': { name: '👁️ Image → Text', kvKey: 'ai_config:ACTIVE_MODEL_IMAGE_TO_TEXT' },
  'DOCUMENT_TO_TEXT': { name: '📄 Document → Text', kvKey: 'ai_config:ACTIVE_MODEL_DOCUMENT_TO_TEXT' },
  'VIDEO_TO_ANALYSIS': { name: '👀 Video → Analysis', kvKey: 'ai_config:ACTIVE_MODEL_VIDEO_TO_ANALYSIS' }

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
  const modelName = modelConfig.MODEL;
  if (!modelConfig) {
    throw new Error(`Модель ${activeModelKey} не найдена в AI_MODELS`);
  }

  await logDebug(`🧠 AI-Модель для режима ${serviceType}:\nСервис <code>${activeModelKey}</code> модель <code>${modelName}</code>`, env);
  return modelConfig;
}