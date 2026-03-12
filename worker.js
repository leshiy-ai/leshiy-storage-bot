/* 🗄 Приложение Storage Bot "Хранилка" by Leshiy
Одновременно работает как Telegram-бот, vk-чат-бот, и vkMiniApp-приложение с функцией аплоад/доунлоад с реферальной системой доступа.
Служит «мостом» между социальными сетями и облачными хранилищами. Позволяет сохранять медиафайлы (фото, видео, документы) в личные облака.
Это продвинутый SaaS-инструмент работающий круглосуточно 24/7 для личного использования или сообщества по обмену файлами с друзьями и родственниками. 
Автор: Огорельцев Александр Валерьевич

✨ Основные функции: Автоматическая загрузка фото и видео на облачные платформы (Яндекс Диск, Google Drive, Dropbox, Облако Mail.Ru WebDAV и др.) 
прямо через телеграмм или вконтакте. Возможность предоставления доступа к Вашему хранилищу друзьям и близким просто отправив им реферальную ссылку.
Универсальность: Поддержка облачных провайдеров с авторизацией OAuth (Яндекс Диск, Google Drive, Dropbox) и WebDAV (Облако Mail.Ru, Yandex WebDAV и др.).
Умное именование: Сохраняет исходные имена для файлов без сжатия и генерирует имена по дате/времени для сжатых фото/видео/голосовых.
Также есть функция поиска по Хранилке и возможность достать файлы с Вашего облака, в телеграмм напрямую в чат, а в вк через ссылку "скачать".
Интеграция ИИ: В сопровождение прикручен умный искуственный интеллект Gemini AI (через Google AI Studio API), который может подсказать любой вопрос.
Поддержка WEBM: Возможность сохранять видеофайлы в современных форматах без потери качества и размера.
Диагностика: Команда /debug для проверки статуса подключения к хранилищу в реальном времени.
*/
// Глобальные константы
const version = "v2.5.1 от 30.01.2026"; // актуальная версия

// ----------------------------------------------------
// ГЛАВНЫЙ ОБРАБОТЧИК (WEBHOOK) Fetch
// ----------------------------------------------------
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const hostname = url.hostname;
    const state = url.searchParams.get("state"); // Это наш userId
    
    //console.log("📥 Запрос:", request.method, request.url);

    // --- ОБРАБОТКА CORS (OPTIONS) ---
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Content-Length, Authorization, Accept, Origin, x-vk-user-id, x-file-name, x-file-size",
      "Access-Control-Expose-Headers": "Content-Length, Location",
      "Access-Control-Max-Age": "86400",
    };

    // Ответ на предварительный запрос браузера
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // --- ЗАГРУЗКА ФАЙЛОВ ИЗ MINI APP ---
    if (url.pathname === "/api/upload-from-vk" && request.method === "POST") {
      const vkUserId = request.headers.get("x-vk-user-id");
      return handleVkUpload(request, env, ctx, vkUserId, corsHeaders); 
    }

    if (url.pathname === "/api/upload-multipart" && request.method === "POST") {
      return handleVkUploadMultipart(request, env, ctx, corsHeaders);
    }

    if (url.pathname === "/api/upload-buffer" && request.method === "POST") {
      return handleVkUploadArrayBuffer(request, env, ctx); 
    }

    if (url.pathname === "/api/get-upload-link" && request.method === "POST") {
      return handleGetUploadLink(request, env);
    }
    
    if (url.pathname === "/api/confirm-upload" && request.method === "POST") {
        return handleConfirmUpload(request, env);
    }
    
    // ЧИТАЕМ request ТУТ
    let body = null;
    if (request.method === "POST") {
      try { body = await request.json();
      } catch (e) { body = {};}}

    // --- ВЕБ-ИНТЕРФЕЙС И МИНИ-ПРИЛОЖЕНИЕ VK ---
    if (request.method === "GET") {
      // 1. Яндекс Диск
      if (url.pathname === "/auth/yandex") {
        const target = `https://oauth.yandex.ru/authorize?response_type=code&client_id=${env.YANDEX_CLIENT_ID}&state=${state}`;
        return renderRedirectPage(target, "Яндекс Диску");
      }

      // 2. Google Drive
      if (url.pathname === "/auth/google") {
        const redirectUri = encodeURIComponent(`https://${hostname}/auth/google/callback`);
        const target = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${env.GOOGLE_CLIENT_ID}&redirect_uri=${redirectUri}&response_type=code&scope=https://www.googleapis.com/auth/drive.file&state=${state}&access_type=offline&prompt=consent`;
        return renderRedirectPage(target, "Google Drive");
      }

      // 3. Dropbox
      if (url.pathname === "/auth/dropbox") {
        const redirectUri = encodeURIComponent(`https://${hostname}/auth/dropbox/callback`);
        const target = `https://www.dropbox.com/oauth2/authorize?client_id=${env.DROPBOX_CLIENT_ID}&redirect_uri=${redirectUri}&response_type=code&state=${state}`;
        return renderRedirectPage(target, "Dropbox");
      }

      if (url.searchParams.get("action") === "get-status") {
        const vkUserId = url.searchParams.get("userId");
        const nameFromUrl = url.searchParams.get("name");   // Получаем имя
        const photoFromUrl = url.searchParams.get("photo"); // Получаем фото
        const friendOf = await env.USER_DB.get("friend_of:" + state);

        // 1. Достаем юзера
        const kvData = await env.USER_DB.get(`user:${vkUserId}`);
        let user = kvData ? JSON.parse(kvData) : {};
        let dataChanged = false;
    
        // ОБНОВЛЕНИЕ ПРОФИЛЯ: если имя или фото пришли и они новые — сохраняем
        if (nameFromUrl && user.name !== nameFromUrl) {
            user.name = nameFromUrl;
            dataChanged = true;
        }
        if (photoFromUrl && user.photo !== photoFromUrl) {
            user.photo = photoFromUrl;
            dataChanged = true;
        }
    
        // Если данные изменились, перезаписываем JSON в KV
        if (dataChanged) {
            await env.USER_DB.put(`user:${vkUserId}`, JSON.stringify(user));
        }
    
        // 2. Проверяем админа
        const adminCfg = await env.USER_DB.get("admin:config", { type: "json" }) || { admins: [] };
        const isAdmin = adminCfg.admins.includes(String(vkUserId));
    
        const isConnected = !!(user.access_token || user.webdav_pass);
    
        let providerName = "Не настроено";
        if (user.provider === 'yandex') providerName = "Яндекс Диск";
        else if (user.provider === 'google') providerName = "Google Drive";
        else if (user.provider === 'dropbox') providerName = "Dropbox";
        else if (user.provider === 'webdav') {
            providerName = (user.webdav_host && user.webdav_host.includes('mail.ru')) ? "Mail.ru" : "WebDAV";
        }
    
        return new Response(JSON.stringify({
            isAdmin: isAdmin,
            friendId: friendOf,
            isConnected: isConnected,
            provider: user.provider || null,
            providerName: providerName,
            currentFolder: user.folderId || "Root",
            userName: user.name || "Пользователь", // Отдаем имя обратно на фронт
            userPhoto: user.photo || "",           // И фото тоже
            webdav_host: user.webdav_host || ""
        }), {
            headers: { 
                "Content-Type": "application/json; charset=UTF-8",
                "Access-Control-Allow-Origin": "*" 
            }
        });
    }

      // --- ОБРАБОТКА ЧАТА ИИ (ДЛЯ МИНИ-АППА) ---
      if (url.searchParams.get("action") === "ai_chat") {
        const chatText = url.searchParams.get("text");
        if (!chatText) {
            return new Response(JSON.stringify({ answer: "Пустой запрос" }), { headers: corsHeaders });
        }
        try {
            // 1. Используем твою функцию для получения текущей модели (как в ВК-боте)
            const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
            // 2. Используем твою функцию обработки запроса
            const responseText = await handleChatRequest(chatText, modelConfig, env);
            return new Response(JSON.stringify({ answer: responseText }), { 
                headers: corsHeaders 
            });
        } catch (e) {
            return new Response(JSON.stringify({ answer: "Ошибка ИИ: " + e.message }), { 
                headers: corsHeaders 
            });
        }
      }

      if (url.pathname === "/api/search" && request.method === "GET") {
        const query = url.searchParams.get("q") || "";
        const userId = request.headers.get('x-vk-user-id') || url.searchParams.get("userId");
    
        if (!userId) {
            return new Response(JSON.stringify({ error: "Unauthorized" }), { 
                status: 401, 
                headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
            });
        }
    
        try {
            // Поиск в D1. Ищем по вхождению строки (LIKE) и фильтруем по владельцу (userId)
            const { results } = await env.FILES_DB.prepare(
                "SELECT fileName, remotePath, timestamp, provider FROM files WHERE userId = ? AND fileName LIKE ? ORDER BY timestamp DESC LIMIT 50"
            ).bind(String(userId), `%${query}%`).all();
    
            return new Response(JSON.stringify({ results }), {
                headers: { 
                    "Content-Type": "application/json", 
                    "Access-Control-Allow-Origin": "*" 
                }
            });
        } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), { 
                status: 500, 
                headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
            });
        }
      }

      if (url.pathname === "/api/download" && request.method === "GET") {
        const path = url.searchParams.get("path");
        const name = url.searchParams.get("name") || "file";
        const userId = request.headers.get('x-vk-user-id') || url.searchParams.get("userId");
    
        if (!path || !userId) {
            return new Response("Missing parameters", { status: 400 });
        }
    
        return handleDownload(path, name, userId, env);
      }

      // --- АДМИНСКИЕ ЭНДПОИНТЫ ---
      const vkUserId = url.searchParams.get("vk_user_id");
      const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
      const isAdmin = adminCfg?.admins?.includes(String(vkUserId));

      if (url.pathname === "/api/admin/get-ai-settings" && isAdmin) {
        let services = {};
        for (const [type, info] of Object.entries(SERVICE_TYPE_MAP)) {
          const modelKey = await env.USER_DB.get(info.kvKey) || Object.keys(AI_MODEL_MENU_CONFIG[type]?.models || {})[0];
          const modelName = AI_MODEL_MENU_CONFIG[type]?.models[modelKey] || "Неизвестно";
          services[type] = {
            name: info.name,
            currentModelName: modelName
          };
        }
        return new Response(JSON.stringify({ services }), { headers: { "Content-Type": "application/json" } });
      }

      if (url.pathname === "/api/admin/list-models" && isAdmin) {
        const type = url.searchParams.get("type");
        const kvKey = SERVICE_TYPE_MAP[type].kvKey;
        const current = await env.USER_DB.get(kvKey) || "default";
        // Собираем список моделей для этого типа
        const models = Object.keys(AI_MODELS)
          .filter(k => k.startsWith(type))
          .map(k => ({ id: k, name: AI_MODELS[k].MODEL }));
        return new Response(JSON.stringify({ current, models }), { headers: { "Content-Type": "application/json" } });
      }

      if (url.pathname === "/api/get-quota") {
        const vkUserId = url.searchParams.get("vk_user_id");
        const kvData = await env.USER_DB.get(`user:${vkUserId}`);
        
        // Стандартные заголовки прямо здесь, чтобы не зависеть от внешних переменных
        const headers = { 
          "Content-Type": "application/json", 
          "Access-Control-Allow-Origin": "*" 
        };
      
        if (!kvData) return new Response(JSON.stringify({ used: 0, total: 0 }), { headers });
        
        const user = JSON.parse(kvData);
        let quota = { used: 0, total: 0 };
      
        try {
          if (user.provider === 'yandex' && user.access_token) {
            const res = await fetch("https://cloud-api.yandex.net/v1/disk/", {
              headers: { "Authorization": "OAuth " + user.access_token }
            });
            const data = await res.json();
            quota = { used: data.used_space || 0, total: data.total_space || 0 };
          } 
          else if (user.provider === 'google' && user.access_token) {
            const res = await fetch("https://www.googleapis.com/drive/v3/about?fields=storageQuota", {
              headers: { "Authorization": "Bearer " + user.access_token }
            });
            const data = await res.json();
            if (data.storageQuota) {
              quota = { used: parseInt(data.storageQuota.usage), total: parseInt(data.storageQuota.limit) };
            }
          }
          else if (user.provider === 'dropbox' && user.access_token) {
            const res = await fetch("https://api.dropboxapi.com/2/users/get_space_usage", {
              method: "POST",
              headers: { "Authorization": "Bearer " + user.access_token }
            });
            const data = await res.json();
            if (data.allocation) {
              quota = { used: data.used, total: data.allocation.allocated };
            }
          }
          else if (user.provider === 'webdav' && user.webdav_host) {
            const res = await fetch(user.webdav_host, {
              method: 'PROPFIND',
              headers: {
                'Authorization': 'Basic ' + btoa(user.webdav_user + ":" + user.webdav_pass),
                'Depth': '0'
              }
            });
            const xml = await res.text();
            const uMatch = xml.match(/quota-used-bytes>(\d+)</);
            const aMatch = xml.match(/quota-available-bytes>(\d+)</);
            const u = uMatch ? parseInt(uMatch[1]) : 0;
            const a = aMatch ? parseInt(aMatch[1]) : 0;
            quota = { used: u, total: u + a };
          }
      
          return new Response(JSON.stringify(quota), { headers });
        } catch (e) {
          // В случае ошибки возвращаем "нули", чтобы фронт не падал
          return new Response(JSON.stringify({ used: 0, total: 0, err: e.message }), { headers });
        }
      }

      if (url.pathname === "/api/create-invite") {
        const uId = url.searchParams.get("userId");
        const corsHeaders = {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        };
      
        try {
          // 1. Получаем основной объект пользователя из KV
          const userDataRaw = await env.USER_DB.get("user:" + uId);
          if (!userDataRaw) {
            return new Response(JSON.stringify({ error: "Пользователь не найден. Подключите диск." }), { status: 400, headers: corsHeaders });
          }
      
          const userData = JSON.parse(userDataRaw);
      
          // 2. Берем данные прямо из объекта (теперь "Документы" не потеряются)
          const provider = userData.provider || "yandex";
          const folderId = userData.folderId || "Root";
          
          // Генерируем код инвайта
          const inviteCode = Math.random().toString(36).substring(2, 12);
      
          // 3. Формируем объект инвайта по твоему стандарту
          const inviteData = {
            inviterId: parseInt(uId),
            provider: provider,
            token: inviteCode, 
            folderId: folderId,
            timestamp: Date.now()
          };
      
          // Сохраняем в KV
          await env.USER_DB.put("invite:" + inviteCode, JSON.stringify(inviteData));
      
          return new Response(JSON.stringify({ inviteCode: inviteCode }), { headers: corsHeaders });
        } catch (e) {
          return new Response(JSON.stringify({ error: "Ошибка: " + e.message }), { status: 500, headers: corsHeaders });
        }
      }

      if (url.pathname === "/api/get-invite-info") {
        const code = url.searchParams.get("code");
        const corsHeaders = {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        };
      
        const inviteDataRaw = await env.USER_DB.get("invite:" + code);
        if (!inviteDataRaw) {
          return new Response(JSON.stringify({ error: "not_found" }), { status: 404, headers: corsHeaders });
        }
      
        // Возвращаем данные инвайта (там есть inviterId, имя владельца и т.д.)
        return new Response(inviteDataRaw, { headers: corsHeaders });
      }

      if (url.pathname === "/api/get-friend-storage") {
        const uId = url.searchParams.get("vk_user_id");
        // Ищем в KV связь, которую мы создали при переходе по ссылке
        const friendId = await env.USER_DB.get("friend_of:" + uId);
        
        return new Response(JSON.stringify({ friendId: friendId || null }), { 
          headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
        });
      }

      if (url.pathname === "/api/connect-friend") {
        const uId = url.searchParams.get("vk_user_id") || vkUserId;
        let fId = url.searchParams.get("friend_id") || "";
      
        const headers = { 
          "Content-Type": "application/json", 
          "Access-Control-Allow-Origin": "*" 
        };
      
        if (uId && fId) {
          // 1. Если fId - код инвайта, достаем ID владельца
          if (isNaN(Number(fId))) {
            const inviteDataRaw = await env.USER_DB.get("invite:" + fId);
            if (inviteDataRaw) {
              const inviteData = JSON.parse(inviteDataRaw);
              fId = String(inviteData.inviterId);
            } else {
              return new Response(JSON.stringify({ success: false, error: "Invite not found" }), { headers, status: 404 });
            }
          }
      
          // 2. Достаем данные владельца хранилища (Друга)
          const ownerDataRaw = await env.USER_DB.get(`user:${fId}`);
          if (!ownerDataRaw) {
            return new Response(JSON.stringify({ success: false, error: "Owner storage not found" }), { headers, status: 404 });
          }
          const ownerData = JSON.parse(ownerDataRaw);
      
          // 3. Достаем или создаем профиль самого Реферала
          const referralDataRaw = await env.USER_DB.get(`user:${uId}`);
          let referralData = referralDataRaw ? JSON.parse(referralDataRaw) : { userId: uId };
      
          // 4. КОПИРУЕМ КЛЮЧЕВЫЕ ДАННЫЕ
          // Теперь реферал "видит" то же облако, что и друг
          referralData.provider = ownerData.provider;
          referralData.access_token = ownerData.access_token || null;
          referralData.refresh_token = ownerData.refresh_token || null;
          referralData.webdav_host = ownerData.webdav_host || null;
          referralData.webdav_user = ownerData.webdav_user || null;
          referralData.webdav_pass = ownerData.webdav_pass || null;
          
          // Привязываем папку инвайта как рабочую для реферала
          referralData.folderId = ownerData.folderId || "Root"; 
          referralData.is_referral = true;
          referralData.invited_by = fId;
      
          // 5. Сохраняем обновленный профиль реферала и связь
          await env.USER_DB.put(`user:${uId}`, JSON.stringify(referralData));
          await env.USER_DB.put("friend_of:" + String(uId), String(fId));
          
          return new Response(JSON.stringify({ success: true, connectedTo: fId }), { headers });
        }
        
        return new Response(JSON.stringify({ success: false }), { headers, status: 400 });
      }
      
      if (url.pathname === "/api/list-folders") {
        const vkUserId = url.searchParams.get("vk_user_id");
        const kvData = await env.USER_DB.get(`user:${vkUserId}`);
        if (!kvData) return new Response("User not found", { status: 404 });
      
        const user = JSON.parse(kvData);
        let folders = [];
      
        try {
          if (user.provider === 'yandex') {
            folders = await listYandexFolders(user.access_token);
          } else if (user.provider === 'google') {
            folders = await listGoogleFolders(user.access_token);
          } else if (user.provider === 'dropbox') {
            folders = await listDropboxFolders(user.access_token);  
          } else if (user.provider === 'webdav') {
            folders = await listWebDavFolders(user);
          }
          
          return new Response(JSON.stringify(folders), {
            headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
          });
        } catch (e) {
          return new Response(JSON.stringify({ error: e.message }), { status: 500 });
        }
      }

      // Обработка корня — обычный сайт
      if (url.pathname === "/") {
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

      // --- ОБРАБОТКА VK MINI APP ---
      if (url.pathname === "/vk" || url.pathname.startsWith("/app")) {
        const params = Object.fromEntries(url.searchParams);
        const vkUserId = params.vk_user_id;
        
        let userData = null;
        try {
            if (vkUserId) {
              const kvData = await env.USER_DB.get(`user:${vkUserId}`);
              if (kvData) {
                  userData = JSON.parse(kvData);
              }
            }
        } catch (e) {
            console.error("DB Error in MiniApp:", e);
        }
        const userId = params.vk_user_id;
        const adminCfg = await env.USER_DB.get("admin:config", { type: "json" }) || { admins: [] };
        const isAdmin = adminCfg.admins.includes(String(userId));
        const listUser = await env.USER_DB.list({ prefix: "user:" });
        const countUser = listUser.keys.length;
        const html = renderVKMiniAppHTML(params, userData, isAdmin, countUser); 
        
        return new Response(html, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Content-Security-Policy": "frame-ancestors https://vk.com https://*.vk.com; script-src https://leshiy-storage-bot.leshiyalex.workers.dev https://unpkg.com 'unsafe-inline'; img-src * data: blob:; connect-src *; style-src 'unsafe-inline';",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
          }
        });
      }
    }

    // --- 2. CALLBACKS авторизации ---
    if (url.pathname === "/auth/yandex/callback") return await handleYandexCallback(request, env);
    if (url.pathname === "/auth/google/callback") return await handleGoogleCallback(request, env);
    if (url.pathname === "/auth/mailru/callback") return await handleMailruCallback(request, env);
    if (url.pathname === "/auth/dropbox/callback") return await handleDropboxCallback(request, env);

    // Mail.ru receiver
    if (url.pathname.endsWith("receiver.html")) {
      const receiverHtml = `<html><body><script src="//connect.mail.ru/js/loader.js"></script><script>mailru.loader.require('receiver', function(){ mailru.receiver.init(); })</script></body></html>`;
      return new Response(receiverHtml, { headers: { "Content-Type": "text/html; charset=utf-8" } });
    }

    // --- 3. ВЕБХУКИ (POST запросы) ---
    if (request.method === "POST") {
      try { // --- ДОБАВЛЯЕМ СЮДА (API Mini App) ---
        
        // --- ВЫБОР ПАПКИ (Backend) ---
        if (url.pathname === "/api/select-folder") {
          try {
            const { userId, folderId } = body;
            const kvKey = `user:${userId}`;
            const kvData = await env.USER_DB.get(kvKey);
            
            if (!kvData) {
              return new Response(JSON.stringify({ error: "User not found" }), { 
                status: 404, 
                headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
              });
            }
            
            let user = JSON.parse(kvData);
            user.folderId = folderId; // Сохраняем имя или ID папки
            
            await env.USER_DB.put(kvKey, JSON.stringify(user));
            
            return new Response(JSON.stringify({ success: true }), { 
              headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
            });
          } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), { 
              status: 500, 
              headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
            });
          }
        }

        // --- СОЗДАНИЕ ПАПКИ (Backend) ---
        if (url.pathname === "/api/create-folder") {
          try {
            const { userId, name } = body;
            const kvKey = `user:${userId}`;
            const kvData = await env.USER_DB.get(kvKey);
            if (!kvData) return new Response(JSON.stringify({error: "User not found"}), { status: 404, headers: {"Access-Control-Allow-Origin": "*"} });
            
            let user = JSON.parse(kvData);
            let newId = name;

            // ВЫЗОВ ВСЕХ ФУНКЦИЙ БЕЗ ПОТЕРЬ:
            if (user.provider === 'yandex') {
              await createYandexFolder(name, user.access_token);
            } else if (user.provider === 'google') {
              // Google возвращает ID, сохраняем его
              newId = await createGoogleFolder(name, user.access_token);
            } else if (user.provider === 'webdav') {
              // ПРАВИЛЬНЫЙ порядок: имя, потом юзер
              await createWebDavFolder(name, user);
            } else if (user.provider === 'dropbox') {
              await createDropboxFolder(name, user.access_token);
            }

            // Автоматически выбираем созданную папку
            user.folderId = newId;
            await env.USER_DB.put(kvKey, JSON.stringify(user));
            
            return new Response(JSON.stringify({ success: true }), { 
              headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
            });
          } catch (e) {
            return new Response(JSON.stringify({ error: e.message }), { 
              status: 500, 
              headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } 
            });
          }
        }
        
        if (url.pathname === "/api/setup-webdav") {
          const userId = String(body.userId);
      
          // 1. Сначала получаем текущие данные пользователя из KV
          const kvData = await env.USER_DB.get(`user:${userId}`);
          let userObj = kvData ? JSON.parse(kvData) : { userId: userId };
      
          // 2. Обновляем поля WebDAV (как это делает команда /setup_webdav)
          userObj.provider = 'webdav';
          userObj.folderId = body.folderId || 'Root';
          userObj.webdav_host = body.host; 
          userObj.webdav_user = body.user;
          userObj.webdav_pass = body.pass;
          userObj.timestamp = Date.now();
      
          // 3. Сохраняем обратно в KV
          await env.USER_DB.put(`user:${userId}`, JSON.stringify(userObj));
      
          return new Response(JSON.stringify({ success: true }), {
              headers: { 
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*" 
              }
          });
        }

        if (url.pathname === "/api/disconnect") {
          const { userId } = body;
          // Просто удаляем данные из KV
          await env.USER_DB.delete(`user:${userId}`);
          return new Response(JSON.stringify({ success: true }), {
              headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
          });
        }

        // Ендпойнты POST-запросов админа
        const vkUserId = url.searchParams.get("vk_user_id");
        const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
        const isAdmin = adminCfg?.admins?.includes(String(vkUserId));

        if (url.pathname === "/api/admin/set-model" && isAdmin && request.method === "POST") {
          try {
            const { type, model } = body;
            // 1. Проверяем, что такая модель вообще существует в глобальном объекте AI_MODELS
            if (!AI_MODELS[model]) {
              return new Response(JSON.stringify({ success: false, error: "Модель не найдена в справочнике" }), { status: 400 });
            }
            // 2. Получаем kvKey для этого типа сервиса из SERVICE_TYPE_MAP
            const serviceInfo = SERVICE_TYPE_MAP[type];
            if (!serviceInfo) {
              return new Response(JSON.stringify({ success: false, error: "Неверный тип сервиса" }), { status: 400 });
            }
            // 3. Сохраняем ключ модели в KV (точно так же, как делает функция в чате)
            await env.USER_DB.put(serviceInfo.kvKey, model);
            return new Response(JSON.stringify({ 
              success: true, 
              service: serviceInfo.name, 
              model: AI_MODELS[model].MODEL 
            }), { headers: { "Content-Type": "application/json" } });
          } catch (e) {
            return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500 });
          }
        }

        // VK API → POST /vk
        if (url.pathname === "/vk") {
          return await handleVK(body, env, hostname, ctx);
        }

        // Telegram
        if (body.callback_query) {
          await handleCallbackQuery(body.callback_query, env, ctx);
          return new Response("OK");
        }
        if (body.message || body.edited_message) {
          return await handleTelegramUpdate({ ...body }, env, hostname, ctx);
        }
      } catch (e) {
        console.error("Ошибка:", e);
      }
      return new Response("OK");
    }

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
async function sendVKMessageWithKeyboard(peerId, text, env, kb = null) {
  const url = "https://api.vk.com/method/messages.send";
  
  const payload = {
    v: "5.199",
    access_token: env.VK_GROUP_TOKEN,
    peer_id: peerId.toString(), // В строку для URLSearchParams
    message: text,
    random_id: Math.floor(Math.random() * 2147483647).toString(), // В строку
    dont_parse_links: "1"
  };

  if (kb) {
    payload.keyboard = JSON.stringify(kb);
  }

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams(payload)
  });

  return await response.json();
}


function getStartKeyboardVK(userId, hostname, env, inviteData = null) {
  let buttons = [];
  
  // Если мы нашли инвайт в этом запросе, вешаем кнопку сверху
  if (inviteData && inviteData.token) {
    buttons.push([{
      action: {
        type: "text",
        label: "✅ Подключить диск",
        payload: JSON.stringify({ cmd: "confirm_ref", token: inviteData.token })
      },
      color: "positive"
    }]);
  }
  // Функция-помощник для создания кнопки ВК
  const createBtn = (label, cmd, extra = {}) => {
    return {
      action: {
        type: "text",
        label: label, // В ВК именно label!
        payload: JSON.stringify({ cmd, ...extra })
      }
    };
  };
  
  buttons.push([createBtn("🔗 Яндекс.Диск", "auth", { provider: "yandex" })]);
  buttons.push([createBtn("🔗 Google Drive", "auth", { provider: "google" })]);
  buttons.push([createBtn("🔗 Dropbox", "auth", { provider: "dropbox" })]);
  buttons.push([createBtn("📧 Облако Mail.ru", "auth_mailru")]);
  buttons.push([createBtn("🖥️ Свой WebDAV", "auth_webdav")]);

  if (inviteData) {
    buttons.push([createBtn("🤝 Подтвердить", "confirm_ref", { token: inviteData.token })]);
    //buttons.push([{ action: { type: "text", label: "📂 Выбрать папку", payload: JSON.stringify({ cmd: "/folder" }) }, color: "primary" }]);
  } else {
    buttons.push([createBtn("🤝 Пригласить друга", "ask_ref")]);
  }

  return {
    inline: true, // Кнопки будут внутри сообщения
    buttons: buttons
  };
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
<b>☁️ Провайдер: ${userData?.provider}</b> 📁 Папка: ${userData?.folderId}`;
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

    let welcome = `👋 <b>Привет! Я твоя личная Хранилка.</b>\n\n` +
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
      `☁️ Облако: <b>${userData.provider}</b>\n` +
      `📁 Папка: <b>${currentFolder}</b>`, 
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
                     `☁️ Провайдер: ${currentProvider}\n` +
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
    
    let dMsg = `🔌 <b>Диск отключен.</b>\nТы больше не подключен к ☁️ ${provider}.`;
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

  if (text.startsWith("/invites") && isAdmin) {
    try {
        const list = await env.USER_DB.list({ prefix: "invite:" });
        
        if (list.keys.length === 0) {
            await sendMessage(chatId, "📭 <b>Список инвайтов пуст.</b>", null, env);
            return new Response("OK");
        }

        let msg = `🎫 <b>Список инвайтов (${list.keys.length})</b>\n\n`;
        const inline_keyboard = [];
        
        // Ограничиваем количество вывода за раз (например, последние 15), 
        // чтобы не упереться в лимит 4096 символов Telegram
        const maxDisplay = 15;
        const keysToShow = list.keys.slice(0, maxDisplay);

        for (let i = 0; i < keysToShow.length; i++) {
            const keyName = keysToShow[i].name;
            const code = keyName.split(":")[1] || "???";
            
            const rawData = await env.USER_DB.get(keyName);
            let inviteInfo = { provider: "unknown", inviterId: "unknown", timestamp: 0 };
            
            if (rawData) {
                try { inviteInfo = JSON.parse(rawData); } catch(e) {}
            }

            msg += `${i + 1}. Код: <code>${code}</code>\n`;
            msg += `🌐 Провайдер: <b>${inviteInfo.provider}</b>\n`;
            msg += `📂 Папка: <b>${inviteInfo.folderId}</b>\n`;
            msg += `👤 От кого (ID): <code>${inviteInfo.inviterId}</code>\n`;
            
            if (inviteInfo.timestamp) {
                const date = new Date(inviteInfo.timestamp).toLocaleString('ru-RU', { timeZone: 'Asia/Yekaterinburg' });
                msg += `📅 ${date}\n`;
            }
            msg += `────────────────────\n`;

            // Кнопки удаления делаем в 2 колонки для компактности
            if (i % 2 === 0) {
                inline_keyboard.push([{ text: `❌ ${code}`, callback_data: `del_inv:${code}` }]);
            } else {
                inline_keyboard[inline_keyboard.length - 1].push({ text: `❌ ${code}`, callback_data: `del_inv:${code}` });
            }
        }

        if (list.keys.length > maxDisplay) {
            msg += `<i>...показаны первые ${maxDisplay} из ${list.keys.length}</i>`;
        }

        // Кнопка очистки — всегда отдельной строкой внизу
        inline_keyboard.push([{ text: "🗑️ УДАЛИТЬ ВСЕ ИНВАЙТЫ", callback_data: "del_inv_all" }]);

        await sendMessage(chatId, msg, { inline_keyboard }, env);
    } catch (e) {
        console.error("Invites Error:", e);
        await sendMessage(chatId, "❌ Ошибка при формировании списка инвайтов", null, env);
    }
    return new Response("OK");
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
      await createWebDavFolder("Storage", userData);

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
 * Исправлено: Мультизагрузка, AI Description, Кнопки, Состояния. 
 * @param {Object} body - Уже распаршенное тело запроса.
 * @param {Object} env - Окружение Cloudflare Worker.
 * @param {string} hostname - Хост (например, leshiy-storage-bot.leshiyalex.workers.dev)
 * @param {Object} ctx - Контекст выполнения (для waitUntil)
 * @returns {Promise<Response>} Ответ для VK.
 */
async function handleVK(body, env, hostname, ctx) {
  let chatId = null;
  
  const VK_APP_ID = env.VK_APP_ID
  const VK_GROUP_ID = env.VK_GROUP_ID
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
      // Запоминаем реф, если он пришел в ссылке
      if (message.ref && message.ref.startsWith("ref_")) {
        await env.USER_DB.put(`pending_ref:${userId}`, message.ref, { expirationTtl: 300 });
      }
      let userData = await env.USER_DB.get(userKey, { type: "json" });
      const adminCfg = await env.USER_DB.get("admin:config", { type: "json" });
      const isAdmin = adminCfg?.admins?.includes(String(userId));
      
      // Определяем команду из текста или payload
      let command = text.toLowerCase();
      let payloadData = null;
      if (message.payload) {
        try {
          payloadData = JSON.parse(message.payload);
          if (payloadData.cmd) command = payloadData.cmd;
        } catch (e) {}
      }

      // --- ОБРАБОТКА PAYLOAD КОМАНД (КНОПКИ) ---
      if (command === "auth") {
        const provider = payloadData.provider;
        let authUrl = "";
        if (provider === "yandex") authUrl = `https://oauth.yandex.ru/authorize?response_type=code&client_id=${env.YANDEX_CLIENT_ID}&state=${userId}`;
        if (provider === "google") authUrl = `https://accounts.google.com/o/oauth2/v2/auth?client_id=${env.GOOGLE_CLIENT_ID}&redirect_uri=https://${hostname}/auth/google/callback&response_type=code&scope=${encodeURIComponent("https://www.googleapis.com/auth/drive.file")}&state=${userId}&access_type=offline&prompt=consent`;
        if (provider === "dropbox") authUrl = `https://www.dropbox.com/oauth2/authorize?client_id=${env.DROPBOX_CLIENT_ID}&response_type=code&redirect_uri=${encodeURIComponent(`https://${hostname}/auth/dropbox/callback`)}&token_access_type=offline&state=${userId}`;
        
        await sendVKMessage(chatId, `🔗 Ссылка для авторизации ${provider}:\n${authUrl}`, env);
        return new Response("OK");
      }

      // Внутри switch/case или if-else для команд в handleVK
      if (command === "auth_mailru") {
        await env.USER_DB.put(`state:${userId}`, "wait_webdav_url"); // Состояние то же самое
        const msg = "📧 Настройка Облака Mail.ru\n\n" +
                    "1. Зайди в почту через браузер.\n" +
                    "2. Настройки ⮕ Пароли для внешних приложений.\n" +
                    "3. Создай новый пароль (назови его 'VK Bot').\n\n" +
                    "🚀 Пришли мне ссылку в формате:\n" +
                    "https://твоя_почта@mail.ru:пароль@webdav.cloud.mail.ru";
        await sendVKMessage(chatId, msg, env);
        return new Response("OK");
      }

      if (command === "auth_webdav") {
        await env.USER_DB.put(`state:${userId}`, "wait_webdav_url");
        const msg = "🖥️ Настройка WebDAV\n\n" +
                    "Пришли данные в одном из форматов:\n" +
                    "1️⃣  ХОСТ|ЛОГИН|ПАРОЛЬ\n" +
                    "2️⃣  https://логин:пароль@хост\n\n" +
                    "Пример:\n" +
                    "https://webdav.yandex.ru|myuser|mypass";
        await sendVKMessage(chatId, msg, env);
        return new Response("OK");
      }
      
      if (command === "next_folders") {
        const offset = payloadData.off || 0;
        const limit = 5;
        
        let folders = [];
        // Повторяем запрос списка (как в Телеге при коллбэках)
        if (userData.provider === "google") folders = await listGoogleFolders(userData.access_token);
        else if (userData.provider === "yandex") folders = await listYandexFolders(userData.access_token);
        else if (userData.provider === "dropbox") folders = await listDropboxFolders(userData.access_token);
        else if (userData.provider === "webdav") folders = await listWebDavFolders(userData);
      
        const sliced = folders.slice(offset, offset + limit);
        
        if (sliced.length > 0) {
          const buttons = sliced.map(f => ([{
            action: { 
              type: "text", 
              label: f.name.substring(0, 35), 
              payload: JSON.stringify({ cmd: "select_folder", name: f.name, id: userData.provider === "google" ? f.id : f.name }) 
            },
            color: "primary"
          }]));
      
          if (folders.length > offset + limit) {
            buttons.push([{
              action: { 
                type: "text", 
                label: "➡️ Загрузить еще", 
                payload: JSON.stringify({ cmd: "next_folders", off: offset + limit }) 
              },
              color: "default"
            }]);
          }
          await sendVKMessageWithKeyboard(chatId, `📂 Еще папки (${offset + 1}-${offset + sliced.length}):`, env, { inline: true, buttons });
        }
        return new Response("OK");
      }
      if (command === "select_folder") {
        userData.folderId = payloadData.name;
        if (userData.provider === "google") userData.folderId = payloadData.id;
        await env.USER_DB.put(userKey, JSON.stringify(userData));
        await sendVKMessage(chatId, `✅ Выбрана папка: ${payloadData.name}`, env);
        return new Response("OK");
      }
      // Нажали кнопку "Создать новую папку"
      if (command === "start_create") {
        await env.USER_DB.put(`state:${userId}`, "wait_create_folder");
        await sendVKMessage(chatId, "📝 Напиши название для новой папки:", env);
        return new Response("OK");
      }
      if (command === "ask_ref") {
        const token = Math.random().toString(36).substring(2, 10);
        const inviteInfo = { 
          inviterId: userId, 
          provider: userData?.provider,
          folderId: userData?.folderId 
        };
        await env.USER_DB.put(`invite:${token}`, JSON.stringify(inviteInfo), { expirationTtl: 86400 });
        
        const refLink = `https://vk.com/write-${VK_GROUP_ID}?ref=ref_${token}`;
        const chatLink = "https://vk.me/join/GkFYvIVWAn5WaETpFi__j5lLouNOp9q2Hns=";
      
        let shareMsg = `🤝 Пригласи друга в облако!\n\n`;
        shareMsg += `Перешли другу эту ссылку для активации диска:\n${refLink}\n\n`;
        shareMsg += `После активации пригласи его в наш общий чат:\n${chatLink}`;
      
        await sendVKMessage(chatId, shareMsg, env);
        return new Response("OK");
      }

      // Обработка нажатия кнопки "Подтвердить"
      if (command === "confirm_ref") {
        const token = payloadData?.token;
        const inviteData = await env.USER_DB.get(`invite:${token}`, { type: "json" });
      
        if (inviteData) {
          const ownerData = await env.USER_DB.get(`user:${inviteData.inviterId}`, { type: "json" });
          if (ownerData) {
            // Клонируем доступ
            userData = {
              provider: ownerData.provider,
              shared_from: String(inviteData.inviterId),
              connected_at: Date.now(),
              folderId: ownerData.folderId,
              access_token: ownerData.access_token,
              refresh_token: ownerData.refresh_token
            };
            await env.USER_DB.put(`user:${userId}`, JSON.stringify(userData));
      
            // Ссылка на чат
            const chatLink = "https://vk.me/join/GkFYvIVWAn5WaETpFi__j5lLouNOp9q2Hns=";
      
            // Создаем клавиатуру с кнопкой-ссылкой на чат
            const joinChatKb = {
              inline: true,
              buttons: [[{
                action: {
                  type: "open_link",
                  link: chatLink,
                  label: "🚀 Перейти в рабочий чат"
                }
              }]]
            };
      
            await sendVKMessageWithKeyboard(
              chatId, 
              `✅ **Успешно!**\n\nТы подключился к диску друга ${inviteData.inviterId}.\nТеперь все файлы, которые ты отправишь в общий чат, будут улетать в облако ${inviteData.provider}.`, 
              env, 
              joinChatKb
            );
            
            // Уведомляем владельца
            await sendVKMessage(inviteData.inviterId, `🔔 Пользователь ${userId} теперь использует твое облако!`, env);
          }
        } else {
          await sendVKMessage(chatId, "❌ Ошибка: Ссылка просрочена (24ч) или неверна.", env);
        }
        return new Response("OK");
      }

      // --- КОМАНДА /START ---
      if (command.startsWith("/start") || command === "start" || command === "Начать" || message.ref) {
        // 1. Извлекаем токен (из прямой ссылки ВК или из текста сообщения)
        let refParam = message.ref || ""; 
        if (!refParam && text.includes(" ")) {
          const parts = text.split(" ");
          if (parts[1] && parts[1].startsWith("ref_")) refParam = parts[1];
        }

        let inviteData = null;
        if (refParam.startsWith("ref_")) {
          const token = refParam.split("_")[1];
          const foundInvite = await env.USER_DB.get(`invite:${token}`, { type: "json" });
          if (foundInvite) {
            inviteData = { ...foundInvite, token: token };
          }
        }

        // 2. Формируем статус с эмодзи
        let statusText = "❌ Диск не подключен";
        if (userData && userData.provider) {
          const folderInfo = userData.folderId ? ` (папка: ${userData.folderId})` : " (корень)";
          const sharedInfo = userData.shared_from ? ` [Общий диск]` : "";
          statusText = `✅ ${userData.provider} подключен${folderInfo}${sharedInfo}`;
        }

        // 3. Возвращаем классическое приветствие с командами
        let welcome = `👋 Привет! Я твоя личная Хранилка.\n`;
        welcome += `📁 Просто пришли мне фото или видео, и я закину их на сервер.\n`;
        welcome += `⚙️ Статус: ${statusText}\n`;
        welcome += `📖 Команды:\n`;
        welcome += `/folder — 📂 Выбрать папку\n`;
        welcome += `/share — 🤝 Ссылка для друга\n`;
        welcome += `/search — 🔎 Поиск файлов\n`;
        welcome += `/disconnect — 🔌 Отключить диск\n`;
        welcome += `/debug — 🛠️ Техническая информация`;
        
        if (inviteData && !userData?.shared_from) {
          welcome += `\n\n🎁 **Найдено приглашение!**\nОт владельца облака ${inviteData.provider}. Нажми "Подтвердить" на клавиатуре, чтобы начать работу.`;
        }

        // 4. Генерируем клавиатуру (она подхватит inviteData и добавит кнопку подтверждения)
        const kb = getStartKeyboardVK(userId, hostname, env, inviteData);
        
        await sendVKMessageWithKeyboard(chatId, welcome, env, kb);
        return new Response("OK");
      }

      // --- КОМАНДА /ADMIN ---
      if (command === "/admin" && isAdmin) {
        const list = await env.USER_DB.list({ prefix: "user:" });
        const adminMsg = `⚙️ Панель администратора\n✅ Авторизовано: ${list.keys.length}\n🚀 Версия: ${version}\n\nВыбери раздел настроек:`;
        
        const adminKb = {
          inline: true,
          buttons: [
            [{ action: { type: "text", label: "🤖 Настройки ИИ", payload: JSON.stringify({ cmd: "/ai_settings" }) }, color: "primary" }],
            [{ action: { type: "text", label: "📊 Статистика", payload: JSON.stringify({ cmd: "/debug" }) }, color: "secondary" }]
          ]
        };

        await sendVKMessageWithKeyboard(chatId, adminMsg, env, adminKb);
        return new Response("OK");
      }

      // --- КОМАНДА /AI_SETTINGS (ЗЕРКАЛО ТЕЛЕГРАМА) ---
      if (command === ("/ai_settings" || message.ref === '/ai_settings') && isAdmin) {
        const type = payloadData?.type;

        if (!type) {
          let msg = "🧠 Текущие модели ИИ:\n";
          for (const [type, info] of Object.entries(SERVICE_TYPE_MAP)) {
            const modelKey = await env.USER_DB.get(info.kvKey) || Object.keys(AI_MODEL_MENU_CONFIG[type]?.models || {})[0];
            const modelName = AI_MODEL_MENU_CONFIG[type]?.models[modelKey] || "—";
            msg += `• ${info.name}: ${modelName}\n`;
          }
          msg += `Выберите раздел для изменения:`;

          const aiKb = {
            inline: true,
            buttons: [
              [{ action: { type: "text", label: "📝 Text → Text", payload: JSON.stringify({ cmd: "/ai_settings", type: "TEXT_TO_TEXT" }) }, color: "primary" }],
              [{ action: { type: "text", label: "🎙️ Audio → Text", payload: JSON.stringify({ cmd: "/ai_settings", type: "AUDIO_TO_TEXT" }) }, color: "primary" }],
              [{ action: { type: "text", label: "🎥 Video → Text", payload: JSON.stringify({ cmd: "/ai_settings", type: "VIDEO_TO_TEXT" }) }, color: "primary" }],
              [{ action: { type: "text", label: "🖼️ Image → Text", payload: JSON.stringify({ cmd: "/ai_settings", type: "IMAGE_TO_TEXT" }) }, color: "primary" }],
              [{ action: { type: "text", label: "📄 Document → Text", payload: JSON.stringify({ cmd: "/ai_settings", type: "DOCUMENT_TO_TEXT" }) }, color: "primary" }],
              [{ action: { type: "text", label: "🎞️ Video → Analysis", payload: JSON.stringify({ cmd: "/ai_settings", type: "VIDEO_TO_ANALYSIS" }) }, color: "primary" }]
            ]
          };
          await sendVKMessageWithKeyboard(chatId, msg, env, aiKb);
        } else {
          // Выбор конкретной модели в разделе
          const currentConfig = await loadActiveConfig(type, env);
          const availableModels = Object.keys(AI_MODELS).filter(k => k.startsWith(type));
          
          let msg = `⚙️ Настройка: ${type}\n✅ Текущая: ${currentConfig.MODEL}\n\nДоступные модели:`;
          let buttons = [];
          
          availableModels.forEach(mKey => {
            buttons.push([{
              action: { type: "text", label: AI_MODELS[mKey].MODEL, payload: JSON.stringify({ cmd: "/set_model", type, model: mKey }) },
              color: "secondary"
            }]);
          });
          // Кнопка Назад
          buttons.push([{ action: { type: "text", label: "⬅️ Назад", payload: JSON.stringify({ cmd: "/ai_settings" }) }, color: "default" }]);

          await sendVKMessageWithKeyboard(chatId, msg, env, { inline: true, buttons });
        }
        return new Response("OK");
      }

      if (command === "/set_model" && isAdmin) {
        const { type, model } = payloadData;
        const kvKey = SERVICE_TYPE_MAP[type].kvKey;
        await env.USER_DB.put(kvKey, model);
        await sendVKMessage(chatId, `✅ Модель для ${type} изменена на ${model}`, env);
        // Возвращаемся в меню
        return await handleVK({ type: "message_new", object: { message: { peer_id: chatId, from_id: userId, text: "/ai_settings" } } }, env, hostname, ctx);
      }

      // --- КОМАНДА /DEBUG ---
      if (command === "/debug") {
        // Запрашиваем актуальное состояние прямо из KV перед выводом
        const actualData = await env.USER_DB.get(userKey, { type: "json" });
        const hasToken = !!(actualData?.access_token || actualData?.webdav_pass || actualData?.shared_from);
        
        let debugInfo = `🔧 DEBUG INFO\n`;
        debugInfo += `🤖 ВК-Чат онлайн\n`;
        debugInfo += `📦 Версия: ${version}\n`;
        debugInfo += `🔗 Статус: ${hasToken ? "✅ Соединение активно" : "❌ Не подключен"}\n`;
        debugInfo += `☁️ Провайдер: ${actualData?.provider || '—'}\n`;
        debugInfo += `📁 Папка: ${actualData?.folderId || 'Root'}\n`;
        debugInfo += `👤 Твой ID: ${userId}\n`;
        debugInfo += `👑 Админ: ${isAdmin ? "Да" : "Нет"}`;
        
        await sendVKMessage(chatId, debugInfo, env);
        return new Response("OK");
      }

      // --- КОМАНДА /SHARE ---
      if (command === "/share") {
        if (!userData?.provider) {
          await sendVKMessage(chatId, "⚠️ Сначала подключи диск!", env);
          return new Response("OK");
        }
        const inviteToken = Math.random().toString(36).substring(2, 12);
        await env.USER_DB.put(`invite:${inviteToken}`, JSON.stringify({ inviterId: userId, provider: userData.provider }), { expirationTtl: 86400 });
        const refLink = `https://vk.com/write-${VK_GROUP_ID}?ref=ref_${inviteToken}`;
        await sendVKMessage(chatId, `🚀 Твоя ссылка для друга:\n${refLink}\n📁 Папка: ${userData.folderId || "Root"}\nОблако: ${userData.provider}`, env);
        return new Response("OK");
      }
      
      // --- КОМАНДА /DISCONNECT ---
      if (command === "/disconnect") {
        await env.USER_DB.delete(userKey);
        await sendVKMessage(chatId, "🔌 Диск отключен.", env);
        return new Response("OK");
      }

      // --- КОМАНДА /FOLDER ---
      if (command === "/folder") {
        if (!userData?.access_token && !userData?.webdav_pass) {
          await sendVKMessage(chatId, "⚠️ Сначала подключи облако.", env);
          return new Response("OK");
        }

        await sendVKMessage(chatId, "📂 Получаю список папок...", env);
        let folders = [];
        try {
          if (userData.provider === "google") folders = await listGoogleFolders(userData.access_token);
          else if (userData.provider === "yandex") folders = await listYandexFolders(userData.access_token);
          else if (userData.provider === "dropbox") folders = await listDropboxFolders(userData.access_token);
          else if (userData.provider === "webdav") folders = await listWebDavFolders(userData); // ВОТ ОН
        } catch (e) {
          await sendVKMessage(chatId, `❌ Ошибка: ${e.message}`, env);
          return new Response("OK");
        }

        if (folders.length > 0) {
          const offset = 0;
          const limit = 4;
          const sliced = folders.slice(offset, offset + limit);

          const buttons = sliced.map(f => ([{
            action: { 
              type: "text", 
              label: f.name.substring(0, 35), 
              payload: JSON.stringify({ 
                cmd: "select_folder", 
                name: f.name,
                // Логика ID как в Телеге: Google - ID, остальные - Имя (включая WebDAV и Dropbox)
                id: userData.provider === "google" ? f.id : f.name 
              }) 
            },
            color: "primary"
          }]));
          // КНОПКА СОЗДАНИЯ ПАПКИ
          buttons.unshift([{
            action: { type: "text", label: "🗂 Создать новую папку", payload: JSON.stringify({ cmd: "start_create" }) },
            color: "positive"
          }]);
          // Кнопка пагинации
          if (folders.length > limit) {
            buttons.push([{
              action: { 
                type: "text", 
                label: "➡️ Загрузить еще", 
                payload: JSON.stringify({ cmd: "next_folders", off: limit }) 
              },
              color: "default"
            }]);
          }
          await sendVKMessageWithKeyboard(chatId, `☁️ Облако: ${userData.provider}. 📂 Всего папок: ${folders.length}. Выбери из (1-${sliced.length})`, env, { inline: true, buttons });
        } else {
          // Если папок нет
          const createBtn = [[{ action: { type: "text", label: "🗂 Создать новую папку", payload: JSON.stringify({ cmd: "start_create" }) }, color: "positive" }]];
          await sendVKMessageWithKeyboard(chatId, "📁 Папок не найдено. Хочешь создать?", env, { inline: true, buttons: createBtn });
        }
        return new Response("OK");
      }

      // --- КОМАНДА /SEARCH ---
      if (command.startsWith("/search")) {
        const query = text.replace(/^\/search\s*/i, '').trim();
        if (!query) {
          await env.USER_DB.put(`state:${userId}`, "waiting_for_search", { expirationTtl: 300 });
          await sendVKMessage(chatId, "🔎 Напиши, что искать (имя файла):", env);
          return new Response("OK");
        }
        await sendVKMessage(chatId, "⏳ Ищу...", env);
        const searchResult = await searchFilesByQuery(userId, isAdmin, query, env);
        if (!searchResult.success || searchResult.fileIds.length === 0) {
          await sendVKMessage(chatId, `❌ Ничего не найдено по запросу "${query}".`, env);
          return new Response("OK");
        }
        let resList = `🔍 Найдено: ${searchResult.fileIds.length}\n`;
        for (const id of searchResult.fileIds.slice(0, 5)) {
          const f = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(id).first();
          resList += `• ${f?.fileName || 'Файл'}\n`;
        }
        await sendVKMessage(chatId, resList, env);
        return new Response("OK");
      }

      // --- ОБРАБОТКА ВЛОЖЕНИЙ (МУЛЬТИЗАГРУЗКА) ---

      // 1. Собираем ВСЕ вложения из всех возможных мест сообщения
      let allAttaches = [];
      if (message.attachments) allAttaches.push(...message.attachments);
      if (message.fwd_messages) {
          message.fwd_messages.forEach(m => { if (m.attachments) allAttaches.push(...m.attachments); });
      }
      if (message.reply_message && message.reply_message.attachments) {
          allAttaches.push(...message.reply_message.attachments);
      }

      // 2. Если нашли хоть что-то
      // --- ОБРАБОТКА ВЛОЖЕНИЙ (ПРЯМАЯ ПОСЛЕДОВАТЕЛЬНАЯ) ---
      if (allAttaches.length > 0) {
        // 1. Сразу отвечаем пользователю
        await sendVKMessage(chatId, `⏳ Начинаю загрузку в облако: ${allAttaches.length} (шт.)`, env);

        // 2. Всю работу уводим в waitUntil, чтобы основной запрос к ВК завершился быстро
        ctx.waitUntil((async () => {
          for (const attach of allAttaches) {
            try {
              // Выполняем загрузку стримом
              const success = await processOneAttachmentStream(attach, userData, userId, chatId, env);
              if (!success) {
                  console.error("Не удалось загрузить один из файлов");
              }
              // Даем воркеру "продышаться" между файлами
              await new Promise(r => setTimeout(r, 200)); 
            } catch (e) {
                console.error("Ошибка в цикле загрузки:", e);
            }
          }
          if (allAttaches.length > 1) {
              await sendVKMessage(chatId, `🏁 Все файлы загружены.`, env).catch(() => {});
          }
        })());
        // Моментально отвечаем ВК "OK", чтобы он не слал повторы
        return new Response("OK");
      }

      // --- ЛОГИКА СОСТОЯНИЙ (FOLDER / SEARCH / CREATE) ---
      const userState = await env.USER_DB.get(`state:${userId}`);
      if (userState && !text.startsWith("/") && !payloadData) {
        if (userState === "wait_folder_choice") {
          const folders = await env.USER_DB.get(`temp_folders:${userId}`, { type: "json" });
          const num = parseInt(text);
          if (folders && num > 0 && num <= folders.length) {
            const sel = folders[num - 1];
            userData.folderId = userData.provider === "google" ? sel.id : sel.name;
            await env.USER_DB.put(userKey, JSON.stringify(userData));
            await sendVKMessage(chatId, `✅ Выбрана папка: ${sel.name}`, env);
            await env.USER_DB.delete(`state:${userId}`);
            return new Response("OK");
          }
        }
        // НОВЫЙ БЛОК: Создание папки
        if (userState === "wait_create_folder") {
          const folderName = text.trim();
          let resultId = null;

          try {
            if (userData.provider === "yandex") {
              const ok = await createYandexFolder(folderName, userData.access_token);
              if (ok) resultId = folderName; 
            } 
            else if (userData.provider === "google") {
              resultId = await createGoogleFolder(folderName, userData.access_token);
            } 
            else if (userData.provider === "webdav") {
              const ok = await createWebDavFolder(folderName, userData);
              if (ok) resultId = folderName;
            }
            else if (userData.provider === "dropbox") {
              const ok = await createDropboxFolder(folderName, userData.access_token);
              if (ok) resultId = folderName;
            }

            if (resultId) {
              // Сразу сохраняем созданную папку как активную
              userData.folderId = resultId;
              await env.USER_DB.put(userKey, JSON.stringify(userData));
              await sendVKMessage(chatId, `✅ Папка "${folderName}" создана и выбрана для загрузки.`, env);
            } else {
              await sendVKMessage(chatId, "❌ Ошибка при создании папки. Возможно, имя недопустимо или она уже есть.", env);
            }
          } catch (e) {
            await sendVKMessage(chatId, `❌ Ошибка: ${e.message}`, env);
          }

          // Чистим стейт в любом случае
          await env.USER_DB.delete(`state:${userId}`);
          return new Response("OK");
        }
        if (userState === "waiting_for_search") {
          await env.USER_DB.delete(`state:${userId}`);
          await sendVKMessage(chatId, `⏳ Ищу "${text}"...`, env);
          const searchResult = await searchFilesByQuery(userId, isAdmin, text, env);
          if (searchResult.success && searchResult.fileIds.length > 0) {
            let resList = `🔍 Результаты:\n`;
            for (const id of searchResult.fileIds.slice(0, 5)) {
              const f = await env.FILES_DB.prepare("SELECT fileName FROM files WHERE id = ?").bind(id).first();
              resList += `• ${f?.fileName || 'Файл'}\n`;
            }
            await sendVKMessage(chatId, resList, env);
          } else {
            await sendVKMessage(chatId, "❌ Ничего не найдено.", env);
          }
          return new Response("OK");
        }

        if (userState === "wait_webdav_url") {
          let rawText = text.trim();
          let url, user, pass;
      
          // 1. Проверяем формат с палочкой URL|Логин|Пароль
          const parts = rawText.split("|");
          
          if (parts.length === 3) {
              url = parts[0].trim();
              user = parts[1].trim();
              pass = parts[2].trim();
          } else {
              // 2. Если палочек нет, пробуем парсить как в Telegram (user:pass@host)
              try {
                  const protocolMatch = rawText.match(/^(https?:\/\/)/);
                  if (!protocolMatch) throw new Error("Ссылка должна начинаться с https://");
                  
                  const protocol = protocolMatch[1];
                  let linkWithoutProtocol = rawText.replace(protocol, "");
      
                  const lastAtIndex = linkWithoutProtocol.lastIndexOf("@");
                  if (lastAtIndex === -1) throw new Error("Используй формат URL|Логин|Пароль или https://логин:пароль@сервер");
      
                  const authPart = linkWithoutProtocol.substring(0, lastAtIndex);
                  const hostPart = linkWithoutProtocol.substring(lastAtIndex + 1);
      
                  const colonIndex = authPart.indexOf(":");
                  if (colonIndex === -1) throw new Error("В ссылке не найден пароль (двоеточие после логина)");
      
                  user = authPart.substring(0, colonIndex);
                  pass = authPart.substring(colonIndex + 1);
                  url = `${protocol}${hostPart}`;
              } catch (e) {
                  await sendVKMessage(chatId, `❌ Ошибка формата:\n${e.message}`, env);
                  return new Response("OK");
              }
          }
      
          // 3. Сохраняем данные (используем имена полей как в твоем WebDAV блоке)
          // ПРОВЕРКА: Это Mail.ru или обычный WebDAV?
          const isMailRu = url.toLowerCase().includes("mail.ru");
          const providerName = isMailRu ? "WebDAV (Облако Mail.ru)" : "WebDAV";

          userData = { 
              ...userData, // сохраняем id и username если были
              provider: "webdav", 
              webdav_host: url, 
              webdav_user: user, 
              webdav_pass: pass,
              folderId: "Storage" 
          };
      
          await env.USER_DB.put(userKey, JSON.stringify(userData));
          await env.USER_DB.delete(`state:${userId}`);
          
          await sendVKMessage(chatId, `✅ ${providerName} успешно настроен!`, env);
          return new Response("OK");
        }

        if (text.startsWith("/create ")) {
          const folderName = text.replace("/create ", "").trim();
          let ok = false;
          if (userData.provider === "google") ok = await createGoogleFolder(folderName, userData.access_token);
          else if (userData.provider === "yandex") ok = await createYandexFolder(folderName, userData.access_token);
          else if (userData.provider === "dropbox") ok = await createDropboxFolder(folderName, userData.access_token);
          if (ok) {
            userData.folderId = folderName;
            await env.USER_DB.put(userKey, JSON.stringify(userData));
            await sendVKMessage(chatId, `✅ Папка "${folderName}" создана!`, env);
          }
          await env.USER_DB.delete(`state:${userId}`);
          return new Response("OK");
        }
      }

      // --- ЧАТ С ИИ (Если нет других команд) ---
      if (text && !text.startsWith("/")) {
        ctx.waitUntil((async () => {
          try {
            const modelConfig = await loadActiveConfig('TEXT_TO_TEXT', env);
            const responseText = await handleChatRequest(text, modelConfig, env);
            await sendVKMessage(chatId, responseText, env);
          } catch (e) {}
        })());
        return new Response("OK");
      }
    }
    
  } catch (e) {
    console.error("VK Error:", e);
    if (chatId) await sendVKMessage(chatId, `❌ Критическая ошибка: ${e.message}`, env);
  }
  return new Response("OK");
}

/**
 * Генерирует HTML-страницу для VK Mini App.
 */
function renderVKMiniAppHTML(params, userData, isAdmin, countUser) {
  const userId = params?.vk_user_id || "UNKNOWN";
  const groupId = params?.vk_group_id || "235249123";
  const appId = params?.vk_app_id || "54419010";
  const cdn = "https://images.leshiyalex.workers.dev";
  
  const isConnected = !!(userData && (userData.access_token || userData.webdav_pass));
  const provider = userData?.provider || 'none'
  const currentFolder = userData?.folderId || "Root";

  let providerName = isConnected ? (provider === 'yandex' ? 'Яндекс Диск' : provider === 'google' ? 'Google Drive' : provider === 'dropbox' ? 'Dropbox' : userData?.webdav_host?.includes('mail.ru') ? 'Облако Mail.ru' : 'WebDAV') : "не настроено";

  return `
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no, user-scalable=no, viewport-fit=cover">
  <script src="https://unpkg.com/@vkontakte/vk-bridge/dist/browser.min.js"></script>
  <style>
    :root {
    --bg-color: #ffffff;
    --panel-bg: #ffffff;
    --text-color: #000000;
    --text-secondary: #818c99;
    --accent-color: #2688eb;
    --bubble-bg: #f5f7f8;
    --border-color: #dce1e6;
    --msg-body-text: #2c2d2e;
    --ai-chat-bg: #e2f7e2;
    --ai-chat-text: #1a5c1a;
    }
    [data-theme="dark"] {
        --bg-color: #19191a;
        --panel-bg: #222222;
        --text-color: #e1e3e6;
        --text-secondary: #909499;
        --accent-color: #71aaeb;
        --bubble-bg: #2d2d2e;
        --border-color: #363738;
        --msg-body-text: #d1d2d3;
        --ai-chat-bg: #213021;
        --ai-chat-text: #a5d6a5;
    }
    body { font-family: -apple-system, system-ui, sans-serif; background: var(--bg-color); margin: 0; padding: 12px; color: var(--text-color); -webkit-tap-highlight-color: transparent; }
    .tg-message { background: var(--panel-bg); border-radius: 12px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); margin-bottom: 12px; position: relative; z-index: 1; }
    .status-group { border-left: 4px solid ${isConnected ? '#4bb34b' : '#eb4242'}; background: var(--bubble-bg); border-radius: 0 8px 8px 0; padding: 12px 16px; margin: 12px 0; font-size: 15px; }
    @keyframes spin { 100% { transform: rotate(360deg); } }
    .refresh-btn { position: absolute; top: 8px; right: 8px; font-size: 24px; cursor: pointer; padding: 8px; z-index: 10; color: var(--text-secondary); line-height: 1; display: flex; align-items: center; justify-content: center; transition: color 0.2s; } .refresh-btn:active { color: var(--accent-color); } @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } } 
    .refresh-btn.loading { animation: spin 0.8s linear infinite; color: var(--accent-color); pointer-events: none; }
    .header-actions { position: absolute; top: 9px; right: 2px; display: flex; z-index: 999; gap:12px;} 
    .header-actions button.action-btn { display: flex; align-items: center; justify-content: center; width: 20px; height: 20px; }
    .action-btn { width: 20px; height: 20px; cursor: pointer; display: flex; align-items: center; justify-content: center; user-select: none; -webkit-tap-highlight-color: transparent; background: transparent; color: rgba(129, 140, 153, 0.4); font-size: 20px; } 
    .action-btn:active { background: rgba(0,0,0,0.05); } @keyframes pulse { 0% { opacity: 0.4; transform: scale(1); } 50% { opacity: 1; transform: scale(1.1); } 100% { opacity: 0.4; transform: scale(1); } } 
    .loading { animation: pulse 1.5s ease-in-out infinite; color: var(--accent-color) !important; } @media screen and (max-width: 600px) { .action-btn { width: 33px; height: 12px; } }
    #ui-admin-commands, #ui-commands-block { background: var(--bg-color); position: relative; z-index: 10; }
    .msg-bubble { background: var(--panel-bg); border-radius: 12px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); margin-bottom: 12px; display: none; position: relative; border-left: 4px solid var(--accent-color); }
    .msg-header { font-weight: bold; margin-bottom: 8px; display: flex; align-items: center; gap: 8px; color: var(--accent-color); }
    .msg-body { font-size: 14px; line-height: 1.5; color: var(--msg-body-text); }
    .msg-body div { margin-bottom: 4px; }
    .chat-btn { background: #5181b8; color: white; border-radius: 8px; padding: 10px; text-align: center; font-weight: 500; margin-top: 8px; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 8px; }
    .chat-btn-secondary { background: var(--bubble-bg); color: var(--accent-color); border-radius: 8px; padding: 10px; text-align: center; font-weight: 500; margin-top: 6px; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 8px; }
    .blue-link { background: var(--bg-color); color: var(--accent-color); cursor: pointer; text-decoration: none; font-weight: 600; display: inline-block; padding: 2px 0; }
    .btn-s { background: var(--panel-bg); border: 1px solid var(--border-color); border-radius: 10px; padding: 12px; margin-top: 8px; width: 100%; box-sizing: border-box; display: flex; align-items: center; justify-content: center; gap: 10px; font-size: 15px; font-weight: 600; color: var(--accent-color); cursor: pointer; position: relative; z-index: 5; }
    .btn-s:active { background: var(--bubble-bg); }
    .btn-s img { width: 22px; height: 22px; pointer-events: none; }
    .btn-s.active { border: 2.5px solid var(--accent-color); background: var(--bubble-bg); }
    .hidden-panel { display: none; background: var(--panel-bg); padding: 16px; border-radius: 12px; margin-bottom: 12px; border: 1px solid var(--border-color); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
    .hidden-panel pre { font-size: 10px; background: var(--bubble-bg); padding: 8px; overflow-x: auto; white-space: pre-wrap; border-radius: 6px; color: var(--text-color); }
    .check-mark { color: #4bb34b; font-weight: bold; pointer-events: none; }
    .wd-form { display: none; background: var(--panel-bg); padding: 16px; border-radius: 12px; margin-top: 10px; border: 1px solid var(--border-color); }
    .mr-form { display: none; background: var(--panel-bg); padding: 16px; border-radius: 12px; margin-top: 10px; border: 1px solid var(--border-color); }
    input { width: 100%; padding: 12px; border: 1px solid var(--border-color); border-radius: 8px; margin-bottom: 8px; box-sizing: border-box; font-size: 15px; background: var(--panel-bg); color: var(--text-color); }
    .quota-card { background: var(--panel-bg); border-radius: 12px; padding: 16px; margin-top: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); }
    .progress-bg { background: var(--bubble-bg); height: 8px; border-radius: 4px; overflow: hidden; margin: 10px 0; }
    .progress-fill { background: #0077ff; width: 0%; height: 100%; transition: width 1s ease; }
    .footer { text-align: center; color: var(--text-secondary); font-size: 11px; margin-top: 25px; padding-bottom: 20px; }
    .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.5); z-index: 1000; align-items: flex-end; }
    .modal { background: var(--panel-bg); width: 100%; border-radius: 15px 15px 0 0; padding: 20px; box-sizing: border-box; max-height: 80vh; overflow-y: auto; color: var(--text-color); }
    .folder-item { padding: 16px; border-bottom: 1px solid var(--border-color); font-weight: 500; color: var(--accent-color); cursor: pointer; }
    .wd-form, .debug-window { display: none; background: var(--panel-bg); padding: 16px; border-radius: 12px; margin-top: 10px; border: 1px solid var(--border-color); }
    .debug-window pre { font-size: 10px; background: var(--bubble-bg); padding: 8px; overflow-x: auto; white-space: pre-wrap; color: var(--text-color); }
    .close-x { position: absolute; top: 8px; right: 12px; color: #adb5bd; font-size: 20px; cursor: pointer; }
    #pull-to-refresh { position: fixed; top: -50px; left: 0; right: 0; height: 50px; display: flex; align-items: center; justify-content: center; transition: transform 0.2s; z-index: 9999; background: var(--bg-color); color: var(--accent-color); font-weight: bold; }
    .pull-indicator { border: 2px solid #f3f3f3; border-top: 2px solid var(--accent-color); border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-right: 10px; display: none; }
    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    @keyframes ptr-spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    body { overscroll-behavior-y: contain; } /* Важно: отключает системный рефреш браузера */
    .progress-bar { width: 100%; height: 8px; background: #e0e0e0; border-radius: 4px; margin-top: 5px; overflow: hidden; }
    .progress-fill { height: 100%; width: 0%; background: #007bff; transition: width 0.2s ease; }
    [data-status="done"] .progress-fill { background: #28a745; }
    [data-status="error"] .progress-fill { background: #dc3545; }
    .search-modal { background: var(--bg-color); }
    .search-input-wrapper { position: sticky; top: 0; background: var(--panel-bg); padding: 12px; border-bottom: 1px solid var(--border-color); z-index: 10; }
    .search-result-item { background: var(--panel-bg); margin: 8px 12px; padding: 12px; border-radius: 10px; display: flex; justify-content: space-between; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
    .file-info { background: var(--panel-bg); display: flex; flex-direction: column; gap: 2px; }
    .file-name { background: var(--panel-bg); font-size: 14px; font-weight: 500; color: var(--text-color); word-break: break-all; }
    .file-date { background: var(--panel-bg); font-size: 11px; color: var(--text-secondary); }
    .download-link { background: var(--panel-bg); color: var(--accent-color); font-weight: 600; font-size: 14px; text-decoration: none; padding: 8px; }
    #ai-chat-container { margin: 15px 10px; background: var(--panel-bg); border-radius: 12px; padding: 12px; border: 2px solid #4986cc; box-shadow: 0 4px 15px rgba(0,0,0,0.1); display: flex; flex-direction: column; }
    #ai-chat-history { max-height: 250px; overflow-y: auto; margin-bottom: 12px; display: none; flex-direction: column; gap: 10px; }
    .chat-msg { padding: 10px 14px; border-radius: 15px; max-width: 85%; font-size: 14px; line-height: 1.4; word-wrap: break-word; display: flex; align-items: flex-start; gap: 8px; animation: slideIn 0.2s ease-out; }
    .user-msg { background-color: #2b5278 !important; color: #ffffff !important; align-self: flex-end; border-bottom-right-radius: 4px; box-shadow: 0 1px 2px rgba(0,0,0,0.2); border: none; flex-direction: row-reverse; }
    .ai-msg { background-color: var(--ai-chat-bg) !important; color: var(--ai-chat-text) !important; align-self: flex-start; border-bottom-left-radius: 2px; flex-direction: row; }
    .msg-content { display: flex; flex-direction: column; }
    .user-msg .msg-content { align-items: flex-end; text-align: right; }
    .ai-msg .msg-content { align-items: flex-start; text-align: left; }
    .msg-name { font-size: 13px; font-weight: bold; padding: 0 4px; }
    .chat-ava { width: 28px; height: 28px; border-radius: 50%; flex-shrink: 0; object-fit: cover; border: 1px solid rgba(255,255,255,0.2); }
    .chat-input-group { display: flex; gap: 8px; clear: both; }
    #ai-input { flex-grow: 1; border: 2px solid var(--border-color); border-radius: 8px; padding: 10px; color: var(--text-color); background: var(--panel-bg); }
    #send-ai-btn { border-radius: 20px !important; padding: 0 20px; height: 40px; background-color: #4986cc; color: white; border: none; cursor: pointer; font-weight: 500; }
    .loading-msg { font-style: italic; color: var(--text-secondary); font-size: 13px; margin-bottom: 5px; display: flex; align-items: center; gap: 5px; }
    @keyframes slideIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
    #ref-banner button:hover { background: #3b6da5 !important; } #ref-banner b { font-weight: bold; }
    /* Новые точечные классы для управления цветом */
    .theme-text-main { color: var(--text-color) !important; }
    .theme-bg-panel { background: var(--panel-bg) !important; }
    .theme-bg-page { background: var(--bg-color) !important; }
    .theme-border { border-color: var(--border-color) !important; }
    .theme-input { background: var(--panel-bg) !important; color: var(--text-color) !important; border-color: var(--border-color) !important; }
    .theme-commands-list { color: var(--text-secondary) !important; }
    #searchInput { background: var(--bg-color) !important; color: var(--text-color) !important; border: 1px solid var(--border-color) !important; }
    .modal-content-styled { text-align: left; font-size: 13px; color: var(--text-color) !important; }
    .modal-code-block { cursor: pointer; color: var(--accent-color) !important; display: block; background: var(--bg-color) !important; padding: 8px; border-radius: 6px; margin-top: 5px; word-break: break-all; border: 1px solid var(--border-color); }
    .modal-info-note { background: var(--bubble-bg) !important; border: 1px solid var(--border-color) !important; padding: 10px; border-radius: 8px; color: var(--text-color) !important; }
    .modal-small-text { color: var(--text-secondary) !important; display: block; margin-top: 5px; }
    .modal-title-bright { margin: 0 0 15px 0; font-size: 18px; font-weight: 800; color: #222222 !important; text-shadow: 0 0 10px rgba(255, 255, 255, 0.2); letter-spacing: 0.5px; text-align: center; }
    [data-theme="dark"] .modal-title-bright { color: #ffffff !important; text-shadow: 0 0 10px rgba(255, 255, 255, 0.3) !important; }
  </style>
</head>
<body class="theme-bg-page">
  <div id="pull-to-refresh" class="theme-bg-page" style="position:fixed; top:0; left:0; width:100%; height:80px; display:flex; flex-direction:column; align-items:center; justify-content:center; background:#ebedf0; z-index:1;">
    <div id="ptr-loader" class="loader"></div>
    <span id="ptr-text" style="font-size:13px; color:#888;">Потяните для обновления</span>
  </div>
  <div id="app-container" class="theme-bg-page" style="position:relative; z-index:2; min-height:100vh; transition: transform 0.2s cubic-bezier(0,0,0.2,1); will-change: transform;">
    <div class="header-actions">
      <button class="action-btn" onclick="toggleLanguage()" style="background:none; border:none; cursor:pointer; padding:0;">
        <span id="langIcon" style="font-size:16px;">🇷🇺</span>
      </button>
      <button id="themeToggle" class="action-btn" onclick="toggleTheme()" style="background:none; border:none; cursor:pointer; padding:0;">
        <span id="themeIcon" style="font-size:16px;">☀️</span>
      </button>
      <div class="action-btn" id="reloadIcon" onclick="uiReload()"><b>⟳</b></div>
      <div class="action-btn close-btn" onclick="closeApp()"><b>✕</b></div>
    </div>

    <div id="ui-header-block" class="tg-message">
      <div><br>👋 <b>Привет! Я твоя личная Хранилка.</b></div>
      <div style="margin-top:8px;">📁 Просто пришли мне фото или видео, и я закину их на сервер.</div>
      <div class="status-group">
      <div>⚙️ Статус: ${isConnected ? `✅ <span style="color:#4bb34b; font-weight:bold;">Подключен ${providerName}</span>` : 'Не настроено'}</div>
      <div id="curFolderLabel">📂 Папка: ${isConnected ? `<b>${currentFolder}</b>` : 'Не выбрана'}</div>
    </div>
  </div>

  <div id="adminPanel" class="msg-bubble" style="border-left-color: #4bb34b;">
    <span class="close-x" onclick="togglePanel('adminPanel')">×</span>
    <div class="msg-header">⚙️ Панель администратора</div>
    <div class="msg-body">
      <div>✅ Авторизовано: <b>${countUser}</b> пользователей</div>
      <div>🚀 <b>Версия:</b> ${version}</div>
      <div style="margin-top:12px;">Выбери раздел настроек:</div>
      <div class="chat-btn" onclick="openAiSettings()">🧠 Настройки ИИ</div>
      <div class="chat-btn-secondary" onclick="togglePanel('debugPanel')">📊 Статистика</div>
    </div>
  </div>

  <div id="aiSettingsPanel" class="msg-bubble" style="border-left-color: #5181b8; display: none;">
    <span class="close-x" onclick="togglePanel('aiSettingsPanel')">×</span>
    <div class="msg-header">🧠 Настройки моделей</div>
    <div class="msg-body">
      <div id="modelsPanel" style="margin-top: 16px; display: none;"></div>
      <div id="aiCurrentStatus" style="font-size: 13px; background: #1a1a1a; color: #fff; padding: 10px; border-radius: 8px; margin-bottom: 12px; font-family: monospace;">
      📊 <b>Текущие модели:</b><br>
      ⏳ Загрузка конфигурации...
    </div>
  </div>
  <div style="margin-bottom:10px;">---<br>Выберите сервис:</div>
    <div class="chat-btn-secondary" id="TEXT_TO_TEXT" onclick="loadModels(this)">📝 Текст → Текст</div>
    <div class="chat-btn-secondary" id="IMAGE_TO_TEXT" onclick="loadModels(this)">🖼️ Картинка → Текст</div>
    <div class="chat-btn-secondary" id="AUDIO_TO_TEXT" onclick="loadModels(this)">🎙️ Аудио → Текст</div>
    <div class="chat-btn-secondary" id="VIDEO_TO_TEXT" onclick="loadModels(this)">🎥 Видео → Текст</div>
    <div class="chat-btn-secondary" id="DOCUMENT_TO_TEXT" onclick="loadModels(this)">📄 Документ → Текст</div>
    <div class="chat-btn-secondary" id="VIDEO_TO_ANALYSIS" onclick="loadModels(this)">🎞️ Видео → Анализ</div>
    <div id="modelsList" style="margin-top: 16px; display: none;"></div>
  </div>

  <div id="debugPanel" class="msg-bubble">
    <span class="close-x" onclick="togglePanel('debugPanel')">×</span>
    <div class="msg-header">🛠 DEBUG INFO</div>
    <div id="debugContent" class="msg-body">
      <div>🗄 <b>Приложение онлайн</b></div>
      <div>📦 <b>Версия:</b> ${version}</div>
      <div>🔗 <b>Статус:</b> ${isConnected ? '✅ Соединение активно' : '❌ Не подключено'}</div>
      <div>☁️ <b>Провайдер:</b> ${isConnected ? `${provider}` : '-'}</div>
      <div>📂 <b>Папка:</b> ${isConnected ? `${currentFolder}` : '-'}</div>
      <div>👤 <b>Твой ID:</b> ${userId}</div>
      <div>👑 <b>Админ:</b> ${isAdmin ? 'Да' : 'Нет'}</div>
    </div>
  </div>

  <div id="sharePanel" class="theme-bg-panel theme-border" style="display:none; position:fixed; top: 32%; left: 50%; transform: translate(-50%, -50%); width:90%; max-width:400px; background:#fff; border:2px solid #0077ff; border-radius:12px; z-index:1000; padding:15px; box-shadow:0 10px 25px rgba(0,0,0,0.2); font-family:sans-serif;">
    <h3 class="modal-title-bright" style="margin:0 0 10px 0; font-size:16px;">Предпросмотр инвайта</h3>
    <div id="shareContent" class="theme-bg-panel theme-border" style="font-size:13px; color:#666; margin-bottom:15px; line-height:1.4;">
        </div>
    <div style="display:flex; gap:10px;">
        <button onclick="confirmAndShare()" style="flex:1; background:#0077ff; color:#fff; border:none; padding:10px; border-radius:8px; cursor:pointer;">Отправить</button>
        <button onclick="document.getElementById('sharePanel').style.display='none'" style="flex:1; background:#eee; color:#333; border:none; padding:10px; border-radius:8px; cursor:pointer;">Отмена</button>
    </div>
  </div>

  <div style="margin-top: 15px;">📖 <b>Команды:</b></div>
  <div id="ui-admin-commands" style="margin-top: 5px;">
  ${isAdmin ? `<span class="blue-link" onclick="togglePanel('adminPanel')" style="color:#4bb34b;">/admin</span> — 👑 Меню админа<br>` : ''}
  </div>
    
  <div id="ui-commands-block" style="margin-top: 0px;">      
    ${isConnected ? `<span class="blue-link" onclick="openFolderSelector()">/folder</span> — 📂 Выбрать папку для загрузки<br>` : ''}
    ${isConnected ? `<span class="blue-link" onclick="shareApp()">/share</span> — 👤 Ссылка для друга<br>` : ''}
    ${isConnected ? `<span class="blue-link" onclick="goToSearch()">/search</span> — 🔎 Поиск файлов по хранилке<br>` : ''}
    <span class="blue-link" onclick="togglePanel('debugPanel')">/debug</span> — 🛠️ Техническая информация<br>
    ${isConnected ? `<span class="blue-link" onclick="disconnect()" style="color:#ff3347;">/disconnect</span> — 🔌 Отключить диск<br>` : ''}
  </div>

  <div id="searchModal" class="modal-overlay" onclick="closeSearch()">
    <div class="modal search-modal theme-bg-page" onclick="event.stopPropagation()" style="height: 90vh; padding: 0;">
      
      <div class="search-input-wrapper theme-bg-panel theme-border">
        <div style="display:flex; align-items:center; gap:10px;">
          <input type="text" id="searchInput" class="theme-input" placeholder="Поиск файлов..." 
                 style="margin-bottom:0; flex-grow:1;" oninput="doSearch(this.value)">
          <span onclick="closeSearch()" style="cursor:pointer; font-size:28px; color:var(--text-secondary);">&times;</span>
        </div>
      </div>

      <div id="searchList" style="padding-bottom: 20px;">
        <div style="text-align:center; color:var(--text-secondary); margin-top:40px;">Введите название файла для поиска</div>
      </div>
    </div>
  </div>
  <div id="inviterZone"></div>
  <div class="upload-container" id="dropZone" style="margin: 10px; padding: 15px; border: 2px dashed #3f8ae0; border-radius: 12px; text-align: center; transition: all 0.2s;">

    <div id="ai-chat-container">
      <div id="ai-chat-history"></div>
      <div class="chat-input-group">
          <input type="text" id="ai-input" placeholder="Чат с ИИ. Спроси что-нибудь..." />
          <button id="send-ai-btn" class="button button-primary" style="padding: 8px 15px;">Отправить</button>
      </div>
    </div>
    <div id="uploadButton">
      <input type="file" id="vkFileInput" style="display: none;" onchange="uploadFileFromVK(this)" multiple>
      ${isConnected ? `
      <button class="btn-s" onclick="document.getElementById('vkFileInput').click()" id="uploadBtn" style="background: #2688eb; color: #fff; border: none; width: 100%; font-weight: 500; cursor: pointer;">
      📎 Выбрать файлы для загрузки
      </button>
      ` : ''}
    </div>
    <div id="uploadProgress" style="margin-top: 10px; font-size: 13px; color: #555; display: none;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span id="progressText">Загрузка...</span>
            <span id="cancelBtn" style="color: #999; cursor: pointer; font-size: 11px; text-decoration: underline; display: none;" onclick="cancelUpload()">отмена</span>
        </div>
        <div style="width: 100%; background: #dce1e6; height: 4px; border-radius: 2px; margin-top: 5px;">
            <div id="progressBar" style="width: 0%; background: #2688eb; height: 100%; border-radius: 2px; transition: width 0.3s;"></div>
        </div>
    </div>
  </div>

  <div id="authButtons">
    <button class="btn-s ${provider === 'yandex' ? 'active' : ''}" onclick="openAuthLink('/auth/yandex')">
      <img src="${cdn}/YandexDisk.png"> Яндекс Диск ${provider === 'yandex' ? '<span class="check-mark">✅</span>' : ''}
    </button>
    <button class="btn-s ${provider === 'google' ? 'active' : ''}" onclick="openAuthLink('/auth/google')">
      <img src="${cdn}/GoogleDrive.png"> Google Drive ${provider === 'google' ? '<span class="check-mark">✅</span>' : ''}
    </button>
    <button class="btn-s ${provider === 'dropbox' ? 'active' : ''}" onclick="openAuthLink('/auth/dropbox')">
      <img src="${cdn}/Dropbox.png"> Dropbox ${provider === 'dropbox' ? '<span class="check-mark">✅</span>' : ''}
    </button>
    <button class="btn-s ${provider === 'webdav' && userData?.webdav_host?.includes('mail.ru') ? 'active' : ''}" onclick="showMailRu()">
      <img src="${cdn}/CloudMailRu.png"> Облако Mail.ru ${userData?.webdav_host?.includes('mail.ru') ? '<span class="check-mark">✅</span>' : ''}
    </button>
    <button class="btn-s" onclick="showCustomWD()">
      <img src="${cdn}/network-drive.png"> Свой FTP/SFTP/WebDAV
    </button>
    <button class="btn-s" onclick="openFriendsStorage()">🤝 Подключить Хранилку по ссылке</button>
    <button class="btn-s" style="margin-top: 12px; background: #2688eb; color: #fff; border: none;" onclick="goToChat()">💬 Открыть чат Хранилку</button>
  </div>

  <div id="wdForm" class="msg-bubble" style="border-left-color: #adb5bd;">
    <span class="close-x" onclick="togglePanel('wdForm')">×</span>
    <div id="wdContent"></div>
    <input type="text" id="wdHost" placeholder="Сервер (WebDAV URL)" oninput="parseUrl(this.value)">
    <input type="text" id="wdUser" placeholder="Логин (Email)">
    <input type="password" id="wdPass" placeholder="Пароль приложения">
    <input type="text" id="wdFolder" placeholder="Папка для сохранения">
    <button id="saveBtn" class="chat-btn" style="width:100%; border:none;" onclick="saveWebDAV()">📥 Подключиться</button>
  </div>

  <div class="quota-card">
    ${isConnected ? `
    <div style="font-size:14px; margin-bottom:4px; opacity:0.8;">☁️ Свободное место</div>
    <div class="progress-bg"><div id="quotaBar" class="progress-fill"></div></div>
    <div id="quotaText" style="font-size:11px; color: #818c99;">Загрузка данных...</div>
    ` : ''}    
  </div>

  <div id="folderModal" class="modal-overlay" onclick="closeFolders()">
    <div class="modal" onclick="event.stopPropagation()">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
        <b>Выбор папки</b>
        <span onclick="closeFolders()" style="cursor:pointer; font-size:28px; padding: 0 10px;">&times;</span>
      </div>
      <div class="btn-s" onclick="promptCreateFolder()">➕ Создать новую папку</div>
      <div id="modalFolderList" style="margin-top:10px;">⏳ Загрузка...</div>
    </div>
  </div>

  <div class="footer">Версия: ${version} | ID: ${userId}</div>

  <script>
    // Сначала инициализируем Bridge
    vkBridge.send('VKWebAppInit');
    
    vkBridge.send('VKWebAppGetUserInfo').then(function(user) {
      if (user) {
        window.userName = user.first_name + ' ' + user.last_name;
        window.userPhoto = user.photo_100;
        console.log("Пользователь определен:", window.userName);
        
        // Теперь, когда имя точно есть, обновляем UI и шлем имя на сервер
        if (typeof refreshData === 'function') refreshData();
      }
    }).catch(function(err) {
      console.error("Ошибка Bridge:", err);
      // Если юзер запретил инфу, всё равно запускаем приложение
      if (typeof refreshData === 'function') refreshData();
    });
    // Очищаем awaiting_auth, если пользователь уже подключён
    if (${isConnected}) {
      localStorage.removeItem('awaiting_auth');
    }
    window.addEventListener("focus", function() {
      if (localStorage.getItem('awaiting_auth') === 'true') {
        localStorage.removeItem('awaiting_auth');
        // Ставим временную метку, что нам нужно открыть папки после обновления данных
        localStorage.setItem('pending_folder_select', 'true'); 
        setTimeout(() => uiReload(), 1500);
      }
    });
    
    // Определяем окружение
    function getLaunchParam(name) {
      const p = new URLSearchParams(window.location.search);
      return p.get(name);
    }
    
    // Инициализация параметров (Исправлено!)
    const userId = ${JSON.stringify(userId)} || "UNKNOWN";
    const groupId = ${JSON.stringify(groupId)} || "235249123";
    const appId = ${JSON.stringify(appId)} || "54419010";
    const currentProvider = ${JSON.stringify(provider)} || "-";
    const currentFolder = ${JSON.stringify(currentFolder)} || "Root";
    const allAiModels = ${JSON.stringify(AI_MODELS)};
    let currentLang = localStorage.getItem('appLang') || 'ru';
    let foldersCache = null;
    const UI_CDN = "${cdn}"; // ссылка на мой https://images.leshiyalex.workers.dev
    const aiInput = document.getElementById('ai-input');
    const aiBtn = document.getElementById('send-ai-btn');
    const aiHistory = document.getElementById('ai-chat-history');

    // Функция обновления стягиванием на мобилке
    (function() {
      let startY = 0;
      const container = document.getElementById('app-container');
      const ptrText = document.getElementById('ptr-text');
      const ptrLoader = document.getElementById('ptr-loader');
      let isPulling = false;
      window.addEventListener('touchstart', function(e) {
        // Начинаем только если мы в самом верху страницы
        if (window.scrollY === 0) {
          startY = e.touches[0].pageY;
          isPulling = true;
          container.style.transition = 'none'; // Убираем анимацию во время движения пальца
        }
      }, { passive: true });
      window.addEventListener('touchmove', function(e) {
        if (!isPulling) return;
        const currentY = e.touches[0].pageY;
        const diff = currentY - startY;
        if (diff > 0 && window.scrollY === 0) {
          e.preventDefault();
          // Плавное затухание движения (резиновый эффект)
          const move = Math.pow(diff, 0.8); 
          container.style.transform = 'translateY(' + move + 'px)';
          if (move > 60) {
            ptrText.innerText = "Отпустите для обновления";
          } else {
            ptrText.innerText = "Потяните для обновления";
          }
        }
      }, { passive: false });
      window.addEventListener('touchend', function() {
        if (!isPulling) return;
        isPulling = false;
        container.style.transition = 'transform 0.3s cubic-bezier(0,0,0.2,1)';
        const matrix = window.getComputedStyle(container).transform;
        const translateY = matrix !== 'none' ? parseFloat(matrix.split(',')[5]) : 0;
        if (translateY > 60) {
          // Фиксируем экран в полуоткрытом состоянии
          container.style.transform = 'translateY(60px)';
          ptrText.innerText = "Обновление...";
          ptrLoader.style.display = "block";
          // Вызываем обновление
          uiReload();
          // Возвращаем всё назад
          setTimeout(function() {
            container.style.transform = 'translateY(0)';
            setTimeout(() => { ptrLoader.style.display = "none"; }, 300);
          }, 1000);
        } else {
          container.style.transform = 'translateY(0)';
        }
      });
    })();

    // Функция смахивания окна влево или вправо для мобильных
    function makeSwipable(panel, onRemove, useRotation = true) {
      let startX = 0;
      let currentX = 0;
      const threshold = 100;
    
      // 1. Запоминаем базовое состояние из CSS (например, матрицу центровки -50% -50%)
      const style = window.getComputedStyle(panel);
      const initialTransform = style.transform !== 'none' ? style.transform : '';
    
      panel.addEventListener('touchstart', function(e) {
        startX = e.touches[0].clientX;
        panel.style.transition = 'none';
      }, {passive: true});
    
      panel.addEventListener('touchmove', function(e) {
        currentX = e.touches[0].clientX - startX;
        if (Math.abs(currentX) > 5) {
          // Собираем строку трансформации через обычные кавычки
          var rotation = useRotation ? ' rotate(' + (currentX / 20) + 'deg)' : '';
          
          // Наслаиваем смещение поверх базы
          panel.style.transform = initialTransform + ' translateX(' + currentX + 'px)' + rotation;
          panel.style.opacity = 1 - (Math.abs(currentX) / 350);
        }
      }, {passive: true});
    
      panel.addEventListener('touchend', function() {
        panel.style.transition = 'transform 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.1)';
        
        if (Math.abs(currentX) > threshold) {
          // Улетает далеко в сторону
          var direction = currentX > 0 ? 1000 : -1000;
          panel.style.transform = initialTransform + ' translateX(' + direction + 'px)';
          panel.style.opacity = '0';
          
          setTimeout(function() {
            panel.style.display = 'none';
            // ОЧЕНЬ ВАЖНО: сбрасываем в исходный CSS-вид для следующего открытия
            panel.style.transform = initialTransform; 
            panel.style.opacity = '1';
            if (onRemove) onRemove();
          }, 400);
        } else {
          // Пружинит обратно в центр
          panel.style.transform = initialTransform;
          panel.style.opacity = '1';
        }
        currentX = 0;
      });
    }

    function initTheme() {
      const savedTheme = localStorage.getItem('user-theme');
      const vkAppearance = new URLSearchParams(window.location.search).get('vk_appearance');
      
      // Приоритет: 1. Сохраненная вручную, 2. От ВК, 3. Системная
      let theme = savedTheme || (vkAppearance && vkAppearance.includes('dark') ? 'dark' : 'light');
      
      applyTheme(theme);
    }
    
    function applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        const icon = document.getElementById('themeIcon');
        if (icon) {
            icon.innerText = theme === 'dark' ? '🌙' : '☀️';
        }
        localStorage.setItem('user-theme', theme);
    }
    
    function toggleTheme() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        applyTheme(newTheme);
    }
    
    // Запускаем при загрузке
    updateLanguageUI();
    initTheme();
  
    function renderHeader(data) {
      if (!data) return;
      window.lastHeaderData = data;
      // Сначала убедимся, что флаг в HTML соответствует текущему языку
      const langBtn = document.getElementById('langToggle');
      if (langBtn) {
          langBtn.innerText = (currentLang === 'ru' ? '🇷🇺' : '🇺🇸');
      }
      const isConn = !!data.isConnected;
      const pName = data.providerName || (currentLang === 'ru' ? 'Не настроено' : 'Not configured');
      const folder = data.currentFolder || (currentLang === 'ru' ? 'Не выбрано' : 'Not selected');
      
      const fullName = data.userName || (currentLang === 'ru' ? "Пользователь" : "User");
      const firstName = fullName.split(' ')[0];
  
      // Словарь текстов
      const i18n = {
          ru: {
              hi: "Привет",
              desc: "Я твоя личная Хранилка. Просто пришли мне фото или видео, и я закину их на сервер.",
              status: "Статус",
              connected: "Подключен",
              folder: "Папка",
              notSet: "Не настроено"
          },
          en: {
              hi: "Hi",
              desc: "I'm your personal Storage. Just send me photos or videos, and I'll upload them to the server.",
              status: "Status",
              connected: "Connected to",
              folder: "Folder",
              notSet: "Not configured"
          }
      };
      const lang = i18n[currentLang];
      // Обновляем саму иконку флага, чтобы она не сбрасывалась при рендере
      const langIcon = document.getElementById('langIcon');

      // Твой HTML хедера (обновляем только тексты)
      document.getElementById('ui-header-block').innerHTML = 
          '<div><br>👋 <b>' + lang.hi + ', ' + firstName + '!</b></div>' +
          '<div style="margin-top:8px; font-size:13px; color:var(--text-secondary);">' + lang.desc + '</div>' +
          '<div class="status-group" style="border-left-color: ' + (data.isConnected ? '#4bb34b' : '#eb4242') + '; margin-top:15px;">' +
              '<div>⚙️ ' + lang.status + ': ' + (data.isConnected ? '✅ <span style="color:#4bb34b; font-weight:bold;">' + lang.connected + ' ' + (data.providerName || '') + '</span>' : lang.notSet) + '</div>' +
              '<div id="curFolderLabel">📂 ' + lang.folder + ': ' + (data.isConnected ? '<b>' + (data.currentFolder || '') + '</b>' : lang.notSelected) + '</div>' +
          '</div>';
    }
    
    function renderCommands(data) {
      if (!data) return;
      var container = document.getElementById('ui-commands-block');
      if (!container) return; // Защита от падения, если элемент не найден
      var html = '';
      if (data.isConnected) {
        html += '<span class="blue-link" onclick="openFolderSelector()">/folder</span> — 📂 Выбрать папку для загрузки<br>';
        html += '<span class="blue-link" onclick="shareApp()">/share</span> — 👤 Ссылка для друга<br>';
        html += '<span class="blue-link" onclick="goToSearch()">/search</span> — 🔎 Поиск файлов по хранилке<br>';
      }
        html += '<span class="blue-link" onclick="togglePanel(' + "'debugPanel'" + ')">/debug</span> — 🛠️ Техническая информация<br>';    
      if (data.isConnected) {
        html += '<span class="blue-link" onclick="disconnect()" style="color:#ff3347;">/disconnect</span> — 🔌 Отключить диск<br>';    
      }
      container.innerHTML = html;
    }
    
    function renderAuth(data) {
      if (!data) return;
      var container = document.getElementById('authButtons');
      if (!container) return;
      // Извлекаем данные из объекта data, который пришел с сервера
      var provider = data.provider || null;
      var isMailRu = data.webdav_host && data.webdav_host.indexOf('mail.ru') !== -1;
      var html = '';
      // Яндекс
      html += '<button class="btn-s ' + (provider === 'yandex' ? 'active' : '') + '" onclick="openAuthLink(' + "'/auth/yandex'" + ')">';
      html += '<img src="' + UI_CDN + '/YandexDisk.png"> Яндекс Диск ' + (provider === 'yandex' ? '<span class="check-mark">✅</span>' : '') + '</button>';
      // Google
      html += '<button class="btn-s ' + (provider === 'google' ? 'active' : '') + '" onclick="openAuthLink(' + "'/auth/google'" + ')">';
      html += '<img src="' + UI_CDN + '/GoogleDrive.png"> Google Drive ' + (provider === 'google' ? '<span class="check-mark">✅</span>' : '') + '</button>';
      // Dropbox
      html += '<button class="btn-s ' + (provider === 'dropbox' ? 'active' : '') + '" onclick="openAuthLink(' + "'/auth/dropbox'" + ')">';
      html += '<img src="' + UI_CDN + '/Dropbox.png"> Dropbox ' + (provider === 'dropbox' ? '<span class="check-mark">✅</span>' : '') + '</button>';
      // Mail.ru
      html += '<button class="btn-s ' + (provider === 'webdav' && isMailRu ? 'active' : '') + '" onclick="showMailRu()">';
      html += '<img src="' + UI_CDN + '/CloudMailRu.png"> Облако Mail.ru ' + (isMailRu ? '<span class="check-mark">✅</span>' : '') + '</button>';
      // Свой WebDAV
      html += '<button class="btn-s" onclick="showCustomWD()">';
      html += '<img src="' + UI_CDN + '/network-drive.png"> Свой FTP/SFTP/WebDAV</button>';
      // Друг
      html += '<button class="btn-s" onclick="openFriendsStorage()">🤝 Подключить Хранилку друга</button>';
      // Чат
      html += '<button class="btn-s" style="margin-top: 12px; background: #2688eb; color: #fff; border: none;" onclick="goToChat()">💬 Открыть чат Хранилку</button>';
      container.innerHTML = html;
    }

    function renderDebug(data) {
      const container = document.getElementById('debugContent');
      if (!container) return;
      // Используем данные из аргумента data, а не глобальные переменные
      const isConn = !!data.isConnected;
      const dProv = isConn ? (data.providerName || data.provider || '-') : '-';
      const dFold = isConn ? (data.currentFolder || 'Root') : '-';
      const dAdmin = data.isAdmin ? 'Да' : 'Нет';
      container.innerHTML = 
          '<div>🗄 <b>Приложение онлайн</b></div>' +
          '<div>📦 <b>Версия:</b> ' + "${version}" + '</div>' +
          '<div>🔗 <b>Статус:</b> ' + (isConn ? '✅ Соединение активно' : '❌ Не подключено') + '</div>' +
          '<div>☁️ <b>Провайдер:</b> ' + (isConn ? (data.providerName || data.provider) : '-') + '</div>' +
          '<div>📂 <b>Папка:</b> ' + (isConn ? (data.currentFolder || 'Root') : '-') + '</div>' +
          '<div>👤 <b>Твой ID:</b> ' + userId + '</div>' + 
          '<div>👑 <b>Админ:</b> ' + (data.isAdmin ? 'Да' : 'Нет') + '</div>';
    }

    function closeApp() {
      vkBridge.send('VKWebAppClose', { status: 'success' });
    }

    function uiReload() {
      const icon = document.getElementById('reloadIcon');
      if (icon) {
        icon.classList.add('loading');
        icon.innerText = '💤'; // Меняем символ ⟳ на сон
      }
      refreshData();
    }
    
    // Функция переключения языка
    window.toggleLanguage = function() {
      currentLang = (currentLang === 'ru' ? 'en' : 'ru');
      localStorage.setItem('appLang', currentLang);
      
      // ВАЖНО: проверяем, что здесь именно ЭМОДЗИ, а не буквы
      const langIcon = document.getElementById('langIcon');
      if (langIcon) {
          langIcon.innerHTML = (currentLang === 'ru' ? '🇷🇺' : '🇺🇸');
      }
  
      if (window.lastHeaderData) renderHeader(window.lastHeaderData);
    };
  
    function updateLanguageUI() {
      const langBtn = document.getElementById('langToggle');
      if (langBtn) {
          langBtn.innerText = (currentLang === 'ru' ? '🇷🇺' : '🇺🇸');
      }
  
      // Если данные хедера уже были загружены, перерисовываем его
      if (window.lastHeaderData) {
          renderHeader(window.lastHeaderData);
      } else {
          // Если данных нет, просто перезагрузим интерфейс (опционально)
          // uiReload(); 
      }
    }

    async function refreshData() {
      const icon = document.getElementById('reloadIcon');
      const currentUserId = userId || new URLSearchParams(window.location.search).get('vk_user_id');
      if (!currentUserId) {
          console.error("Критическая ошибка: userId не найден в URL");
          return;
      }
      try {
          const nameParam = window.userName ? '&name=' + encodeURIComponent(window.userName) : '';
          const photoParam = window.userPhoto ? '&photo=' + encodeURIComponent(window.userPhoto) : '';
          const response = await fetch('?action=get-status&userId=' + userId + nameParam + photoParam + '&t=' + Date.now());
          const contentType = response.headers.get("content-type");
          if (!contentType || !contentType.includes("application/json")) {
              throw new TypeError("Сервер вернул не JSON, а " + contentType);
          }
          const data = await response.json();
          if (data.userPhoto) window.currentUserPhoto = data.userPhoto;
          
          // Обновляем только UI блоки, чат ИИ не трогаем
          renderHeader(data);
          renderCommands(data);
          renderAuth(data);
          renderDebug(data);
          
          // ОБНОВЛЯЕМ ГЛОБАЛЬНЫЙ ПРОВАЙДЕР
          if (data.provider) window.currentProvider = data.provider;
          // ОБНОВЛЯЕМ КВОТУ (если диск подключен)
          if (data.isConnected && typeof updateQuota === 'function') {
            updateQuota();
          }
          // НОВАЯ ЛОГИКА: Авто-открытие папок после авторизации
          if (data.isConnected && localStorage.getItem('pending_folder_select') === 'true') {
              localStorage.removeItem('pending_folder_select');
              // Даем небольшую задержку, чтобы UI успел отрисоваться
              setTimeout(() => {
                  if (typeof openFolderSelector === 'function') {
                      openFolderSelector();
                  }
              }, 500);
          }
          // --- НОВЫЙ БЛОК ДЛЯ ОБНОВЛЕНИЯ ПОИСКА ---
          // Обновляем глобальные переменные (на всякий случай)
          window.currentProvider = data.provider;
          window.currentFolder = data.currentFolder;

          const searchInput = document.getElementById('searchInput');
          if (searchInput && searchInput.value.trim() !== "") {
              // ПЕРЕДАЕМ АКТУАЛЬНЫЕ ДАННЫЕ В ПОИСК ПРИНУДИТЕЛЬНО
              doSearch(searchInput.value.trim(), data.provider, data.currentFolder);
          }
      } catch (e) {
          console.error('Ошибка обновления UI:', e);
          // Если упало — вернем обычную иконку через время
      } finally {
          if (icon) {
              setTimeout(() => {
                  icon.classList.remove('loading');
                  icon.innerText = '⟳'; // Возвращаем стрелочку
              }, 500);
          }
      }
    }

    async function sendToAI() {
      const input = document.getElementById('ai-input');
      const text = input.value.trim();
      if (!text) return;
  
      const aiHistory = document.getElementById('ai-chat-history');
      aiHistory.style.display = 'flex'; // Убеждаемся, что флекс включен
      aiHistory.style.flexDirection = 'column'; // Включаем вертикальный режим
      // Ссылка на аватарку сообщества
      const COMMUNITY_AVATAR = "https://sun93-1.userapi.com/b97pMvMc003zZNmW_KT7Jf1ADR9rGGZ5ZQKPYQ/IIoSYBOMotM.jpg";

      // --- 1. СООБЩЕНИЕ ПОЛЬЗОВАТЕЛЯ ---
      const uMsg = document.createElement('div');
      uMsg.className = 'chat-msg user-msg';
      
      // Собираем HTML через плюсы
      uMsg.innerHTML = 
          '<img src="' + (window.userPhoto || '') + '" class="chat-ava">' +
          '<div class="msg-content">' +
              '<div class="msg-name">' + (window.userName || 'Я') + '</div>' +
              '<div class="msg-text">' + text + '</div>' +
          '</div>';
      
      aiHistory.appendChild(uMsg);
      input.value = '';
      
      // 2. Песочные часы (лоадер)
      const loader = document.createElement('div');
      loader.className = 'loading-msg';
      loader.id = 'temp-loader';
      loader.innerHTML = '<span>⌛</span> Запрос отправлен...';
      aiHistory.appendChild(loader);
      
      aiHistory.scrollTop = aiHistory.scrollHeight;
      try {
          const apiUrl = window.location.origin + window.location.pathname + 
                         '?action=ai_chat&state=' + userId + '&text=' + encodeURIComponent(text);
  
          const response = await fetch(apiUrl, {
              method: 'GET',
              headers: { 'Accept': 'application/json' }
          });
  
          if (!response.ok) throw new Error('Status: ' + response.status);
  
          const data = await response.json();

          // Удаляем лоадер перед выводом ответа
          const loaderNode = document.getElementById('temp-loader');
          if (loaderNode) loaderNode.remove();
          
          // --- 3. СООБЩЕНИЕ AI ---
          const aiMsg = document.createElement('div');
          aiMsg.className = 'chat-msg ai-msg';
          aiMsg.innerHTML = 
              '<img src="' + COMMUNITY_AVATAR + '" class="chat-ava">' +
              '<div class="msg-content">' +
                  '<div class="msg-name">Leshiy-AI</div>' +
                  '<div class="msg-text">' + (data.answer || 'Пустой ответ') + '</div>' +
              '</div>';
          
          aiHistory.appendChild(aiMsg);
      } catch (e) {
          const loaderNode = document.getElementById('temp-loader');
          if (loaderNode) loaderNode.remove();
          const err = document.createElement('div');
          err.className = 'chat-msg ai-msg';
          err.style.color = 'red';
          err.innerText = 'Ошибка: ' + e.message;
          aiHistory.appendChild(err);
      }
      aiHistory.scrollTop = aiHistory.scrollHeight;
    }
    
    aiBtn.onclick = sendToAI;
    aiInput.onkeydown = function(e) { if(e.key === 'Enter') sendToAI(); };

    function showCustomWD() {
      document.getElementById('wdContent').innerHTML = \`
        <div class="msg-header">📁 Подключение своего сервера</div>
        <div class="wd-info-box">
          <b>Поддерживаются следующие протоколы:</b><br>
          ✅ WebDAV (рекомендуется) — работает в Cloudflare Workers<br><br>
          🔗 <b>Формат для WebDAV:</b><br>
          https://user:pass@ваш-сервер.ru<br><br>
          ❌ FTP / SFTP — НЕ работают в Cloudflare Workers<br>
          📘 Используйте <a href="https://github.com/leshiy-ai/leshiy-storage-bot" target="_blank">Python-версию бота</a> для FTP/SFTP (на Render/VPS).<br>
          Это полноценный продукт для личного хостинга.<br><br>
          Укажи ссылку в формате:
        </div>
        <div style="font-size:11px; margin-bottom:12px; word-break:break-all; color:#2688eb;">https://ваша@почта:пароль_для_внешнего_приложения@webdav.yandex.ru</div>
      \`;
      togglePanel('wdForm');
    }

    function showMailRu() {
      document.getElementById('wdContent').innerHTML = \`
        <div class="msg-header">📧 Облако Mail.ru через WebDAV</div>
        <div class="wd-info-box">
          1. Перейди в Настройки → «Пароли для внешних приложений»<br>
          2. Создай пароль для WebDAV<br>
          3. Укажи ссылку в формате ниже:
        </div>
        <div style="font-size:11px; margin-bottom:12px; word-break:break-all; color:#2688eb;">https://ваша-почта@mail.ru:пароль_для_внешнего_приложения@webdav.cloud.mail.ru</div>
      \`;
      document.getElementById('wdHost').value = "https://webdav.cloud.mail.ru";
      document.getElementById('wdFolder').value = "Storage";
      togglePanel('wdForm');
    }

    function togglePanel(id) {
      const el = document.getElementById(id);
      if (!el) return;
    
      const isVisible = el.style.display === 'block';
    
      if (!isVisible) {
        // 1. Сначала принудительно возвращаем её "домой"
        // Если это дебаг или админ, сбрасываем transform в исходное состояние
        el.style.opacity = '1';
        // 2. Делаем видимой
        el.style.display = 'block';
        // 3. Инициализируем свайп (без поворота для дебага)
        if (!el.dataset.swipable) {
          makeSwipable(el, null, false);
          el.dataset.swipable = "true";
        }
        // 4. Теперь скролл сработает корректно, так как панель уже в центре
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      } else {
        el.style.display = 'none';
      }
    }

    function goToChat() {
      vkBridge.send("VKWebAppOpenExternalLink", { "url": "https://vk.com/write-" + groupId })
        .catch(() => { window.open("https://vk.com/write-" + groupId, "_blank"); });
    }

    function openAuthLink(path) {
      // Прямое получение платформы и UID здесь и сейчас
      const platform = getLaunchParam('vk_platform') || '';
      const userId = getLaunchParam('vk_user_id') || '';
      const isMobileWeb = platform === 'mobile_web' || platform === 'mvk_external';
      const isNativeApp = ['mobile_android', 'mobile_iphone', 'mobile_ipad', 'mobile_android_messenger', 'mobile_iphone_messenger'].includes(platform);

      // Формируем ссылку
      const url = "https://leshiy-storage-bot.leshiyalex.workers.dev" + path + "?state=" + userId;
      // Метка для рефреша
      localStorage.setItem('awaiting_auth', 'true');
      // ЛОГИКА ОТКРЫТИЯ
      if (isNativeApp) {
        // А. Если мы в нативном мобильном приложении ВК
        vkBridge.send("VKWebAppOpenExternalLink", { "url": url })
          .catch(() => { window.location.href = url; });
      } else if (isMobileWeb) {
        // Б. Если это мобильный браузер (m.vk.com) - самый проблемный случай
        // Используем replace, чтобы не плодить историю и проскочить away.php
        window.location.replace(url);
      } else {
        // В. Все остальные случаи (ПК)
        const win = window.open(url, "_blank"); // Объявляем переменную здесь!
        
        // Если браузер заблокировал поп-ап (win будет null), идем через href
        if (!win || win.closed || typeof win.closed === 'undefined') {
          window.location.href = url;
        }
      }
    }
    
    function openLink(path) {
      const url = "https://leshiy-storage-bot.leshiyalex.workers.dev" + path + "?state=" + userId;
      
      // 1. Ставим метку для рефреша (LocalStorage работает везде)
      localStorage.setItem('awaiting_auth', 'true');
      // 2. Определяем среду
      const isVkEnv = window.location.search.includes('vk_app_id') || window.name.includes('fXD');
      const bridgeExists = (typeof vkBridge !== 'undefined' && vkBridge.send);
    
      if (isVkEnv && bridgeExists) {
        // Сценарий: Мы в ВК и Bridge живой
        vkBridge.send("VKWebAppOpenExternalLink", { "url": url })
          .catch(() => { 
            // Если на ПК Bridge "проглотил" вызов, но не открыл окно
            window.open(url, "_blank"); 
          });
      } else {
        // Сценарий: Мы открыты просто во вкладке или Bridge не загрузился
        // Используем window.open для новой вкладки или location.href для текущей
        window.location.href = url;
      }
    }
    
    function parseUrl(v) {
      try {
        if (v.includes('://')) {
          const u = new URL(v);
          if(u.username) document.getElementById('wdUser').value = decodeURIComponent(u.username);
          if(u.password) document.getElementById('wdPass').value = decodeURIComponent(u.password);
          document.getElementById('wdHost').value = u.hostname;
          document.getElementById('wdFolder').value = "Storage";
        }
      } catch(e) {}
    }

    function setupMailRu() {
      document.getElementById('wdHost').value = "https://webdav.cloud.mail.ru";
      document.getElementById('wdFolder').value = "Storage";
      document.getElementById('wdForm').style.display = 'block';
      document.getElementById('wdForm').scrollIntoView({behavior: 'smooth'});
      f.style.display = f.style.display === 'block' ? 'none' : 'block';
    }

    function toggleWD() {
      const f = document.getElementById('wdForm');
      f.style.display = f.style.display === 'block' ? 'none' : 'block';
    }

    async function saveWebDAV() {
      const b = document.getElementById('saveBtn');
      const panel = document.getElementById('wdForm');
      let h = document.getElementById('wdHost').value.trim();
      if (!h.startsWith('http://') && !h.startsWith('https://')) {
        h = 'https://' + h;
      }
      const u = document.getElementById('wdUser').value; 
      const p = document.getElementById('wdPass').value;
      const f = document.getElementById('wdFolder').value;
      if(!h || !u || !p || !f) return alert("Заполните все поля");
      b.disabled = true; b.innerText = "💾 Сохраняю и подключаю...";
      try {
        console.log("Отправка данных WebDAV на сервер...");
        const res = await fetch('/api/setup-webdav', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ userId, host: h, user: u, pass: p, folderId: f })
        });
        if(res.ok) {
          // 1. Сразу закрываем панель настройки (чтобы не висела)
            if (panel) {
                panel.style.display = 'none'; // Скрываем блок формы
            }
          
          // 2. Обновляем локальные данные, чтобы не ждать ответа сервера
          window.currentProvider = 'webdav';
          window.currentFolder = f;

          // 2. Если нужно сразу показать папки (без фокуса)
          if (typeof openFolderSelector === 'function') {
              openFolderSelector(); 
          }
          // 3. Сбрасываем состояние кнопки для следующего раза
          const b = document.getElementById('saveBtn');
          b.disabled = false;
          b.innerText = "📥 Подключиться";

          // 4. И только потом делаем тихий рефреш данных в фоне
          refreshData(); 
        } else {
          b.disabled = false;
          b.innerText = "🔌 Подключиться";
        }
      } catch(e) {
        b.disabled = false;
        b.innerText = "🔌 Подключиться";
      }
    }

    let searchDebounce;

    // Открываем поиск
    function goToSearch() {
        document.getElementById('searchModal').style.display = 'flex';
        document.getElementById('searchInput').focus();
    }

    function closeSearch() {
        document.getElementById('searchModal').style.display = 'none';
        document.getElementById('searchInput').value = '';
        document.getElementById('searchList').innerHTML = '<div style="text-align:center; color:#818c99; margin-top:40px;">Введите название файла для поиска</div>';
    }

    async function doSearch(query, providerOverride, folderOverride) {
      clearTimeout(searchDebounce);
      var list = document.getElementById('searchList');
      
      if (!query || query.trim().length === 0) {
          list.innerHTML = '<div style="text-align:center; color:#818c99; margin-top:40px;">Введите название файла для поиска</div>';
          return;
      }
  
      searchDebounce = setTimeout(async function() {
          list.innerHTML = '<div style="text-align:center; color:#818c99; margin-top:20px;">🔍 Ищу...</div>';
          
          try {
              // Определяем, какие данные использовать: 
              // Если переданы из refreshData — берем их, если нет — берем глобальные
              var activeProvider = providerOverride || window.currentProvider;
              var activeFolder = folderOverride || window.currentFolder;
              // Используем window.userId или userId (смотря как у тебя в коде объявлено)
              var currentUid = window.userId || userId;
              var response = await fetch('/api/search?q=' + encodeURIComponent(query), {
                  headers: { 'x-vk-user-id': currentUid }
              });
              var data = await response.json();
              
              if (!data.results || data.results.length === 0) {
                  list.innerHTML = '<div style="text-align:center; color:#818c99; margin-top:40px;">Ничего не найдено</div>';
                  return;
              }
  
              var html = '';
              for (var i = 0; i < data.results.length; i++) {
                // Внутри цикла for в функции doSearch
                var file = data.results[i];
                var date = new Date(file.timestamp).toLocaleDateString('ru-RU');
                var currentUid = window.userId || userId;
                var fileFolder = file.remotePath.split('/')[0];
                // ВАЖНО: Мы сравниваем провайдера и папку напрямую из объекта файла
                var isSameProvider = (file.provider === activeProvider);
                var isSameFolder = (fileFolder === activeFolder);
            
                var statusText, statusColor, borderStyle, canDownload;
                // 1. Сначала подправим базовый стиль (В светлой он будет черным/темным, в темной — белым/светлым.)
                var badgeStyle = 'font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 800; color: var(--text-color) !important; border: 1px solid ';
                if (isSameProvider && isSameFolder) {
                  statusText = '● Доступен';
                  statusColor = '#4bb34b';
                  // Добавляем прозрачность фону, чтобы текст на нем читался в обеих темах
                  var providerBadge = badgeStyle + statusColor + '; background: ' + statusColor + '33;';
                  var folderBadge = badgeStyle + statusColor + '; background: ' + statusColor + '33;';
                  canDownload = true;
                } else if (isSameProvider && !isSameFolder) {
                  statusText = '● Не доступен (Другая папка)';
                  statusColor = '#ffc107'; 
                  var providerBadge = badgeStyle + '#4bb34b; background: #4bb34b33;';
                  var folderBadge = badgeStyle + statusColor + '; background: ' + statusColor + '33;';
                  canDownload = true;
                } else {
                  statusText = '● Не доступен (Другой диск)';
                  statusColor = '#99a2ad';
                  // Для нейтральных бейджей используем вторичный цвет текста
                  var providerBadge = badgeStyle + 'var(--border-color); background: var(--bubble-bg); color: var(--text-secondary) !important;';
                  var folderBadge = badgeStyle + 'var(--border-color); background: var(--bubble-bg); color: var(--text-secondary) !important;';
                  canDownload = false;
                }
 
                // Определяем иконку по расширению
                var ext = file.fileName.split('.').pop().toLowerCase();
                var icon = '📄';
                if (['jpg', 'jpeg', 'png', 'gif'].includes(ext)) icon = '🖼️';
                if (['mp4', 'mov', 'avi', 'wmv'].includes(ext)) icon = '🎬';
                if (['mp3', 'wav'].includes(ext)) icon = '🎵';
                if (['ogg', 'oga'].includes(ext)) icon = '🎙️';

                // Ссылка на скачивание
                var downloadUrl = 'https://leshiy-storage-bot.leshiyalex.workers.dev/api/download' +
                                  '?path=' + encodeURIComponent(file.remotePath) +
                                  '&name=' + encodeURIComponent(file.fileName) +
                                  '&userId=' + currentUid;

                html += '<div class="search-result-item" style="border-left: 4px solid #4bb34b; position: relative; padding-left: 45px;">' +
                            // Зеленый индикатор (в базе есть = считаем доступным)
                            '<div style="position: absolute; left: 12px; top: 50%; transform: translateY(-50%); font-size: 20px;">' + icon + '</div>' +
                            
                            '<div class="file-info">' +
                                '<span class="file-name">' + file.fileName + '</span>' +
                                '<div style="display: flex; gap: 8px; align-items: center; margin-top: 2px;">' +
                                    '<span class="file-date">' + date + '</span>' +
                                    '<span style="' + providerBadge + '; color: #555;">' + file.provider + '</span>' +
                                    '<span style="' + folderBadge + '; color: #555;">' + fileFolder + '</span>' +
                                    '<span style="color: ' + statusColor + '; font-size: 10px; font-weight: 500;">' + statusText + '</span>' +
                                '</div>' +
                            '</div>' +
                            
                            '<a href="' + downloadUrl + '" target="_blank" class="download-link" style="border-radius: 6px; margin-left: 10px;">' + '⬇️ Скачать' + '</a>' +
                        '</div>';
              }
              list.innerHTML = html;
              
          } catch (e) {
              console.error("Search error:", e);
              list.innerHTML = '<div style="text-align:center; color:#ff4d4f; margin-top:20px;">Ошибка поиска: ' + e.message + '</div>';
          }
      }, 400);
    }

    function showDebug() { 
      const w = document.getElementById('debugWindow');
      w.style.display = w.style.display === 'block' ? 'none' : 'block';
    }

    function showAdmin() {
      const w = document.getElementById('adminWindow');
      if(w) w.style.display = w.style.display === 'block' ? 'none' : 'block';
    }

    function openAiSettings() {
      togglePanel("aiSettingsPanel");
      // ✅ ВСЕГДА ОБНОВЛЯЙ СТАТУС ПРИ ОТКРЫТИИ ПАНЕЛИ
      updateCurrentAiStatus();
    }
    
    function updateCurrentAiStatus() {
      const st = document.getElementById("aiCurrentStatus");
      // Добавляем timestamp, чтобы обойти кэш
      fetch("/api/admin/get-ai-settings?vk_user_id=" + userId + "&t=" + Date.now())
        .then(r => r.json())
        .then(d => {
          let txt = "📊 <b>Текущие модели:</b><br>";
          for (const k in d.services) {
            const s = d.services[k];
            txt += "• " + s.name + ": " + s.currentModelName + "<br>";
          }
          st.innerHTML = txt;
        })
        .catch(e => {
          st.innerHTML = "❌ Ошибка загрузки";
          console.error(e);
        });
    }
    
    function loadModels(el) {
      const type = el.id; // Например, TEXT_TO_TEXT
      const panel = document.getElementById("modelsList");
      panel.style.display = "block";
      
      // МГНОВЕННАЯ ГЕНЕРАЦИЯ ИЗ ПАМЯТИ
      let html = "<b>Доступные модели для " + type + ":</b><br>";
      
      // Перебираем ключи локального объекта
      Object.keys(allAiModels).forEach(key => {
        if (key.startsWith(type)) {
          const modelName = allAiModels[key].MODEL;
          const serviceName = allAiModels[key].SERVICE;
          const displayName = "<b>" + serviceName + "</b>: " + modelName;
          html += '<div class="chat-btn" id="' + key + '" title="' + type + '" onclick="applyModel(this)">' + displayName + '</div>';
        }
      });
    
      panel.innerHTML = html;
      panel.scrollIntoView({behavior: 'smooth'});
    }
    
    function applyModel(el) {
      const modelId = el.id; 
      const type = el.getAttribute("title");
    
      if (!confirm("Выбрать модель " + modelId + "?")) return;
    
      const statusBox = document.getElementById("aiCurrentStatus");
      const originalText = el.innerText; // Сохраняем имя (например, Gemini)
    
      // 1. Включаем индикацию загрузки
      el.innerText = "⏳ Сохранение...";
      el.style.pointerEvents = "none";
      statusBox.style.opacity = "0.5";
    
      fetch("/api/admin/set-model?vk_user_id=" + userId, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId, type, model: modelId })
      })
      .then(r => r.json())
      .then(res => {
        if (res.success) {
          // Мгновенно подменяем текст в черном блоке вручную (Оптимистичный UI)
          // Чтобы не ждать KV, мы сами знаем, что выбрали
          const modelName = allAiModels[modelId] ? allAiModels[modelId].MODEL : modelId;
          console.log("Модель успешно изменена на:", modelName);
          
          alert("✅ Установлено!");
          
          // Скрываем список и обновляем статус
          document.getElementById("modelsList").style.display = "none";
          if (typeof updateCurrentAiStatus === "function") {
              updateCurrentAiStatus();
          }
        } else {
        }
      })
      .catch(err => {
        console.error("Fetch error:", err);
      })
      .finally(() => {
        // 2. В ЛЮБОМ СЛУЧАЕ возвращаем кнопку и статус в норму
        el.innerText = originalText;
        el.style.pointerEvents = "auto";
        statusBox.style.opacity = "1";
      });
    }

    async function disconnect() {
      if(confirm("Отключить хранилище?")) {
        await fetch('/api/disconnect', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({userId}) });
        localStorage.setItem('awaiting_auth', 'true'); 
        uiReload();
      }
    }

    // 1. Глобальное состояние
    var uploadQueue = [];
    var isUploading = false;

    function uploadFileFromVK(input) {
      var files = input.files;
      if (!files || files.length === 0) return;
  
      var container = document.getElementById('dropZone');
  
      // 1. Очистка старых строк (кроме тех, что в очереди или грузятся)
      var oldRows = container.querySelectorAll('.upload-row');
      oldRows.forEach(function(r) {
          var status = r.getAttribute('data-status');
          if (status === 'done' || status === 'error' || status === 'cancelled' || status === 'warning') {
              r.remove();
          }
      });
  
      for (var i = 0; i < files.length; i++) {
          var file = files[i];
          var fileId = 'f' + Date.now() + i;
  
          var row = document.createElement('div');
          row.id = fileId;
          row.className = 'upload-row';
          row.setAttribute('data-status', 'waiting');
          row.style.cssText = 'margin-top:10px; padding:10px; background:#fff; border-radius:8px; border:1px solid #dce1e6; text-align:left; position:relative;';
  
          // ТВОЙ СТАБИЛЬНЫЙ ВАРИАНТ (без onclick внутри строки)
          row.innerHTML = 
            '<div class="info" style="font-size:12px; display:flex; justify-content:space-between;">' +
                '<span>⌛ В очереди: <b>' + file.name + '</b></span>' +
                '<span class="cancel-btn" style="color:#ff4d4f; cursor:pointer; font-size:11px; text-decoration:underline;">Отмена</span>' +
            '</div>' +
            // Высота 6px и убрали внутренний блок pct
            '<div style="width:100%; background:#dce1e6; height:6px; border-radius:2px; overflow:hidden; position:relative; margin-top:8px;">' +
                '<div class="bar" style="width:0%; background:#2688eb; height:100%; transition:width 0.2s;"></div>' +
            '</div>';
  
          container.appendChild(row);
  
          // 2. Создаем задачу
          var task = {
              id: fileId,
              file: file,
              fileName: file.name,
              row: row,
              bar: row.querySelector('.bar'),
              pct: row.querySelector('.pct'),
              info: row.querySelector('.info span'),
              xhr: null
          };
  
          // 3. ПРАВИЛЬНАЯ ПРИВЯЗКА КНОПКИ (Через замыкание)
          // Мы используем let или создаем отдельную область видимости, чтобы id не перепутались
          (function(currentId) {
              row.querySelector('.cancel-btn').onclick = function() {
                  cancelUploadTask(currentId);
              };
          })(fileId);
  
          uploadQueue.push(task);
      }
  
      input.value = '';
      if (!isUploading) processQueue();
    }

    function cancelUploadTask(id) {
      var taskIndex = uploadQueue.findIndex(t => t.id === id);
      if (taskIndex === -1) return;
  
      var task = uploadQueue[taskIndex];
      var row = document.getElementById(id);
      var wasUploading = (row.getAttribute('data-status') === 'uploading');
  
      if (task.xhr) task.xhr.abort();
  
      // Визуальное оформление отмены
      row.setAttribute('data-status', 'cancelled');
      row.style.opacity = '0.5';
      var infoSpan = row.querySelector('.info span');
      if (infoSpan) infoSpan.innerHTML = '🔘 Отменено: <b>' + task.fileName + '</b>';
  
      // Меняем кнопку на "Вернуть"
      var btn = row.querySelector('.cancel-btn');
      if (btn) {
          btn.innerHTML = 'Вернуть';
          btn.style.color = '#2688eb';
          btn.onclick = function() {
              restoreUploadTask(id);
          };
      }
  
      if (wasUploading) {
          isUploading = false;
          setTimeout(processQueue, 100);
      }
    }

    function restoreUploadTask(id) {
      var row = document.getElementById(id);
      var task = uploadQueue.find(t => t.id === id);
      if (!task) return;
  
      // Возвращаем исходный вид
      row.setAttribute('data-status', 'waiting');
      row.style.opacity = '1';
      var infoSpan = row.querySelector('.info span');
      if (infoSpan) infoSpan.innerHTML = '⌛ В очереди: <b>' + task.fileName + '</b>';
  
      // Возвращаем кнопку "Отмена"
      var btn = row.querySelector('.cancel-btn');
      if (btn) {
          btn.innerHTML = 'Отмена';
          btn.style.color = '#ff4d4f';
          btn.onclick = function() {
              cancelUploadTask(id);
          };
      }
  
      // Если ничего не грузится — запускаем
      if (!isUploading) processQueue();
    }
    
    function applyCancelledStyle(row) {
      row.setAttribute('data-status', 'cancelled');
      row.style.opacity = '0.5';
      var bar = row.querySelector('.bar');
      if (bar) {
          bar.style.background = '#999';
          bar.style.width = '100%';
      }
      // Обновляем только текст статуса, сохраняя имя файла
      var infoSpan = row.querySelector('.info span');
      if (infoSpan) {
          var fileNameTag = infoSpan.querySelector('b');
          var fileName = fileNameTag ? fileNameTag.innerText : "Файл";
          infoSpan.innerHTML = '🔘 Отменено: <b>' + fileName + '</b>';
      }
      var btn = row.querySelector('.cancel-btn');
      if (btn) btn.style.display = 'none';
    }

    async function retryConfirm(task) {
      var btn = task.row.querySelector('.cancel-btn');
      if (btn) btn.style.display = 'none'; // Прячем кнопку на время попытки
      
      task.info.innerHTML = '🔄 Перезапись... Файл: <b>' + task.fileName + '</b>';
  
      try {
          const confResponse = await fetch('https://leshiy-storage-bot.leshiyalex.workers.dev/api/confirm-upload', {
              method: 'POST',
              headers: {
                  'Content-Type': 'application/json',
                  'x-vk-user-id': window.userId || userId
              },
              body: JSON.stringify({
                  fileName: task.fileName,
                  fileSize: task.file.size
              })
          });
          
          await confResponse.json();
  
          // Если получилось
          task.row.setAttribute('data-status', 'done');
          task.info.innerHTML = '✅ Готово! Файл: <b>' + task.fileName + '</b>';
          if (task.bar) task.bar.style.background = '#28a745';
      } catch (e) {
          // Если опять не вышло — возвращаем кнопку "Повторить"
          task.info.innerHTML = '⚠️ Снова ошибка базы! Файл: <b>' + task.fileName + '</b>';
          if (btn) {
              btn.style.display = 'inline';
              btn.innerHTML = 'Повторить';
          }
      }
    }
    
    // Основная функция обработки (ЕДИНСТВЕННАЯ)
    async function processQueue() {
      try {
        if (typeof uploadQueue === 'undefined' || !uploadQueue || isUploading) return;
        var task = uploadQueue.find(t => t.row && t.row.getAttribute('data-status') === 'waiting');
        if (!task || isUploading) return;
    
        isUploading = true;
        task.row.setAttribute('data-status', 'uploading');
        var fileNameHTML = ' Файл: <b>' + task.fileName + '</b>';
    
        if (currentProvider === 'webdav') {
          // WebDav эндпоинт /api/upload-from-vk
          const uploadRes = await fetch('/api/upload-from-vk', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/octet-stream',
              'Content-Length': task.file.size.toString(),
              'x-file-name': encodeURIComponent(task.fileName),
              'x-vk-user-id': userId
            },
            body: task.file
          });
    
          if (!uploadRes.ok) {
            const errorData = await uploadRes.json().catch(() => ({}));
            throw new Error(errorData.error || "Ошибка загрузки в Webdav");
          }
    
          // Успех
          task.row.setAttribute('data-status', 'done');
          task.info.innerHTML = '✅ Готово! ' + fileNameHTML;
          if (task.bar) { task.bar.style.background = '#28a745'; task.bar.style.width = '100%'; }
          var btn = task.row.querySelector('.cancel-btn');
          if (btn) btn.style.display = 'none';

        } else if (currentProvider === 'webdav-buffer') {
          // --- WEBDAV: ИСПОЛЬЗУЕМ /api/upload-buffer ---
          const arrayBuffer = await task.file.arrayBuffer();
          const uploadRes = await fetch('/api/upload-buffer', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/octet-stream',
              'x-file-name': encodeURIComponent(task.fileName),
              'x-vk-user-id': window.userId || userId
            },
            body: arrayBuffer
          });
        
          if (!uploadRes.ok) {
            const errorData = await uploadRes.json().catch(() => ({}));
            throw new Error(errorData.error || "Ошибка загрузки в WebDAV");
          }
        
          task.row.setAttribute('data-status', 'done');
          task.info.innerHTML = '✅ Готово! ' + fileNameHTML;
          if (task.bar) { task.bar.style.background = '#28a745'; task.bar.style.width = '100%'; }
          var btn = task.row.querySelector('.cancel-btn');
          if (btn) btn.style.display = 'none';
        } else {
          // --- ВСЕ ОСТАЛЬНЫЕ: как раньше через /api/get-upload-link ---
          const res = await fetch('/api/get-upload-link', {
            method: 'POST',
            headers: {
              'x-file-name': encodeURIComponent(task.fileName),
              'x-file-size': task.file.size.toString(),
              'x-vk-user-id': window.userId || userId
            }
          });
          const plan = await res.json();
          if (!plan.upload_url) throw new Error(plan.error || "Нет ссылки");
    
          // ШАГ 2: Прямая загрузка в облако
          const xhr = new XMLHttpRequest();
          task.xhr = xhr;
          xhr.open(plan.method, plan.upload_url, true);
    
          if (plan.headers) {
            for (let k in plan.headers) {
              xhr.setRequestHeader(k, plan.headers[k]);
            }
          }

          if (plan.provider == 'google') {
            // Не ставим заголовки для гугла
          } else if (plan.provider == 'yandex') {
            xhr.setRequestHeader('Content-Type', task.file.type || 'application/octet-stream');
          } else if (plan.provider == 'dropbox') {
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
          } else if (plan.provider == 'webdav') {
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
          } else {
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
          }

          xhr.upload.onprogress = function(e) {
            if (e.lengthComputable && task.info) {
              var pct = (e.loaded / e.total) * 100;
              if (task.bar) task.bar.style.width = pct + '%';
              task.info.innerHTML = '📤 ' + Math.floor(pct) + '%' + fileNameHTML;
            }
          };
    
          xhr.onload = async function() {
            if (xhr.status >= 200 && xhr.status <= 204) {
              try {
                const confResponse = await fetch('/api/confirm-upload', {
                  method: 'POST',
                  headers: {
                    'Content-Type': 'application/json',
                    'x-vk-user-id': window.userId || userId
                  },
                  body: JSON.stringify({
                    fileName: task.fileName,
                    fileSize: task.file.size
                  })
                });
                await confResponse.json();
    
                task.row.setAttribute('data-status', 'done');
                task.info.innerHTML = '✅ Готово! ' + fileNameHTML;
                if (task.bar) { task.bar.style.background = '#28a745'; task.bar.style.width = '100%'; }
                var btn = task.row.querySelector('.cancel-btn');
                if (btn) btn.style.display = 'none';
              } catch (e) {
                console.error("Ошибка подтверждения:", e);
                task.row.setAttribute('data-status', 'warning');
                task.info.innerHTML = '⚠️ Ошибка базы! ' + fileNameHTML;
                if (task.bar) { task.bar.style.background = '#ffc107'; task.bar.style.width = '100%'; }
                var btn = task.row.querySelector('.cancel-btn');
                if (btn) {
                  btn.innerHTML = 'Повторить';
                  btn.style.color = '#2688eb';
                  btn.onclick = function() { retryConfirm(task); };
                }
              }
            } else {
              task.row.setAttribute('data-status', 'error');
              task.info.innerHTML = '❌ Ошибка облака: ' + xhr.status + fileNameHTML;
            }
            finish();
          };
    
          xhr.onerror = function() {
            task.row.setAttribute('data-status', 'error');
            task.info.innerHTML = '❌ Ошибка сети. Файл: <b>' + task.fileName + '</b>';
            finish();
          };
    
          xhr.send(task.file);
        }
    
      } catch (e) {
        console.error("Ошибка в очереди:", e);
        if (task && task.row) {
          task.row.setAttribute('data-status', 'error');
          task.info.innerHTML = '❌ Ошибка: ' + e.message + '. Файл: <b>' + task.fileName + '</b>';
          if (task.bar) task.bar.style.background = '#ff4d4f';
        }
      } finally {
        finish();
      }
    
      function finish() {
        isUploading = false;
        setTimeout(processQueue, 200);
      }
    }

    // Оживляем Drop Zone (чтобы файлы не открывались в окне)
    var dz = document.getElementById('dropZone');
    if (dz) {
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(function(ev) {
            dz.addEventListener(ev, function(e) {
                e.preventDefault();
                e.stopPropagation();
            }, false);
        });
    
        dz.addEventListener('drop', function(e) {
            var files = e.dataTransfer.files;
            if (files && files.length > 0) {
                uploadFileFromVK({ files: files });
            }
        }, false);
    }

    async function updateQuota() {
      if (!${isConnected}) return;
      try {
        const res = await fetch('/api/get-quota?vk_user_id=' + userId);
        const data = await res.json();
        if (data.total > 0) {
          const percent = Math.round((data.used / data.total) * 100);
          document.getElementById('quotaBar').style.width = percent + '%';
          document.getElementById('quotaText').innerHTML = 'Заполнено на <b>' + percent + '%</b>. Использовано ' + (data.used/1e9).toFixed(1) + ' ГБ из ' + (data.total/1e9).toFixed(1) + ' ГБ';
        }
      } catch(e) {}
    }
    setTimeout(updateQuota, 800);

    // Функция шаринга (вызывается из меню по кнопке /share)
    async function shareApp() {
      const panel = document.getElementById("sharePanel");
      const content = document.getElementById("shareContent");
      // СБРОС СОСТОЯНИЯ ПОСЛЕ СВАЙПА
      panel.style.display = "block";
      panel.style.opacity = "1";
      // Возвращаем панель в центр (учитывай свои стили top/left)
      panel.style.transform = "translate(-50%, -50%)"; // Если у тебя top: 50%
      content.innerHTML = "<span class='loader'>⏳ Генерация инвайт-кода...</span>";

      try {
        const response = await fetch('/api/create-invite?userId=' + userId);
        const data = await response.json();
    
        if (data && data.inviteCode) {
          // Сохраняем ссылку в глобальную переменную (без ключевых слов let/var если она уже есть выше)
          window.pendingInviteLink = 'https://vk.com/app' + appId + '#ref=' + data.inviteCode;
          // Словарь имен провайдеров
          const providerNames = {
              'yandex': 'Яндекс Диск',
              'google': 'Google Drive',
              'mailru': 'Облако Mail.ru',
              'dropbox': 'Dropbox',
              'webdav': 'WebDAV'
          };
          
          // Берем данные для предпросмотра
          const pName = currentProvider || "Google Drive";
          const fName = currentFolder || "Root";
          // Получаем красивое имя или оставляем как есть, если в списке нет
          const pDisplay = providerNames[pName] || pName;

          // Наполняем твой блок shareContent
          // Используем обычные двойные кавычки внутри, чтобы не конфликтовать с твоими бэктиксами
          const inviteUrl = 'https://vk.com/app' + appId + '#ref=' + data.inviteCode;
          content.innerHTML =
          "<div class='modal-content-styled'>" +
            "<span>👋 <b>Что происходит?</b> Вы формируете приглашение на доступ в свою Хранилку. Ваш друг сможет загружать файлы в выбранную Вами в данный момент папку.</span><br><br>" +
            
            "<b>Куда даем доступ:</b><br>" +
            "☁️ <b>Провайдер:</b> " + pDisplay + "<br>" +
            "📁 <b>Папка:</b> " + fName + "<br>" +
            "🎟️ <b>Токен:</b> " + data.inviteCode + "<br><br>" +
            
            "🔗 <b>Ваша ссылка (клик для копирования):</b><br>" +
            "<code onclick='copyToClipboard(this)' class='modal-code-block'>" + inviteUrl + "</code>" +
            "<small class='modal-small-text'>Эта ссылка будет на кнопке <b>Открыть</b> в сообщении.</small><br><br>" +

            "<div class='modal-info-note'>" +
                "📝 <b>Текст сообщения (клик для копирования):</b><br>" +
                "<span onclick='copyToClipboard(this)' style='cursor:pointer; display:block; margin-top:5px; font-style:italic;'>" +
                  "Я предоставил тебе доступ к своей Хранилке. Провайдер: " + pName + ". Папка: " + fName + ". Жми Открыть и подключайся!" + 
                "</span>" +
            "</div><br>" +
            
            "<b>Что дальше?</b><br>" +
            "1️⃣ Нажмите кнопку <b>Отправить</b> ниже.<br>" +
            "2️⃣ Выберите друга в открывшемся списке ВК.<br>" +
            "3️⃣ Если вы на ПК — вставьте скопированный текст в поле сообщения, а на мобильном телефоне он вставится автоматически." +
          "</div>";
            // Показываем твою панель
          document.getElementById("sharePanel").style.display = "block";
          const sharePanel = document.getElementById("sharePanel");
          makeSwipable(sharePanel);
        }
      } catch (error) {
        console.error("Ошибка при подготовке шаринга:", error);
      }
    }

    function copyToClipboard(element) {
      const text = element.innerText;
      
      // Создаем временный элемент
      const textArea = document.createElement("textarea");
      textArea.value = text;
      
      // Прячем его за пределами экрана
      textArea.style.position = "fixed";
      textArea.style.left = "-9999px";
      textArea.style.top = "0";
      document.body.appendChild(textArea);
      
      // Выделяем и копируем
      textArea.focus();
      textArea.select();
      
      try {
          const successful = document.execCommand('copy');
          if (successful) {
              // Визуальный эффект
              const oldColor = element.style.color;
              const oldText = element.innerHTML;
              element.style.color = "#28a745";
              element.innerText = "✅ Скопировано!";
              
              setTimeout(function() {
                  element.style.color = oldColor;
                  element.innerHTML = oldText;
              }, 1500);
          }
      } catch (err) {
          console.error('Даже старый метод не сработал:', err);
      }
  
      document.body.removeChild(textArea);
    }

    // Функция подтверждения
    function confirmAndShare() {
      const pName = currentProvider || "Google Drive";
      const fName = currentFolder || "Root";
      
      // Формируем текст сообщения БЕЗ обратных слэшей в коде
      // Используем String.fromCharCode(10) для переноса строки
      const nl = String.fromCharCode(10);
      const shareText = "Я предоставил тебе доступ к своей Хранилке. Провайдер: " + pName + ". Папка: " + fName + ". Жми Открыть и подключайся!";
    
      // Вызываем мост (используем 'text' по документации)
      vkBridge.send("VKWebAppShare", { 
        "link": window.pendingInviteLink,
        "text": shareText 
      });
    
      // Закрываем твою панель
      document.getElementById("sharePanel").style.display = "none";
    }

    async function openFolderSelector() {
      const modal = document.getElementById('folderModal');
      const listCont = document.getElementById('modalFolderList');
      modal.style.display = 'flex';
      
      // Очищаем или пишем "Загрузка...", чтобы юзер видел активность
      listCont.innerText = '⏳ Загрузка...';
  
      if (window.foldersCache) renderMyList(window.foldersCache);
      
      try {
        const res = await fetch('/api/list-folders?vk_user_id=' + userId);
        const folders = await res.json();
        
        // Если бэкенд вернул ошибку в формате {error: "..."}
        if (folders.error) {
            listCont.innerText = '❌ Ошибка: ' + folders.error;
            return;
        }

        window.foldersCache = folders;
        renderMyList(folders);
      } catch (e) { 
        console.error(e);
        listCont.innerText = '❌ Ошибка загрузки'; 
      }
    }

    function renderMyList(data) {
      // Находим контейнер заново, так как мы вне области видимости openFolderSelector
      const listCont = document.getElementById('modalFolderList');
      if (!listCont) return;

      var html = '';
      // Проверка, что пришел массив
      if (!Array.isArray(data)) {
          listCont.innerText = 'Папок не найдено';
          return;
      }

      for (var i = 0; i < data.length; i++) {
          var f = data[i];
          var name = (typeof f === 'object') ? f.name : f;
          var id = (typeof f === 'object') ? f.id : f;

          html += '<div class="folder-item" ' +
                  'data-id="' + id + '" ' +
                  'data-name="' + name + '" ' +
                  'onclick="handleFolderClick(this)">' +
                  '📁 ' + name + '</div>';
      }
      listCont.innerHTML = html || 'Папки пусты';
    }

    function handleFolderClick(el) {
      var id = el.getAttribute('data-id');
      var name = el.getAttribute('data-name');
      selectFolder(id, name);
    }

    function closeFolders() { document.getElementById('folderModal').style.display = 'none'; }

    async function selectFolder(id, name) {
      const label = document.querySelector('#curFolderLabel b');
      if (label) label.innerText = "⏳ " + name;
      
      closeFolders();
  
      // Определяем, что именно сохранить в KV
      let folderValue;
      
      if (typeof currentProvider !== 'undefined' && currentProvider === 'google') {
          // Для Google Drive шлем технический ID
          folderValue = id;
      } else if (typeof currentProvider !== 'undefined' && currentProvider === 'dropbox') {
          // Для Dropbox шлем имя без лидирующего слэша
          folderValue = name.startsWith('/') ? name.substring(1) : name;
      } else {
          // Для Яндекса и прочих оставляем имя как есть
          folderValue = name;
      }
  
      try {
          await fetch('/api/select-folder', { 
              method: 'POST', 
              headers: { 'Content-Type': 'application/json' }, 
              body: JSON.stringify({ 
                  userId: userId, 
                  folderId: folderValue 
              }) 
          });
          uiReload();
      } catch (e) {
          console.error("Ошибка смены папки:", e);
          if (label) label.innerText = name;
      }
  }

    async function promptCreateFolder() {
      const folderName = prompt("Название папки:");
      if (!folderName || !folderName.trim()) return;
      
      try {
        // 1. Создаем папку и ЖДЕМ ответ с ID
        const res = await fetch('/api/create-folder', { 
          method: 'POST', 
          headers: { 'Content-Type': 'application/json' }, 
          body: JSON.stringify({ userId, name: folderName.trim() }) 
        });
        
        const data = await res.json();
        let newFolderId;

        // 3. УСЛОВИЯ ПО ПРОВАЙДЕРАМ
        if (currentProvider === 'google') {
          // Для гугла строго ID из ответа бэкенда
          newFolderId = data.folderId; 
        } else if (currentProvider === 'dropbox') {
          // Для дропбокса путь должен начинаться со слэша
          newFolderId = folderName.startsWith('/') ? folderName : '/' + folderName;
        } else {
          // Для Яндекс, WebDAV, Mail.ru используем просто имя
          newFolderId = folderName;
        }
    
        // 3. Сохраняем именно ID в KV
        await fetch('/api/select-folder', { 
          method: 'POST', 
          headers: { 'Content-Type': 'application/json' }, 
          body: JSON.stringify({ userId, folderId: newFolderId }) 
        });
        // Прячем модалку вручную перед релоадом, чтобы не висела
        const modal = document.getElementById('folderModal');
        if (modal) modal.style.display = 'none';
        // Обновляем статус (чтобы в шапке изменилось имя папки)
        if (typeof refreshData === 'function') {
          await refreshData();
        }
      } catch (e) { 
        console.error("Ошибка при создании:", e);
        uiReload();
      }
    }

    // Проверка реферала при загрузке страницы
    async function checkReferral() {
      try {
        const urlParameters = new URLSearchParams(window.location.search);
        const hashParameters = new URLSearchParams((window.location.hash || '').replace('#', '?'));
        
        // Собираем потенциальный ID из всех возможных источников
        let rawReferrer = urlParameters.get('ref') || hashParameters.get('ref') || urlParameters.get('vk_ref');
        
        if (!rawReferrer) return; // Если параметра нет вообще — выходим

        // Очищаем от префиксов
        let referrerId = String(rawReferrer).replace('ref_', '').trim();

        // ФИЛЬТРАЦИЯ: баннер не покажется, если:
        // - В ref передана системная строка (меню группы, реклама и т.д.)
        // - Это твой собственный ID
        // - ID отрицательный (метка сообщества)
        const systemTags = ['group_menu', 'none', 'ads', 'snippet_im', 'catalog', 'story', 'left_nav', 'right_nav'];
        const isSystemTag = systemTags.includes(referrerId);
        //const isYourself = referrerId === String(userId);
        const isGroup = referrerId.startsWith('-');
        
        // Самая важная проверка: если это не текстовый код (как из ТГ/чата), 
        // то это должен быть либо чистый числовой ID, либо мы это игнорируем
        // (Проверка на !isNaN помогает отсечь мусор вроде "group_menu")
        const isNumeric = !isNaN(referrerId);
        const isShortCode = referrerId.length >= 8; // Твои инвайты обычно длинные строки

        //if (isSystemTag || isYourself || isGroup || (!isNumeric && !isShortCode)) {
        if (isSystemTag || isGroup || (!isNumeric && !isShortCode)) {
          console.log("Вход без приглашения друга или системная метка:", referrerId);
          return;
        }

        // --- БЛОК ПРОВЕРКИ ИНВАЙТА ---
        let finalFriendId = referrerId;
        if (!isNumeric) {
          const inviteRes = await fetch('/api/get-invite-info?code=' + referrerId);
          if (inviteRes.ok) {
            const inviteInfo = await inviteRes.json();
            finalFriendId = String(inviteInfo.inviterId);
          }
        }
        //const isYourselfInvite = finalFriendId === String(userId);
        //if (isYourselfInvite) return; 
        // ----------------------------

        // Запрашиваем данные только если прошли фильтры
        const [myStatusResponse, friendStatusResponse] = await Promise.all([
          fetch('?action=get-status&userId=' + userId),
          fetch('?action=get-status&userId=' + finalFriendId)
        ]);
        
        const myData = await myStatusResponse.json();
        const friendData = await friendStatusResponse.json();
        
        // ГЛАВНОЕ ИЗМЕНЕНИЕ: Сравниваем через String, чтобы типы данных не мешали.
        // Если myData.friendId совпадает с тем, что в ссылке — баннер НЕ РИСУЕМ.
        if (myData.friendId && String(myData.friendId) === String(finalFriendId)) {
          console.log("Связь уже установлена в базе, баннер скрыт.");
          return;
        }

        // Показываем баннер, только если в базе записан ДРУГОЙ ID или пусто
        if (myData.friendId !== finalFriendId) {
          renderInviteBanner(referrerId, friendData.userName, friendData.userPhoto);
        }
      } catch (error) {
        console.error("Ошибка при обработке реферала:", error);
      }
    }

    // Отрисовка баннера приглашения
    function renderInviteBanner(friendId, friendName, friendPhoto) {
      if (sessionStorage.getItem('hideInviterBanner') === 'true') return;
    
      const zone = document.getElementById('inviterZone');
      if (!zone || document.getElementById('inviter-panel')) return;
      
      const panel = document.createElement('div');
      panel.id = 'inviter-panel';
      
      // Добавили transition для плавной анимации возврата/улета
      panel.style.cssText = "position:relative; margin:10px 0; padding:15px; background: var(--bubble-bg); border-radius:12px; border:2px dashed #4caf50; text-align:center; display:flex; flex-direction:column; align-items:center; gap:12px; transition: transform 0.2s ease-out, opacity 0.2s ease-out; touch-action: pan-y; overflow: hidden; color: var(--text-color);";
      const photoUrl = friendPhoto || 'https://vk.com/images/camera_50.png';
      const displayName = friendName || ('ID ' + friendId);
    
      panel.innerHTML = 
        '<div id="close-inviter-banner" style="position:absolute; top:8px; right:12px; cursor:pointer; color:#4caf50; font-size:22px; line-height:1; opacity:0.5; padding: 5px;">&times;</div>' +
        '<div style="display:flex; align-items:center; gap:12px; pointer-events: none;">' +
          '<img src="' + photoUrl + '" style="width:45px; height:45px; border-radius:50%; border:2px solid #4caf50; object-fit:cover;">' +
          '<div style="text-align:left;">' +
            '<span style="font-size:13px; color:#558b2f; display:block;">Вас пригласил друг:</span>' +
            '<b style="font-weight:bold; font-size:16px; color:#1b5e20;">' + displayName + '</b>' +
          '</div>' +
        '</div>' +
        '<button id="confirm-ref-button" style="background:#4caf50; color:white; border:none; padding:12px 20px; border-radius:25px; cursor:pointer; font-weight:bold; box-shadow:0 3px 8px rgba(76,175,80,0.3); width:100%; transition:0.2s;">' +
        '🤝 Подключить Хранилку друга</button>';
      
      zone.appendChild(panel);
    
      // Кнопки по клику (для ПК и обычного нажатия)
      document.getElementById('close-inviter-banner').onclick = () => {
        panel.style.opacity = '0';
        setTimeout(() => { panel.remove(); sessionStorage.setItem('hideInviterBanner', 'true'); }, 200);
      };
      
      document.getElementById('confirm-ref-button').onclick = () => confirmFriendConnection(friendId);
      const inviterPanel = document.getElementById('inviter-panel');
      makeSwipable(inviterPanel, () => {
        sessionStorage.setItem('hideInviterBanner', 'true');
      });
    }

    // Функция записи связи в базу
    async function confirmFriendConnection(friendId) {
      const btn = document.getElementById('confirm-ref-button');
      if (btn) {
        btn.disabled = true;
        btn.innerHTML = '⌛ Подключаем...';
        btn.style.opacity = '0.7';
      }
      try {
        const response = await fetch('/api/connect-friend?vk_user_id=' + userId + '&friend_id=' + friendId);
        const result = await response.json();
        
        if (result.success) {
          // МГНОВЕННО прячем баннер в сессии
          sessionStorage.setItem('hideInviterBanner', 'true');
          const panel = document.getElementById('inviter-panel');
          if (panel) {
            panel.innerHTML = '<b style="color:#1a5c1a;font-weight:bold;">✅ Хранилка друга успешно подключена!</b>';
          }
          // Перезагружаем через секунду, чтобы интерфейс обновился
          setTimeout(function() {
            location.reload();
          }, 1500);
        }
      } catch (error) {
        alert("Не удалось подключиться к другу");
      }
    }

    // Функция для ручного ввода ссылки (та самая кнопка внизу)
    async function openFriendsStorage() {
      const inputLink = prompt("Вставьте ссылку друга:");
      if (!inputLink) return;
    
      let extractedCode = "";
    
      // 1. Ищем текстовый код (ТГ или ВК-чат)
      const codeMatch = inputLink.match(/ref_([a-zA-Z0-9_-]+)/);
      
      if (codeMatch && codeMatch[1]) {
        extractedCode = codeMatch[1];
      } else {
        // 2. Ищем цифровой ID (ВК приложение #ref=123)
        // Специально ищем после #ref= или ?ref=
        const idMatch = inputLink.match(/(?:ref|start)[=_]([0-9]+)/) || inputLink.match(/#ref=([0-9]+)/);
        if (idMatch && idMatch[1]) {
          extractedCode = idMatch[1];
        }
      }
    
      if (extractedCode) {
        // Проверяем, чтобы это не был ID твоего бота (235249123)
        if (extractedCode === "235249123") {
          alert("Это ID бота, а не друга!");
          return;
        }
        confirmFriendConnection(extractedCode);
      } else {
        alert("Код приглашения не найден в ссылке");
      }
    }

    // Запуск при полной загрузке страницы
    window.addEventListener('DOMContentLoaded', function() {
      checkReferral();
    });
  </script>
</body>
</html>`;
}

async function handleVkUpload(request, env, ctx, userId, corsHeaders) {
  // 1. СРАЗУ отвечаем на предзапрос (если вдруг затесался)
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }
  try {
    const name = decodeURIComponent(request.headers.get('x-file-name') || 'file.bin');
    const uploadUrl = request.headers.get('x-upload-url') || null;
    const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
    if (!userData) throw new Error("Пользователь не авторизован");

    // Получаем поток напрямую — БЕЗ formData!
    const fileStream = request.body;
    if (!fileStream) throw new Error("No file stream");

    // Клонируем поток для облака и VK
    //const [streamForCloud, streamForVK] = fileStream.tee();

    // Определяем размер потока (придётся читать один раз)
    const fileSize = request.headers.get('content-length');
    if (!fileSize) throw new Error("Missing Content-Length header");

    // --- 1. ОПРЕДЕЛЕНИЕ ТИПА ---
    let mimeType = "application/octet-stream";
    let dbFileType = "document";
    const ext = (name.split('.').pop() || "").toLowerCase();

    if (["jpg", "jpeg", "png", "webp", "gif"].includes(ext)) {
      dbFileType = "photo";
      mimeType = ext === "png" ? "image/png" : "image/jpeg";
    } else if (["mp3", "wav", "ogg", "m4a"].includes(ext)) {
      dbFileType = "audio";
      mimeType = "audio/mpeg";
    } else if (["mp4", "mov", "avi", "mkv"].includes(ext)) {
      dbFileType = "video";
      mimeType = "video/mp4";
    }

    /*/ --- 2. ПРОКСИ НА ВК (в фоне) ---
    if (uploadUrl) {
      ctx.waitUntil((async () => {
        try {
          const vkBlob = await new Response(fileStream).blob();
          const vkFd = new FormData();
          vkFd.append('photo', vkBlob, name);
          await fetch(uploadUrl, { method: 'POST', body: vkFd });
        } catch (e) { console.error("VK Error:", e); }
      })());
    }*/

    /*/ --- 3. ЗАПИСЬ В БАЗУ D1 ---
    const fileId = String(Date.now());
    await env.FILES_DB.prepare(
      "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).bind(String(userId), name, fileId, dbFileType, userData.provider, userData.folderId || "Root", Date.now()).run();
    */

    // --- 4. ЗАГРУЗКА В ОБЛАКО ---
    let uploadOk = false;
    if (userData.provider === "google") {
      uploadOk = await uploadToGoogleStream(fileStream, name, userData.access_token, userData.folderId, mimeType, fileSize);
    } else if (userData.provider === "yandex") {
      uploadOk = await uploadToYandexStream(fileStream, name, userData.access_token, userData.folderId, mimeType, fileSize);
    } else if (userData.provider === "dropbox") {
      uploadOk = await uploadToDropboxStream(fileStream, name, userData.access_token, userData.folderId, fileSize);
    } else if (userData.provider === "webdav") {
      uploadOk = await uploadWebDAVStream(fileStream, name, userData, env, mimeType, fileSize, mimeType);
    }

    if (!uploadOk) {
      throw new Error("Cloud upload failed");
    }

    // --- 5. AI АНАЛИТИКА (В ФОНЕ) ---
    // ❌ УДАЛЕНО: const aiData = await file.arrayBuffer(); — это ломало всё
    // ✅ Временно отключено, чтобы не вызывать arrayBuffer()

    return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
  }
}

async function handleDirectPut(request, env, ctx, userId, fileName, corsHeaders) {
  try {
    // 1. Базовые проверки
    if (!userId || !fileName) {
      throw new Error("Параметры userId или fileName не получены");
    }

    // 2. Достаем настройки пользователя из KV
    const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
    if (!userData) {
      throw new Error("Настройки пользователя не найдены в базе (KV)");
    }

    // 3. Собираем URL для WebDAV (Mail.ru / Yandex / etc)
    const baseUrl = (userData.webdav_url || userData.webdav_host || "").replace(/\/$/, "");
    const folder = (userData.folderId || "").replace(/^\/|\/$/g, "");
    
    // Экранируем имя, чтобы не было ошибок с пробелами
    const safeFileName = encodeURIComponent(fileName);
    const fullUrl = folder ? `${baseUrl}/${folder}/${safeFileName}` : `${baseUrl}/${safeFileName}`;
    
    // 4. Авторизация
    const auth = btoa(`${userData.webdav_user}:${userData.webdav_pass}`);

    // 5. САМОЕ ВАЖНОЕ: Пробрасываем поток байтов из запроса прямо в облако
    const res = await fetch(fullUrl, {
      method: 'PUT',
      headers: { 
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/octet-stream'
      },
      body: request.body // Передаем входящий поток дальше (Streaming)
    });

    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`Облако ответило (${res.status}): ${errorText.substring(0, 100)}`);
    }

    // 6. Записываем в базу данных D1 (в фоновом режиме, чтобы не задерживать ответ)
    if (env.FILES_DB) {
      ctx.waitUntil(
        env.FILES_DB.prepare(
          "INSERT INTO files (userId, fileName, timestamp) VALUES (?, ?, ?)"
        ).bind(String(userId), fileName, Date.now()).run()
      );
    }

    return new Response(JSON.stringify({ success: true }), { 
      headers: { ...corsHeaders, "Content-Type": "application/json" } 
    });

  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { 
      status: 500, 
      headers: { ...corsHeaders, "Content-Type": "application/json" } 
    });
  }
}

async function handleVkUploadMultipart(request, env, ctx, corsHeaders) {
  try {
    const formData = await request.formData();
    const file = formData.get('file'); // Это объект File/Blob
    const fileName = formData.get('fileName');
    const userId = formData.get('userId');

    if (!file || !userId) throw new Error("Файл или ID пользователя не получены");

    // Получаем настройки пользователя
    const userData = await env.USER_DB.get(`user:${userId}`, { type: "json" });
    if (!userData) throw new Error("Пользователь не найден");

    const baseUrl = (userData.webdav_url || userData.webdav_host || "").replace(/\/$/, "");
    const folder = (userData.folderId || "").replace(/^\/|\/$/g, "");
    const safeFileName = encodeURIComponent(fileName);
    const url = folder ? `${baseUrl}/${folder}/${safeFileName}` : `${baseUrl}/${safeFileName}`;
    const auth = btoa(`${userData.webdav_user}:${userData.webdav_pass}`);

    // Проксируем файл в WebDAV
    const res = await fetch(url, {
      method: 'PUT',
      headers: { 
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/octet-stream'
      },
      body: file.stream() // Передаем поток, чтобы не забивать память воркера
    });

    if (!res.ok) throw new Error(`Ошибка WebDAV: ${res.status}`);

    // Запись в базу
    ctx.waitUntil(
      env.FILES_DB.prepare(
        "INSERT INTO files (userId, fileName, timestamp) VALUES (?, ?, ?)"
      ).bind(String(userId), fileName, Date.now()).run()
    );

    return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });

  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { 
      status: 500, 
      headers: corsHeaders 
    });
  }
}

async function handleVkUploadArrayBuffer(request, env, ctx) {
  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-vk-user-id, x-file-name, x-upload-url",
  };

  if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const fileName = decodeURIComponent(request.headers.get('x-file-name') || 'file.bin');
    const vkUserId = request.headers.get('x-vk-user-id');
    const uploadUrl = decodeURIComponent(request.headers.get('x-upload-url') || '');

    const fileBuffer = await request.arrayBuffer();
    if (fileBuffer.byteLength === 0) throw new Error("Файл пустой");

    const userData = await env.USER_DB.get("user:" + vkUserId, { type: "json" });
    if (!userData) return new Response("User Error", { status: 403, headers: corsHeaders });

    let dbFileType = "document";
    let mimeType = "application/octet-stream";
    let sType = "";
    const ext = (fileName.split('.').pop() || "").toLowerCase();

    if (["jpg", "jpeg", "png", "webp", "gif"].includes(ext)) {
      dbFileType = "photo";
      mimeType = ext === "png" ? "image/png" : "image/jpeg";
      sType = "IMAGE_TO_TEXT";
    } else if (["mp3", "wav", "ogg", "m4a"].includes(ext)) {
      dbFileType = "audio";
      mimeType = "audio/mpeg";
      sType = "AUDIO_TO_TEXT";
    } else if (["mp4", "mov", "avi", "mkv"].includes(ext)) {
      dbFileType = "video";
      mimeType = "video/mp4";
      sType = "VIDEO_TO_ANALYSIS";
    } else if (["pdf", "docx", "txt"].includes(ext)) {
      dbFileType = "document";
      if (["jpg", "jpeg", "png"].includes(ext)) sType = "IMAGE_TO_TEXT";
    }

    // Прокси на ВК (в фоне)
    if (uploadUrl) {
      ctx.waitUntil((async () => {
        try {
          const vkFd = new FormData();
          vkFd.append('photo', new Blob([fileBuffer], { type: mimeType }), fileName);
          await fetch(uploadUrl, { method: 'POST', body: vkFd });
        } catch (e) { console.error("VK Sync Error:", e); }
      })());
    }

    // Загрузка в облако
    let uploadOk = false;
    const { provider, access_token, folderId } = userData;
    const folder = folderId || "";

    if (provider === "yandex") {
      uploadOk = await uploadToYandexFromArrayBuffer(fileBuffer, fileName, access_token, folder);
    } else if (provider === "google") {
      uploadOk = await uploadToGoogleFromArrayBuffer(fileBuffer, fileName, access_token, folder || "root");
    } else if (provider === "dropbox") {
      uploadOk = await uploadToDropboxFromArrayBuffer(fileBuffer, fileName, access_token, folder);
    } else if (provider === "webdav") {
      uploadOk = await uploadWebDAVFromArrayBuffer(fileBuffer, fileName, userData, env);
    }

    if (!uploadOk) throw new Error("Cloud upload failed");

    // Запись в D1
    const fileId = "app_" + Date.now();
    await env.FILES_DB.prepare(
      "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).bind(String(vkUserId), fileName, fileId, dbFileType, provider, folder, Date.now()).run();

    // AI аналитика (в фоне)
    if (sType && ctx?.waitUntil) {
      ctx.waitUntil((async () => {
        try {
          const cfg = await loadActiveConfig(sType, env);
          if (cfg?.FUNCTION) {
            const description = await cfg.FUNCTION(cfg, fileBuffer, env, mimeType);
            if (description) {
              await env.FILES_DB.prepare("UPDATE files SET ai_description = ? WHERE userId = ? AND fileName = ?")
                .bind(description, String(vkUserId), fileName).run();
            }
          }
        } catch (e) { console.error("AI BG Error:", e); }
      })());
    }

    return new Response(JSON.stringify({ success: true, fileId }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
  }
}

// --- ПОЛУЧЕНИЕ ССЫЛКИ ---
async function handleGetUploadLink(request, env) {
  const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, x-vk-user-id, x-file-name",
  };

  try {
      const vkUserId = request.headers.get('x-vk-user-id');
      const userData = await env.USER_DB.get("user:" + vkUserId, { type: "json" });
      const fileName = decodeURIComponent(request.headers.get('x-file-name'));
      const fileSize = request.headers.get('x-file-size'); // Вот он!
      //const mimeType = request.headers.get('x-file-type') || 'application/octet-stream';
      if (!userData) return new Response("User Error", { status: 403, headers: corsHeaders });

      const { provider, access_token, folderId } = userData;
      const folder = folderId || "";

      // --- YANDEX ---
      if (provider === "yandex") {
          const path = (folder ? `/${folder}/${fileName}` : `/${fileName}`).replace(/\/+/g, '/');
          const res = await fetch(`https://cloud-api.yandex.net/v1/disk/resources/upload?path=${encodeURIComponent(path)}&overwrite=true`, {
              headers: { "Authorization": `OAuth ${access_token}` }
          });
          const data = await res.json();
          return new Response(JSON.stringify({ upload_url: data.href, method: "PUT", provider: "yandex" }), { headers: corsHeaders });
      } 
      
      // --- GOOGLE DRIVE ---
      if (provider === "google") {
        const res = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${access_token}`,
                'X-Upload-Content-Type': 'application/octet-stream',
                'X-Upload-Content-Length': fileSize,
                'Content-Type': 'application/json; charset=UTF-8',
                // ДОБАВЛЯЕМ ORIGIN, чтобы Google знал, кто будет загружать
                'Origin': 'https://leshiy-storage-bot.leshiyalex.workers.dev' 
            },
            body: JSON.stringify({ 
                name: fileName, 
                parents: folder ? [folder] : [] 
            })
        });

        const uploadUrl = res.headers.get('Location');

        // КРИТИЧЕСКИЕ ЗАГОЛОВКИ ДЛЯ БРАУЗЕРА
        const responseHeaders = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*", // Разрешаем фронту читать ответ
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT",
            "Access-Control-Expose-Headers": "Location" // Чтобы фронт увидел ссылку
        };

        if (!uploadUrl) {
            return new Response(JSON.stringify({ error: "Google session failed" }), { status: 500, headers: responseHeaders });
        }

        return new Response(JSON.stringify({ 
            upload_url: uploadUrl, 
            method: "PUT", 
            provider: "google" 
        }), { status: 200, headers: responseHeaders });
      }

      // --- DROPBOX ---
      if (provider === "dropbox") {
          const dbxUrl = 'https://content.dropboxapi.com/2/files/upload';
          const fullPath = ('/' + folder + '/' + fileName).replace(/\/+/g, '/');

          // Создаём объект и сериализуем его в JSON с экранированием Unicode
          const argObj = { path: fullPath, mode: "overwrite" };
          let args = JSON.stringify(argObj);

          // Экранируем все не-ASCII символы как \uXXXX
          args = args.replace(/[\u0080-\uFFFF]/g, c => 
            '\\u' + ('0000' + c.charCodeAt(0).toString(16)).slice(-4)
          );

          // Для Dropbox фронту нужно будет добавить эти заголовки в XHR
          return new Response(JSON.stringify({ 
              upload_url: dbxUrl, 
              method: "POST", 
              headers: { 
                  "Authorization": `Bearer ${access_token}`, 
                  "Dropbox-API-Arg": args, 
              } 
          }), { headers: corsHeaders });
      }

      // --- WEBDAV ---
      if (provider === "webdav") {
        // 1. Берем хост. У Mail.ru это webdav.cloud.mail.ru
        let host = userData.webdav_host || "https://webdav.cloud.mail.ru";
        if (!host.startsWith('https')) host = 'https://' + host;
        const baseUrl = host.endsWith('/') ? host : host + '/';
        const uploadUrl = baseUrl + (folder ? folder + '/' : '') + fileName;
        // 4. Авторизация. Используем webdav_user и webdav_pass из твоего KV
        const auth = btoa(`${userData.webdav_user}:${userData.webdav_pass}`);
        return new Response(JSON.stringify({ 
            upload_url: uploadUrl, 
            method: "PUT", 
            headers: { 
                "Authorization": `Basic ${auth}`,
                "X-Upload-Content-Length": `${fileSize}`,
            } 
        }), { headers: corsHeaders });
      }

      throw new Error("Неизвестный провайдер");
  } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
  }
}

// --- ПОДТВЕРЖДЕНИЕ И ЗАПИСЬ В БАЗУ ---
async function handleConfirmUpload(request, env) {
  const corsHeaders = { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" };
  try {
      // 1. Берем данные из JSON
      const { fileName } = await request.json();
      // 2. А userId берем из заголовков (там он точно есть)
      const userId = request.headers.get('x-vk-user-id');

      if (!userId) throw new Error("userId is missing in headers");

      const userData = await env.USER_DB.get("user:" + userId, { type: "json" });
      if (!userData) throw new Error("User data not found in KV");

      await env.FILES_DB.prepare(
          "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).bind(String(userId), fileName, "app_" + Date.now(), "document", userData.provider, userData.folderId || "Root", Date.now()).run();

      return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
  } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
  }
}

async function handleDownload(path, fileName, userId, env) {
  try {
      // 1. ПОИСК ПОЛЬЗОВАТЕЛЯ (учитываем формат user:ID из твоего KV)
      let userDataRaw = await env.USER_DB.get("user:" + userId) || 
                        await env.USER_DB.get(String(userId));

      if (!userDataRaw) throw new Error("Данные пользователя не найдены для ID: " + userId);
      const userData = JSON.parse(userDataRaw);

      // 2. ИСПРАВЛЕНИЕ ПУТИ
      // Если в базе (D1) лежит только папка 'STORAGE', склеиваем её с именем файла
      let fullPath = path;
      if (!fullPath.includes('.') && !fullPath.toLowerCase().endsWith('.zip')) {
          const folder = fullPath.replace(/\/$/, '');
          fullPath = folder + '/' + fileName;
      }

      let cloudResponse;
      const provider = userData.provider;

      // 3. ОБРАБОТКА 5 ПРОВАЙДЕРОВ
      if (provider === 'google') {
        // В path у нас ID папки (например 1CbEI7PT...)
        const folderId = path; 
        // Сначала ищем ID файла по имени внутри этой папки
        const searchUrl = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+name='${encodeURIComponent(fileName)}'+and+explicitlyTrashed=false&fields=files(id)`;
        const searchRes = await fetch(searchUrl, {
            headers: { 'Authorization': `Bearer ${userData.access_token}` }
        });
        const searchData = await searchRes.json();
        if (!searchRes.ok || !searchData.files || searchData.files.length === 0) {
            throw new Error(`Google Drive: Файл "${fileName}" не найден в папке ${folderId}`);
        }
        const realFileId = searchData.files[0].id;
        // Теперь качаем по настоящему ID файла
        cloudResponse = await fetch(`https://www.googleapis.com/drive/v3/files/${realFileId}?alt=media`, {
            headers: { 'Authorization': `Bearer ${userData.access_token}` }
        });
      } else if (provider === 'yandex' && userData.access_token) {
          // YANDEX (OAuth API) - Нужен префикс disk:/
          const yaPath = fullPath.startsWith('disk:/') ? fullPath : 'disk:/' + fullPath;
          const apiRes = await fetch('https://cloud-api.yandex.net/v1/disk/resources/download?path=' + encodeURIComponent(yaPath), {
              headers: { 'Authorization': 'OAuth ' + userData.access_token }
          });
          
          if (!apiRes.ok) throw new Error("Yandex API Error: " + await apiRes.text());
          const { href } = await apiRes.json();
          cloudResponse = await fetch(href);

      } else if (provider === 'dropbox') {
          // DROPBOX (OAuth)
          cloudResponse = await fetch('https://content.dropboxapi.com/2/files/download', {
              method: 'POST',
              headers: {
                  'Authorization': `Bearer ${userData.access_token}`,
                  'Dropbox-API-Arg': JSON.stringify({ path: fullPath.startsWith('/') ? fullPath : '/' + fullPath })
              }
          });

      } else if (userData.webdav_host) {
          // MAIL.RU и WEBDAV (через Basic Auth)
          const auth = 'Basic ' + btoa(userData.webdav_user + ':' + userData.webdav_pass);
          // Проверяем слэш между хостом и путем
          const url = userData.webdav_host.replace(/\/$/, '') + '/' + fullPath.replace(/^\//, '');
          cloudResponse = await fetch(url, {
              method: 'GET',
              headers: { 'Authorization': auth }
          });

      } else {
          throw new Error("Неизвестный провайдер или отсутствуют настройки");
      }

      // 4. ПРОВЕРКА ОТВЕТА ОБЛАКА
      if (!cloudResponse || !cloudResponse.ok) {
          throw new Error(`Облако ${provider} ответило ошибкой: ${cloudResponse?.status}`);
      }

      // 5. ФОРМИРОВАНИЕ ОТВЕТА (СТРИМИНГ)
      // Используем TransformStream, чтобы избежать обрывов на больших файлах
      const { readable, writable } = new TransformStream();
      cloudResponse.body.pipeTo(writable);

      const headers = new Headers();
      headers.set('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(fileName)}`);
      headers.set('Content-Type', cloudResponse.headers.get('content-type') || 'application/octet-stream');
      headers.set('Access-Control-Allow-Origin', '*');
      
      // Обязательно пробрасываем размер, чтобы браузер не писал "ошибка сети" в конце
      const size = cloudResponse.headers.get('content-length');
      if (size) headers.set('Content-Length', size);

      return new Response(readable, {
          status: 200,
          headers: headers
      });

  } catch (e) {
      console.error("Download fail:", e.message);
      return new Response("Ошибка: " + e.message, { 
          status: 500,
          headers: { 'Access-Control-Allow-Origin': '*' }
      });
  }
}

// Универсальная функция для генерации HTML-перехода (чтобы не дублировать код)
const renderRedirectPage = (targetUrl, providerName) => {
  return new Response(`
  <!DOCTYPE html>
  <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Авторизация...</title>
      <style>
        body { background: #ebedf0; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; font-family: -apple-system, system-ui, sans-serif; }
        .loader { border: 3px solid #f3f3f3; border-top: 3px solid #2688eb; border-radius: 50%; width: 30px; height: 30px; animation: spin 1s linear infinite; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
      </style>
    </head>
    <body>
      <div style="text-align: center;">
        <div class="loader" style="margin: 0 auto 15px;"></div>
        <div style="color: #555; font-size: 14px;">Переход к авторизации...</div>
      </div>
      <script>
        // Выполняем переход через JS, что "нравится" мобильным браузерам
        setTimeout(() => { window.location.href = "${targetUrl}"; }, 100);
      </script>
    </body>
  </html>`, { headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
};

function renderSuccessPage() {
  return new Response(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
          body { font-family: -apple-system, system-ui, sans-serif; text-align:center; padding-top:50px; background: #ebedf0; margin: 0; }
          .card { background: white; margin: 20px; padding: 30px; border-radius: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); max-width: 400px; margin: 0 auto; }
          h2 { color: #4bb34b; margin-bottom: 10px; }
          p { color: #626d7a; font-size: 15px; line-height: 1.4; padding: 0 10px; }
          .btn { margin-top: 20px; padding: 14px 24px; border-radius: 12px; border: none; background: #0077ff; color: white; font-weight: bold; cursor: pointer; width: 100%; font-size: 16px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h2>✅ Успешно подключено!</h2>
          <p>Теперь вы можете закрыть это окно и вернуться назад.<br>Окно закроется автоматически через 3 секунды.</p>
          <button class="btn" onclick="handleClose()">Закрыть окно</button>
        </div>
        <script>
          // Инициализация Bridge сразу при загрузке
          if (window.vkBridge) {
            vkBridge.send("VKWebAppInit");
          }
          function handleClose() {
            // 1. Попытка для Telegram
            if (window.Telegram && window.Telegram.WebApp) {
              window.Telegram.WebApp.close();
            }
            // 2. Попытка для ВК (Bridge)
            if (typeof vkBridge !== 'undefined') {
              vkBridge.send("VKWebAppClose", {"status": "success"});
            }
            // 3. Стандартный метод для ПК
            window.close();
            // 4. Если всё выше не сработало (обычный браузер или чат)
            // Информируем пользователя, что пора выходить
            const btn = document.querySelector('.btn');
            if (btn) btn.innerText = "Закройте вкладку вручную";
          }
          // Авто-закрытие
          setTimeout(handleClose, 3000);
        </script>
      </body>
    </html>`, { 
    headers: { "Content-Type": "text/html; charset=utf-8" } 
  });
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
  list += `\n<b>☁️ Провайдер: ${userData?.provider}</b> 📁 Папка: ${userData?.folderId}`;
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

  if (data.startsWith("del_inv:")) {
    const code = data.split(":")[1];
    await env.USER_DB.delete(`invite:${code}`);
    
    // Уведомляем Telegram, что кнопка нажата, и правим сообщение
    await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: data.id, text: `Инвайт ${code} удален` })
    });
    
    await sendMessage(chatId, `✅ Инвайт <code>${code}</code> успешно удален.`, null, env);
    return new Response("OK");
  }

  if (data === "del_inv_all") {
      const list = await env.USER_DB.list({ prefix: "invite:" });
      for (const key of list.keys) {
          await env.USER_DB.delete(key.name);
      }
      await sendMessage(chatId, "🗑️ Все инвайты были очищены.", null, env);
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
        success = await createWebDavFolder(folderIdOrName, userData);
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

// Функция обработки каждого файла в карусели сообщения ВК

async function processOneAttachment(attach, userData, userId, chatId, env) {
  try {
    let url = "", name = "", fType = attach.type;
    let dbFileType = fType;
    let mimeType = ""; 

    const now = new Date();
    const dateStr = now.getFullYear() + '-' + 
                    String(now.getMonth() + 1).padStart(2, '0') + '-' + 
                    String(now.getDate()).padStart(2, '0') + '_' + 
                    String(now.getHours()).padStart(2, '0') + '-' + 
                    String(now.getMinutes()).padStart(2, '0') + '-' + 
                    String(now.getSeconds()).padStart(2, '0');

    // --- 1. ЛОГИКА ОПРЕДЕЛЕНИЯ ТИПА (ПОЛНАЯ) ---
    if (fType === "photo") {
      url = attach.photo.sizes.sort((a,b) => b.width - a.width)[0].url;
      name = `Photo_${dateStr}.jpg`;
      mimeType = "image/jpeg";
    } 
    else if (fType === "doc") {
      url = attach.doc.url;
      name = attach.doc.title || `Doc_${dateStr}.${attach.doc.ext}`;
      dbFileType = "document";
      const ext = (attach.doc.ext || "").toLowerCase();
      
      // ИСПРАВЛЕННЫЙ MIME ДЛЯ PNG
      if (ext === "pdf") mimeType = "application/pdf";
      else if (ext === "png") mimeType = "image/png"; // Вот это добавили
      else if (["jpg","jpeg"].includes(ext)) mimeType = "image/jpeg";
      else mimeType = "text/plain";
    }
    else if (fType === "audio") {
      url = attach.audio.url;
      const artist = (attach.audio.artist || "Unknown").replace(/[\\/:*?"<>|]/g, "");
      const title = (attach.audio.title || "Track").replace(/[\\/:*?"<>|]/g, "");
      name = `${artist} - ${title}.mp3`;
      dbFileType = "audio";
      mimeType = "audio/mpeg";
    } 
    else if (fType === "video") {
      const v = attach.video;
      let vFiles = v.files || {};
      url = vFiles.mp4_1080 || vFiles.mp4_720 || vFiles.mp4_480 || vFiles.mp4_360 || vFiles.src;

      if (!url && v.player) {
        try {
          const playerRes = await fetch(v.player, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
          });
          if (playerRes.ok) {
            const html = await playerRes.text();
            const videoMatch = html.match(/https?:\/\/[^\s"'<>]+?\.mp4[^\s"'<>]*\?[\w=&%-]+/g);
            if (videoMatch) {
              url = videoMatch[0].replace(/\\/g, ''); 
            } else {
              const flashVarsMatch = html.match(/\"url(\d+)\"\:\"(https?.*?)\"/g);
              if (flashVarsMatch) {
                const lastMatch = flashVarsMatch[flashVarsMatch.length - 1];
                url = lastMatch.split('":"')[1].replace('"', '').replace(/\\/g, '');
              }
            }
          }
        } catch (e) { console.error("Player parsing failed", e); }
      }
      if (!url) return false;
      name = (v.title || `Video_${dateStr}`).replace(/[\\/:*?"<>|]/g, "") + ".mp4";
      dbFileType = "video";
      mimeType = "video/mp4";
    }
    else if (fType === "video_message") {
      url = attach.video_message.video_url;
      name = `VideoNote_${dateStr}.mp4`;
      dbFileType = "video";
      mimeType = "video/mp4";
    }
    else if (fType === "audio_message") {
      url = attach.audio_message.link_mp3 || attach.audio_message.link_ogg;
      name = `Voice_${dateStr}.mp3`;
      dbFileType = "audio_message";
      mimeType = "audio/mpeg";
    }

    if (!url) return false;

    // --- 2. СКАЧИВАНИЕ ---
    let fileRes = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*'
      }
    });
    if (!fileRes.ok) return false;

    // --- 3. ФИНАЛЬНЫЙ ПЕРЕХВАТЧИК REDIRECT/HTML ---
    let fileBuffer = await fileRes.arrayBuffer();
    const contentType = fileRes.headers.get("Content-Type") || "";

    if (contentType.includes("text/html")) {
      const textContent = new TextDecoder().decode(fileBuffer);
      const match = textContent.match(/https?:\/\/[^\s"'<>]+(?:psv4|userapi|vk-cdn|vk\.me)[^\s"'<>]+\b/);
      if (match) {
        let directUrl = match[0].replace(/&amp;/g, '&');
        const secondRes = await fetch(directUrl, {
          headers: { 
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://vk.com/'
          }
        });
        if (secondRes.ok) {
          const secondType = secondRes.headers.get("Content-Type") || "";
          if (!secondType.includes("text/html")) {
            fileBuffer = await secondRes.arrayBuffer();
          }
        }
      }
    }

    if (fileBuffer.byteLength < 100) return false;

    // --- 4. ЗАПИСЬ В БАЗУ ---
    const vkId = String(attach[fType]?.id || Date.now());
    await env.FILES_DB.prepare(
      "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).bind(String(userId), name, vkId, dbFileType, userData.provider, userData.folderId || "Root", Date.now()).run();

    // --- 5. ЗАГРУЗКА В ОБЛАКО ---
    let uploadOk = false;
    if (userData.provider === "google") {
      uploadOk = await uploadToGoogleFromArrayBuffer(fileBuffer, name, userData.access_token, userData.folderId || "root");
    } else if (userData.provider === "yandex") {
      uploadOk = await uploadToYandexFromArrayBuffer(fileBuffer, name, userData.access_token, userData.folderId || "");
    } else if (userData.provider === "webdav") {
      uploadOk = await uploadWebDAVFromArrayBuffer(fileBuffer, name, userData, env);
    }

    if (uploadOk) {
      await sendVKMessage(chatId, `✅ Сохранен: ${name}`, env);

      // --- 6. AI АНАЛИТИКА (ПОЛНАЯ) ---
      try {
        let sType = "";
        if (dbFileType === "photo") sType = "IMAGE_TO_TEXT";
        else if (dbFileType === "document") {
          const ext = name.split('.').pop().toLowerCase();
          sType = ["jpg","jpeg","png","webp"].includes(ext) ? "IMAGE_TO_TEXT" : "DOCUMENT_TO_TEXT";
        }
        else if (dbFileType === "audio" || dbFileType === "audio_message") sType = "AUDIO_TO_TEXT";
        else if (dbFileType === "video") sType = "VIDEO_TO_ANALYSIS";

        if (sType) {
          const cfg = await loadActiveConfig(sType, env);
          if (cfg && cfg.FUNCTION) {
            let description = "";
            if (sType === "DOCUMENT_TO_TEXT" || sType === "VIDEO_TO_ANALYSIS") {
              description = await cfg.FUNCTION(cfg, fileBuffer, env, mimeType);
            } else {
              description = await cfg.FUNCTION(cfg, fileBuffer, env);
            }
            if (description) {
              await env.FILES_DB.prepare("UPDATE files SET ai_description = ? WHERE userId = ? AND fileName = ?")
                .bind(description, String(userId), name).run();
            }
          }
        }
      } catch (aiErr) { console.error("AI Error:", aiErr); }
    }
    
    fileBuffer = null;
    return uploadOk;
  } catch (err) {
    console.error("Critical OneAttach Error:", err);
    return false;
  }
}

async function processOneAttachmentStream(attach, userData, userId, chatId, env, ctx) {
  try {
      let url = "", name = "", fType = attach.type;
      let dbFileType = fType;
      let mimeType = ""; 

      const now = new Date();
      const dateStr = now.getFullYear() + '-' + 
                      String(now.getMonth() + 1).padStart(2, '0') + '-' + 
                      String(now.getDate()).padStart(2, '0') + '_' + 
                      String(now.getHours()).padStart(2, '0') + '-' + 
                      String(now.getMinutes()).padStart(2, '0') + '-' + 
                      String(now.getSeconds()).padStart(2, '0');

      // --- 1. ПОЛНАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ТИПА И ПАРСИНГА ВИДЕО ---
      if (fType === "photo") {
          url = attach.photo.sizes.sort((a,b) => b.width - a.width)[0].url;
          name = `Photo_${dateStr}.jpg`;
          mimeType = "image/jpeg";
      } 
      else if (fType === "doc") {
          url = attach.doc.url;
          name = attach.doc.title || `Doc_${dateStr}.${attach.doc.ext}`;
          dbFileType = "document";
          const ext = (attach.doc.ext || "").toLowerCase();
          if (ext === "pdf") mimeType = "application/pdf";
          else if (ext === "png") mimeType = "image/png";
          else if (["jpg","jpeg"].includes(ext)) mimeType = "image/jpeg";
          else mimeType = "application/octet-stream";
      }
      else if (fType === "audio") {
          url = attach.audio.url;
          const artist = (attach.audio.artist || "Unknown").replace(/[\\/:*?"<>|]/g, "");
          const title = (attach.audio.title || "Track").replace(/[\\/:*?"<>|]/g, "");
          name = `${artist} - ${title}.mp3`;
          dbFileType = "audio";
          mimeType = "audio/mpeg";
      } 
      else if (fType === "video") {
          const v = attach.video;
          let vFiles = v.files || {};
          // Пытаемся взять прямую ссылку
          url = vFiles.mp4_1080 || vFiles.mp4_720 || vFiles.mp4_480 || vFiles.mp4_360 || vFiles.src;

          // Если прямой ссылки нет, парсим плеер
          if (!url && v.player) {
              try {
                  const playerRes = await fetch(v.player, {
                      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
                  });
                  if (playerRes.ok) {
                      const html = await playerRes.text();
                      const videoMatch = html.match(/https?:\/\/[^\s"'<>]+?\.mp4[^\s"'<>]*\?[\w=&%-]+/g);
                      if (videoMatch) {
                          url = videoMatch[0].replace(/\\/g, ''); 
                      } else {
                          const flashVarsMatch = html.match(/\"url(\d+)\"\:\"(https?.*?)\"/g);
                          if (flashVarsMatch) {
                              const lastMatch = flashVarsMatch[flashVarsMatch.length - 1];
                              url = lastMatch.split('":"')[1].replace('"', '').replace(/\\/g, '');
                          }
                      }
                  }
              } catch (e) { console.error("Player parsing failed", e); }
          }
          if (!url) return false;
          name = (v.title || `Video_${dateStr}`).replace(/[\\/:*?"<>|]/g, "") + ".mp4";
          dbFileType = "video";
          mimeType = "video/mp4";
      }
      else if (fType === "video_message") {
          url = attach.video_message.video_url;
          name = `VideoNote_${dateStr}.mp4`;
          dbFileType = "video";
          mimeType = "video/mp4";
      }
      else if (fType === "audio_message") {
          url = attach.audio_message.link_mp3 || attach.audio_message.link_ogg;
          name = `Voice_${dateStr}.mp3`;
          dbFileType = "audio_message";
          mimeType = "audio/mpeg";
      }

      if (!url) return false;

      // --- 2. СКАЧИВАНИЕ (ПЕРВИЧНЫЙ ЗАПРОС) ---
      let fileRes = await fetch(url, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': '*/*' }
      });
      if (!fileRes.ok) return false;

      // --- 3. ПЕРЕХВАТЧИК REDIRECT/HTML (ЧТОБЫ НЕ БЫЛО 55КБ HTML) ---
      let finalStream = fileRes.body;
      let finalSize = fileRes.headers.get("Content-Length");
      const contentType = fileRes.headers.get("Content-Type") || "";

      if (contentType.includes("text/html")) {
          const textContent = await fileRes.text();
          const match = textContent.match(/https?:\/\/[^\s"'<>]+(?:psv4|userapi|vk-cdn|vk\.me)[^\s"'<>]+\b/);
          if (match) {
              let directUrl = match[0].replace(/&amp;/g, '&');
              const secondRes = await fetch(directUrl, {
                  headers: { 'User-Agent': 'Mozilla/5.0...', 'Referer': 'https://vk.com/' }
              });
              if (secondRes.ok && !secondRes.headers.get("Content-Type").includes("text/html")) {
                  finalStream = secondRes.body;
                  finalSize = secondRes.headers.get("Content-Length");
              }
          }
      }

      // --- 4. ЗАПИСЬ В БАЗУ ---
      const vkId = String(attach[fType]?.id || Date.now());
      await env.FILES_DB.prepare(
          "INSERT INTO files (userId, fileName, fileId, fileType, provider, remotePath, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).bind(String(userId), name, vkId, dbFileType, userData.provider, userData.folderId || "Root", Date.now()).run();

      // --- 5. ЗАГРУЗКА В ОБЛАКО (СТРИМИНГ) ---
      let uploadOk = false;
      if (userData.provider === "google") {
          uploadOk = await uploadToGoogleStream(finalStream, name, userData.access_token, userData.folderId, mimeType, finalSize);
      } else if (userData.provider === "yandex") {
          uploadOk = await uploadToYandexStream(finalStream, name, userData.access_token, userData.folderId, mimeType, finalSize);
      } else if (userData.provider === "dropbox") {
          uploadOk = await uploadToDropboxStream(finalStream, name, userData.access_token, userData.folderId, finalSize);
      } else if (userData.provider === "webdav") {
          uploadOk = await uploadWebDAVStream(finalStream, name, userData, env, mimeType, finalSize);
      }

      if (uploadOk) {
          await sendVKMessage(chatId, `✅ Сохранен: ${name}`, env);
          
          // --- 6. AI АНАЛИТИКА (В ФОНЕ) ---
          if (ctx && ctx.waitUntil) {
              ctx.waitUntil((async () => {
                  try {
                      // Для аналитики скачиваем заново в Buffer (т.к. стрим уже закрыт)
                      const aiRes = await fetch(url);
                      const aiBuffer = await aiRes.arrayBuffer();
                      
                      let sType = "";
                      if (dbFileType === "photo") sType = "IMAGE_TO_TEXT";
                      else if (dbFileType === "document") {
                          const ext = name.split('.').pop().toLowerCase();
                          sType = ["jpg","jpeg","png","webp"].includes(ext) ? "IMAGE_TO_TEXT" : "DOCUMENT_TO_TEXT";
                      }
                      else if (dbFileType === "audio" || dbFileType === "audio_message") sType = "AUDIO_TO_TEXT";
                      else if (dbFileType === "video") sType = "VIDEO_TO_ANALYSIS";

                      if (sType) {
                          const cfg = await loadActiveConfig(sType, env);
                          if (cfg && cfg.FUNCTION) {
                              const description = await cfg.FUNCTION(cfg, aiBuffer, env, mimeType);
                              if (description) {
                                  await env.FILES_DB.prepare("UPDATE files SET ai_description = ? WHERE userId = ? AND fileName = ?")
                                      .bind(description, String(userId), name).run();
                              }
                          }
                      }
                  } catch (e) { console.error("AI BG Error:", e); }
              })());
          }
      }
      
      return uploadOk;
  } catch (err) {
      console.error("Critical OneAttach Error:", err);
      return false;
  }
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
    return renderSuccessPage();
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


async function uploadToYandexStream(stream, name, token, folder, type, fileSize) {
  const path = folder ? `/${folder}/${name}` : `/${name}`;
  const getUrl = `https://cloud-api.yandex.net/v1/disk/resources/upload?path=${encodeURIComponent(path.replace(/\/+/g, '/'))}&overwrite=true`;
  
  const r = await fetch(getUrl, { headers: { "Authorization": `OAuth ${token}` } });
  const d = await r.json();
  if (!d.href) return false;

  const res = await fetch(d.href, { 
      method: "PUT", 
      body: stream, 
      headers: { "Content-Type": type },
      
      // @ts-ignore
      duplex: 'half' 
  });
  return res.ok;
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
    return renderSuccessPage();
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


async function uploadToGoogleStream(stream, name, token, folder, type, fileSize) {
  // Для Google используем Simple Upload (поддерживает стрим до 5МБ на бесплатном лимите легко)
  // Если нужно больше 5МБ, нужен Resumable, но для начала стабилизируем это
  
  const folderId = (folder === "root" || !folder) ? "" : folder;
  const url = `https://www.googleapis.com/upload/drive/v3/files?uploadType=media&ignoreDefaultVisibility=true`;
  
  // В простом режиме метаданные (имя) передать сложнее одним запросом через стрим, 
  // поэтому сначала создаем файл, потом переименуем, ЛИБО используем multipart (но он сложен для стрима).
  // Оставим пока медиа-аплоад для стабильности.
  const res = await fetch(url, {
      method: 'POST',
      headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': type
      },
      body: stream,
      
      // @ts-ignore
      duplex: 'half'
  });
  return res.ok;
}

async function listGoogleFolders(token) {
  try {
    // Ищем только папки, не в корзине. Параметр spaces=drive обязателен для некоторых типов аккаунтов.
    const q = encodeURIComponent("mimeType='application/vnd.google-apps.folder' and trashed=false");
    const url = `https://www.googleapis.com/drive/v3/files?q=${q}&fields=files(id,name)&pageSize=30&spaces=drive`;

    const res = await fetch(url, {
      headers: { "Authorization": `Bearer ${token}` }
    });

    if (!res.ok) {
      const errBody = await res.text();
      console.error("DRIVE_ERROR:", errBody);
      return [];
    }

    const data = await res.json();
    // Если папок реально нет, вернет [], если есть — массив объектов
    return (data.files || []).map(f => ({ id: f.id, name: f.name }));
  } catch (e) {
    return [];
  }
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
  let baseUrl = (userData.webdav_host || "https://webdav.cloud.mail.ru").trim();
  if (!baseUrl.startsWith('http')) baseUrl = 'https://' + baseUrl;
  baseUrl = baseUrl.replace(/\/+$/, '');

  const folder = userData.folderId || "";
  const path = folder ? `${folder}/${fileName}` : fileName;
  const url = `${baseUrl}/${path}`;

  const auth = btoa(`${userData.webdav_user}:${userData.webdav_pass}`);

  const res = await fetch(url, {
    method: 'PUT',
    headers: {
      'Authorization': `Basic ${auth}`,
      'Content-Type': 'application/octet-stream',
      'Content-Length': String(arrayBuffer.byteLength)
    },
    body: arrayBuffer
  });

  return res.ok;
}


async function uploadWebDAVStream(stream, name, userData, env, type, fileSize, mimeType) {
    // Подходит и для Mail.ru, и для своего WebDAV
    const baseUrl = userData.webdav_url || userData.webdav_host || "";
    // Берём папку
    const folder = userData.folderId || "";
    // Формируем путь: если есть папка — добавляем её
    const path = folder ? `${folder}/${name}` : name;
    const url = `${baseUrl}/${path}`;
    const auth = btoa(`${userData.webdav_user}:${userData.webdav_pass}`);
    
    const res = await fetch(url, {
        method: 'PUT',
        headers: { 
            'Authorization': `Basic ${auth}`, 
            'Content-Length': fileSize
        },
        body: stream,
        
        // @ts-ignore
        duplex: 'half'
    });
    return res.ok;
}

async function listWebDavFolders(user) {
  const host = user.webdav_host.startsWith('http') ? user.webdav_host : `https://${user.webdav_host}`;
  
  // Запрос PROPFIND для получения списка файлов и папок
  // Depth: 1 означает "только в текущей папке"
  const response = await fetch(host, {
    method: 'PROPFIND',
    headers: {
      'Authorization': 'Basic ' + btoa(`${user.webdav_user}:${user.webdav_pass}`),
      'Depth': '1',
      'Content-Type': 'application/xml; charset=utf-8'
    }
  });

  if (!response.ok) {
    throw new Error('WebDAV server error: ' + response.status);
  }

  const xml = await response.text();
  
  // Парсим XML вручную (так как воркеры не имеют полноценного DOMParser)
  // Ищем теги <d:response>, где внутри есть <d:collection />
  const folders = [];
  const responses = xml.split('</d:response>');

  responses.forEach(res => {
    // Проверяем, что это папка (есть тег collection)
    if (res.includes('<d:collection') || res.includes('<collection')) {
      // Вытаскиваем имя папки из <d:href>
      const hrefMatch = res.match(/<d:href>([^<]+)<\/d:href>/) || res.match(/<href>([^<]+)<\/href>/);
      if (hrefMatch) {
        let path = hrefMatch[1];
        // Декодируем URL (превращаем %20 в пробелы и т.д.)
        path = decodeURIComponent(path);
        
        // Отрезаем полный путь, оставляя только имя папки
        const parts = path.split('/').filter(Boolean);
        const name = parts[parts.length - 1] || "/";
        
        // Не добавляем корневую папку в список выбора (чтобы не дублировать)
        if (name !== user.webdav_host.split('/').pop()) {
          folders.push({
            name: name,
            path: path
          });
        }
      }
    }
  });

  return folders;
}

async function createWebDavFolder(folderName, userData) {
  let host = userData.webdav_host || "";
  
  // Убеждаемся, что хост не заканчивается на слэш, чтобы не было двойного //
  if (host.endsWith('/')) host = host.slice(0, -1);
  
  const url = `${host}/${encodeURIComponent(folderName)}/`; // ← Обязательно с /
  const auth = btoa(`${userData.webdav_user}:${userData.webdav_pass}`);

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
    return renderSuccessPage();
  }
  return new Response("Error", { status: 400 });
}

async function uploadToDropboxFromArrayBuffer(arrayBuffer, fileName, accessToken, folderPath) {
  const path = (folderPath ? `/${folderPath}/${fileName}` : `/${fileName}`).replace(/\/+/g, '/');
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
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Dropbox API error: ${res.status} ${text}`);
  }
  return true;
}


async function uploadToDropboxStream(stream, name, token, folder, fileSize) {
  const path = (folder ? `/${folder}/${name}` : `/${name}`).replace(/\/+/g, '/');
  const arg = JSON.stringify({ path, mode: "overwrite" });

  const res = await fetch('https://content.dropboxapi.com/2/files/upload', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Dropbox-API-Arg': arg,
      'Content-Type': 'application/octet-stream'
    },
    body: stream
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
        path: "", // Попробуй сначала так, если не сработает - замени на "/"
        recursive: false
      })
    });

    const data = await res.json();

    if (!res.ok) {
      // Это поможет нам понять, что не так
      console.error("Dropbox Error:", data);
      return [];
    }

    // Dropbox возвращает папки с тегом "folder"
    const folders = (data.entries || [])
      .filter(item => item[".tag"] === "folder")
      .map(item => ({ 
        id: item.path_display.replace(/^\/+/, ''), // Убирает лидирующий /
        name: item.name 
      }));

    return folders;
  } catch (e) {
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
    if (res.status === 200 || data?.error_summary?.includes("path_already_exists")) {
      // Возвращаем folderName без слэша, чтобы фронт записал его в KV
      return folderName.replace(/^\/+/, '');
    }
    
    return false;
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

// ✅ *** callBothubDocumentVision - Обработчик для Document Analysis (BotHub/Gemini)
/**
 * @description Отправляет запрос на анализ документов через Bothub (Gemini 2.5 Flash).
 * @param {Object} config - Объект активной конфигурации
 * @param {ArrayBuffer} arrayBuffer - Файл в виде ArrayBuffer.
 * @param {Object} env - Объект окружения.
 * @param {string} mimeType - MIME-тип документа (e.g., 'application/pdf'). 
* @returns {Promise<string>} Сгенерированный текстовый анализ.
 */
async function callBothubDocumentVision(config, arrayBuffer, env, mimeType) {
  const apiKey = env[config.API_KEY];
  const baseUrl = config.BASE_URL;
  const model = config.MODEL;

  if (!apiKey) {
      throw new Error(`API Key для Document_to_Text (на BotHub) не настроен.`);
  }

   // Конвертируем ArrayBuffer в Base64
  const base64Data = arrayBufferToBase64(arrayBuffer);

  // --- ПЕРЕФОРМУЛИРОВАННАЯ СИСТЕМНАЯ ИНСТРУКЦИЯ (для Видеоаналитика) ---
  const systemMessage = "РОЛЬ И ЯЗЫК: Ты — эксперт по анализу документов.";
  
  // 2. Формирование тела запроса (мультимодальный формат для видео)
  const userPrompt = "Проанализируй документ. Дай краткое и информативное описание этого документа на русском языке. Сосредоточься на сути.";

  const messages = [
    { role: "system", content: systemMessage },
    {
      role: "user",
      content: [
        { type: "text", text: userPrompt },
        { 
          type: "image_url", 
          image_url: { 
            url: `data:${mimeType};base64,${base64Data}` 
          } 
        }
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
      throw new Error(`BOTHUB DOC2TXT API error (Status ${response.status}): ${errorText}. Это может быть связано с тем, что BotHub не пропускает видео в формате 'image_url', даже для Gemini.`);
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
      throw new Error(`BOTHUB DOC2TXT API response error: Received empty content from model.`);
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
  // --- BOTHUB VIDEO VISION --- (ПЛАТНО)
  DOCUMENT_TO_TEXT_BOTHUB: { 
    SERVICE: 'BOTHUB', 
    FUNCTION: callBothubDocumentVision, 
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