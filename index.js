const { USER_DB_ADAPTER, FILES_DB_ADAPTER, TypedValues, runQuery, filesDriver } = require('./db_adapter');
const nodeCrypto = require('crypto');
const worker = require('./worker'); 
const fetch = require('node-fetch');
const AWS = require('aws-sdk');

// Глобальные пропсы для имитации среды Cloudflare
global.fetch = fetch;
global.Headers = fetch.Headers;
global.Request = fetch.Request;
global.Response = fetch.Response;

module.exports.handler = async (event, context) => {
    // ПАРСИНГ ТЕЛА (для Telegram)
    let body = {};
    try {
        body = typeof event.body === 'string' ? JSON.parse(event.body) : (event.body || {});
    } catch (e) {
        body = event.body;
    }

    // СБОРКА ПОЛНОГО URL
    const uri = event.headers['x-envoy-original-path'] || event.url || '/';
    const domain = process.env.APP_DOMAIN || "d5dtt5rfr7nk66bbrec2.kf69zffa.apigw.yandexcloud.net";
    const fullUrl = `https://${domain}${uri}`;
    console.log("🛠 URL ДЛЯ ВОРКЕРА:", fullUrl);

    // СОЗДАНИЕ ОБЪЕКТА ЗАПРОСА
    const requestOptions = {
        method: event.httpMethod,
        headers: event.headers,
    };

    if (event.httpMethod !== 'GET' && event.httpMethod !== 'HEAD' && event.body) {
        requestOptions.body = event.isBase64Encoded ? Buffer.from(event.body, 'base64') : event.body;
    }

    const request = new Request(fullUrl, { ...requestOptions });

    const env = {
        ...process.env,           // Подтягиваем ВСЕ 30+ переменных из GitHub/Yandex
        USER_DB: USER_DB_ADAPTER, // Используем готовый адаптер
        FILES_DB: FILES_DB_ADAPTER,
        TypedValues: TypedValues, // Переменная из db_adapter
        runQuery: runQuery,       // Функция из db_adapter
        filesDriver: filesDriver, // Драйвер из db_adapter
        nodeCrypto: nodeCrypto,
        // Остальные переменные (токены)
        APP_DOMAIN: process.env.APP_DOMAIN,
        BOTHUB_API_KEY: process.env.BOTHUB_API_KEY,
        BOT_USERNAME: process.env.BOT_USERNAME,
        CLOUDFLARE_ACCOUNT_ID: process.env.CLOUDFLARE_ACCOUNT_ID,
        CLOUDFLARE_API_TOKEN: process.env.CLOUDFLARE_API_TOKEN,
        DEEPSEEK_API_KEY: process.env.DEEPSEEK_API_KEY,
        DROPBOX_CLIENT_ID: process.env.DROPBOX_CLIENT_ID,
        DROPBOX_CLIENT_SECRET: process.env.DROPBOX_CLIENT_SECRET,
        GEMINI_API_KEY: process.env.GEMINI_API_KEY,
        GEMINI_BOT_TOKEN: process.env.GEMINI_BOT_TOKEN,
        GEMINI_PROXY_KEY: process.env.GEMINI_PROXY_KEY,
        GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
        GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
        MAILRU_CLIENT_ID: process.env.MAILRU_CLIENT_ID,
        MAILRU_CLIENT_PRIVATE: process.env.MAILRU_CLIENT_PRIVATE,
        MAILRU_CLIENT_SECRET: process.env.MAILRU_CLIENT_SECRET,
        OK_APP_ID: process.env.OK_APP_ID,
        TELEGRAM_TOKEN: process.env.TELEGRAM_TOKEN,
        VK_APP_ID: process.env.VK_APP_ID,
        VK_GROUP_ID: process.env.VK_GROUP_ID,
        VK_GROUP_TOKEN: process.env.VK_GROUP_TOKEN,
        VK_SECURE_KEY: process.env.VK_SECURE_KEY,
        VK_SERVICE_KEY: process.env.VK_SERVICE_KEY,
        YANDEX_API_KEY: process.env.YANDEX_API_KEY,
        YANDEX_CLIENT_ID: process.env.YANDEX_CLIENT_ID,
        YANDEX_CLIENT_SECRET: process.env.YANDEX_CLIENT_SECRET,
        YANDEX_FOLDER_ID: process.env.YANDEX_FOLDER_ID,
        YANDEX_S3_KEY_ID: process.env.YANDEX_S3_KEY_ID,
        YANDEX_S3_SECRET: process.env.YANDEX_S3_SECRET        
    };

    const ctx = { waitUntil: (promise) => promise };

    // ЗАПУСК ВОРКЕРА
    try {
        if (body.callback_query) {
            console.log(`[TELEGRAM] Клик: ${body.callback_query.data}`);
        }

        // Вызываем именно ту функцию, которая у тебя в worker.js
        const response = await worker.worker_code_fetch(request, env, ctx);
        
        // Обработка ответа
        if (!response || typeof response.text !== 'function') {
            return {
                statusCode: 200,
                body: typeof response === 'string' ? response : JSON.stringify(response || "OK")
            };
        }

        const resText = await response.text();
        const resHeaders = {};
        if (response.headers) {
            response.headers.forEach((v, k) => { resHeaders[k] = v; });
        }

        return {
            statusCode: response.status || 200,
            headers: resHeaders,
            body: resText
        };
    } catch (e) {
        console.error("CRITICAL RUNTIME ERROR:", e);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: e.message })
        };
    }
};