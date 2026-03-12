const { Driver, getCredentialsFromEnv, getLogger, TypedValues } = require('yandex-cloud/nodejs-sdk');
const { worker_code_fetch } = require('./worker.js'); // Импортируем код воркера
const { initYdb, runQuery, USER_DB_ADAPTER, FILES_DB_ADAPTER, filesDriver } = require('./db_adapter.js');

// --- Handler (точка входа для Yandex.Cloud Functions) ---
module.exports.handler = async (event, context) => {
    try {
        // --- 1. Адаптация Yandex.Cloud Event в Request-совместимый объект ---
        const urlSearchParams = new URLSearchParams();
        if (event.queryStringParameters) {
            for (const key in event.queryStringParameters) {
                urlSearchParams.append(key, event.queryStringParameters[key]);
            }
        }
        const queryString = urlSearchParams.toString();

        // Формируем полный URL (примерный, для совместимости)
        // APP_DOMAIN должен быть в переменных окружения
        const domain = process.env.APP_DOMAIN || 'localhost';
        const fullUrl = `https://${domain}${event.url}${queryString ? '?' + queryString : ''}`;

        // Адаптируем заголовки
        const headers = new Headers();
        if (event.headers) {
            for (const key in event.headers) {
                headers.append(key, event.headers[key]);
            }
        }

        // Формируем объект Request
        const requestOptions = {
            method: event.httpMethod,
            headers: headers,
        };

        if (event.httpMethod !== 'GET' && event.httpMethod !== 'HEAD' && event.body) {
            requestOptions.body = event.isBase64Encoded ? Buffer.from(event.body, 'base64') : event.body;
        }

        const request = new Request(fullUrl, { ...requestOptions });

        const env = {
          USER_DB: USER_DB_ADAPTER, // Используем готовый адаптер
          FILES_DB: FILES_DB_ADAPTER,
          TypedValues: TypedValues, // Переменная из db_adapter
          runQuery: runQuery,       // Функция из db_adapter
          filesDriver: filesDriver, // Драйвер из db_adapter
          // --- Переменные окружения ---
          APP_DOMAIN: process.env.APP_DOMAIN,
          BOTHUB_API_KEY: process.env.BOTHUB_API_KEY,
          BOT_USERNAME: process.env.BOT_USERNAME,
          CLOUDFLARE_ACCOUNT_ID: process.env.CLOUDFLARE_ACCOUNT_ID,
          CLOUDFLARE_API_TOKEN: process.env.CLOUDFLARE_API_TOKEN,
          DEEPSEEK_API_KEY: process.env.DEEPSEEK_API_KEY,
          DROPBOX_CLIENT_ID: process.env.DROPBOX_CLIENT_ID,
          DROPBOX_CLIENT_SECRET: process.env.DROPBOX_CLIENT_SECRET,
          GEMINI_API_KEY: process.env.GEMINI_API_KEY,
          GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
          GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
          MAILRU_CLIENT_ID: process.env.MAILRU_CLIENT_ID,
          MAILRU_CLIENT_PRIVATE: process.env.MAILRU_CLIENT_PRIVATE,
          MAILRU_CLIENT_SECRET: process.env.MAILRU_CLIENT_SECRET,
          TELEGRAM_TOKEN: process.env.TELEGRAM_TOKEN,
          VK_APP_ID: process.env.VK_APP_ID,
          VK_GROUP_ID: process.env.VK_GROUP_ID,
          VK_GROUP_TOKEN: process.env.VK_GROUP_TOKEN,
          VK_SECURE_KEY: process.env.VK_SECURE_KEY,
          YANDEX_CLIENT_ID: process.env.YANDEX_CLIENT_ID,
          YANDEX_CLIENT_SECRET: process.env.YANDEX_CLIENT_SECRET
      };


        // --- 2. Вызов логики воркера ---
        const response = await worker_code_fetch(request, env, context);

        // --- 3. Адаптация Response в формат Yandex.Cloud Functions ---
        const responseBody = await response.text();
        const responseHeaders = {};
        for (const [key, value] of response.headers.entries()) {
            responseHeaders[key] = value;
        }

        return {
            statusCode: response.status,
            headers: responseHeaders,
            body: responseBody,
            isBase64Encoded: false // Предполагаем, что воркер не отдает base64
        };

    } catch (error) {
        console.error('Critical error in handler:', error);
        return {
            statusCode: 500,
            body: 'Internal Server Error: ' + error.message
        };
    }
};

// Инициализация YDB при старте функции
initYdb();
