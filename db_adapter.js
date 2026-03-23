const { Driver, getCredentialsFromEnv, TypedValues } = require('ydb-sdk');

const config = {
    userDb: {
        // Пробуем взять из системы, если пусто - берем твой адрес
        endpoint: 'grpcs://ydb.serverless.yandexcloud.net:2135',
        database: '/ru-central1/b1gov9f01s68fqhqbg7m/etnjbpn3j100lbfkh9rf',
    },
    filesDb: {
        endpoint: 'grpcs://ydb.serverless.yandexcloud.net:2135',
        database: '/ru-central1/b1gov9f01s68fqhqbg7m/etnij2u9r8dhav6finsm',
    }
};

// Самый надежный способ авторизации внутри функций Яндекса
const authService = getCredentialsFromEnv();

const userDriver = new Driver({ 
    endpoint: config.userDb.endpoint, 
    database: config.userDb.database, 
    authService 
});
const filesDriver = new Driver({ 
    endpoint: config.filesDb.endpoint, 
    database: config.filesDb.database, 
    authService 
});

async function runQuery(driver, query, params = {}) {
    return await driver.tableClient.withSession(async (session) => {
        return await session.executeQuery(query, params);
    });
}

const USER_DB_ADAPTER = {
    async get(key) {
        try {
            const query = `DECLARE $key AS Utf8; SELECT value FROM users WHERE key = $key;`;
            const result = await runQuery(userDriver, query, { '$key': TypedValues.utf8(key) });
            if (!result || !result.resultSets || result.resultSets[0].rows.length === 0) return null;
            const item = result.resultSets[0].rows[0].items[0];
            let val = item.textValue !== undefined ? item.textValue : (item.value !== undefined ? item.value : null);
            if (val === null) return null;
            // Если это строка, которая НЕ похожа на JSON (просто текст)
            if (typeof val === 'string' && !val.trim().startsWith('{') && !val.trim().startsWith('[')) {
                return val; 
            }
            // Если это уже объект - отдаем как есть
            if (typeof val === 'object') return val;
            // Если это строка JSON - пробуем распарсить
            try {
                return JSON.parse(val);
            } catch (e) {
                return val;
            }
        } catch (e) {
            console.error("DB GET ERROR:", key, e);
            return null;
        }
    },
    async put(key, value) {
        try {
            // Используем REPLACE — он в YDB работает быстрее и надежнее для "создать или перезаписать"
            const query = `DECLARE $key AS Utf8; DECLARE $value AS Utf8; REPLACE INTO users (key, value) VALUES ($key, $value);`;
            
            let valStr = typeof value === 'string' ? value : JSON.stringify(value);
            
            // Лог в консоль Яндекса, чтобы увидеть, дошли ли мы до этого места
            console.log(`[DB-PUT] Пробую создать/обновить: ${key}`);

            await runQuery(userDriver, query, { 
                '$key': TypedValues.utf8(String(key)),
                '$value': TypedValues.utf8(valStr)
            });
            
            console.log(`[DB-PUT] Готово: ${key}`);
        } catch (e) {
            console.error(`[DB-PUT-ERROR] Ключ: ${key}, Ошибка:`, e);
        }
    },
    // НОВЫЙ МЕТОД: Листинг ключей
    async list(options = {}) {
        try {
            const prefix = options.prefix || '';
            // Используем YQL синтаксис для поиска по префиксу
            const query = `DECLARE $prefix AS Utf8; SELECT key FROM users WHERE key LIKE $prefix || '%';`;
            const result = await runQuery(userDriver, query, { 
                '$prefix': TypedValues.utf8(prefix) 
            });
            
            // Извлекаем ключи из результата YDB
            const keys = result.resultSets[0].rows.map(row => ({
                name: row.items[0].textValue
            }));
            
            return { keys, list_complete: true };
        } catch (e) {
            console.error("DB LIST ERROR:", e);
            return { keys: [], list_complete: true };
        }
    },
    // ДОБАВЛЕННЫЙ МЕТОД DELETE
    async delete(key) {
        try {
            const query = `DECLARE $key AS Utf8; DELETE FROM users WHERE key = $key;`;
            console.log(`[DB-DELETE] Удаление ключа: ${key}`);
            await runQuery(userDriver, query, { 
                '$key': TypedValues.utf8(String(key)) 
            });
            console.log(`[DB-DELETE] Успешно удалено: ${key}`);
            return true;
        } catch (e) {
            console.error(`[DB-DELETE-ERROR] Ключ: ${key}, Ошибка:`, e);
            return false;
        }
    }
};

const FILES_DB_ADAPTER = {
    prepare(sql) {
        // Определяем, что пришло: INSERT, UPDATE или SELECT
        const isUpdate = sql.includes('UPDATE');
        const isSelect = sql.includes('SELECT');

        return {
            bind: (...args) => {
                return {
                    run: async () => {
                        try {
                            if (isUpdate) {
                                // --- ЛОГИКА ДЛЯ АНАЛИТИКИ (Обновление по fileId) ---
                                const query = `
                                    DECLARE $ai_desc AS Utf8;
                                    DECLARE $fileId AS Utf8;
                                    UPDATE files SET ai_description = $ai_desc 
                                    WHERE fileId = $fileId;
                                `;
                                const params = {
                                    '$ai_desc': TypedValues.utf8(String(args[0])),
                                    '$fileId': TypedValues.utf8(String(args[1])) // Берем второй аргумент из .bind()
                                };
                                await runQuery(filesDriver, query, params);
                                console.log(`[FILES-DB] Аналитика записана для ID: ${args[1]}`);
                            } else {
                                // --- ТВОЯ РАБОЧАЯ ЛОГИКА (INSERT) ---
                                const query = `
                                    DECLARE $id AS Utf8; DECLARE $userId AS Utf8; DECLARE $fileName AS Utf8;
                                    DECLARE $fileId AS Utf8; DECLARE $fileType AS Utf8; DECLARE $provider AS Utf8;
                                    DECLARE $folderId AS Utf8; DECLARE $timestamp AS Datetime;
                                    REPLACE INTO files (id, userId, fileName, fileId, fileType, provider, folderId, timestamp)
                                    VALUES ($id, $userId, $fileName, $fileId, $fileType, $provider, $folderId, $timestamp);
                                `;
                                const params = {
                                    '$id': TypedValues.utf8(String(args[2])),
                                    '$userId': TypedValues.utf8(String(args[0])),
                                    '$fileName': TypedValues.utf8(String(args[1])),
                                    '$fileId': TypedValues.utf8(String(args[2])),
                                    '$fileType': TypedValues.utf8(String(args[3])),
                                    '$provider': TypedValues.utf8(String(args[4])),
                                    '$folderId': TypedValues.utf8(String(args[5])),
                                    '$timestamp': TypedValues.datetime(new Date(args[6]))
                                };
                                await runQuery(filesDriver, query, params);
                                console.log(`[FILES-DB] Запись создана: ${args[1]}`);
                            }
                            return { success: true, meta: { changes: 1 } };
                        } catch (e) {
                            console.error(`[FILES-DB-ERROR]`, e);
                            return { success: false, error: e.message };
                        }
                    },
                    // Логика для поиска (SELECT)
                    all: async () => {
                        try {
                            const isAISearch = sql.includes('ai_description');
                            let query;
                            let p;

                            if (isAISearch) {
                                // Оставляем как было вчера - это для ИИ
                                query = sql.includes('userId = ?')
                                    ? `DECLARE $u AS Utf8; SELECT id, fileName, ai_description FROM files WHERE userId = $u AND ai_description IS NOT NULL ORDER BY timestamp DESC LIMIT 50;`
                                    : `SELECT id, fileName, ai_description FROM files WHERE ai_description IS NOT NULL ORDER BY timestamp DESC LIMIT 50;`;
                                p = sql.includes('userId = ?') ? { '$u': TypedValues.utf8(String(args[0])) } : {};
                            } else {
                                // ОБЫЧНЫЙ ПОИСК (Для Телеги и ВК)
                                // Мы берем ВСЕ нужные поля, чтобы они были в объекте
                                query = sql.includes('userId = ?') 
                                    ? `DECLARE $u AS Utf8; DECLARE $s AS Utf8; 
                                    SELECT id, fileName, provider, folderId, timestamp, fileType, fileId 
                                    FROM files 
                                    WHERE userId = $u AND (Unicode::ToLower(fileName) LIKE Unicode::ToLower($s) OR Unicode::ToLower(tags) LIKE Unicode::ToLower($s)) 
                                    ORDER BY timestamp DESC LIMIT 50;`
                                    : `DECLARE $s AS Utf8; 
                                    SELECT id, fileName, provider, folderId, timestamp, fileType, fileId 
                                    FROM files 
                                    WHERE (Unicode::ToLower(fileName) LIKE Unicode::ToLower($s) OR Unicode::ToLower(tags) LIKE Unicode::ToLower($s)) 
                                    ORDER BY timestamp DESC LIMIT 100;`;

                                p = sql.includes('userId = ?') 
                                    ? { '$u': TypedValues.utf8(String(args[0])), '$s': TypedValues.utf8(String(args[1])) }
                                    : { '$s': TypedValues.utf8(String(args[0])) };
                            }

                            const res = await runQuery(filesDriver, query, p);
                            
                            const rows = res.resultSets[0].rows.map(r => {
                                // 1. Базовый маппинг (id и name всегда 0 и 1)
                                const item = {
                                    id: r.items[0]?.textValue || r.items[0]?.uint64Value?.toString(),
                                    fileName: r.items[1]?.textValue
                                };

                                if (isAISearch) {
                                    item.ai_description = r.items[2]?.textValue || null;
                                } else {
                                    // 2. Маппинг для обычного поиска (СТРОГО по порядку в SELECT выше)
                                    item.provider = r.items[2]?.textValue || "unknown";
                                    item.folderId = r.items[3]?.textValue || "";
                                    
                                    // Исправляем дату (она у тебя строкой в базе)
                                    const dateStr = r.items[4]?.textValue;
                                    item.timestamp = dateStr ? new Date(dateStr).getTime() : Date.now();
                                    
                                    item.fileType = r.items[5]?.textValue || "file";
                                    item.fileId = r.items[6]?.textValue || item.id;
                                    
                                    // Для совместимости с твоим фронтендом (если он ждет ai_description)
                                    item.ai_description = null; 
                                }
                                return item;
                            });

                            return { success: true, results: rows };
                        } catch (e) {
                            console.error("CRITICAL SEARCH ERROR:", e);
                            return { success: false, results: [] };
                        }
                    },
                    first: async () => {
                        try {
                            const idVal = String(args[0]);
                            // ВАЖНО: folderId вместо remotePath. И убедись, что fileId тоже тут.
                            const query = `DECLARE $id AS Utf8; SELECT id, fileName, provider, folderId, fileId, fileType FROM files WHERE id = $id LIMIT 1;`;
                            const res = await runQuery(filesDriver, query, { '$id': TypedValues.utf8(idVal) });
                            
                            if (res && res.resultSets && res.resultSets[0].rows && res.resultSets[0].rows.length > 0) {
                                const r = res.resultSets[0].rows[0];
                                // Мапим колонки СТРОГО по порядку из SELECT выше:
                                // 0:id, 1:fileName, 2:provider, 3:folderId, 4:fileId, 5:fileType
                                return {
                                    id: r.items[0].textValue,
                                    fileName: r.items[1].textValue,
                                    provider: r.items[2].textValue,
                                    folderId: r.items[3].textValue,
                                    fileId: r.items[4].textValue,
                                    fileType: r.items[5].textValue
                                };
                            }
                            return null;
                        } catch (e) {
                            console.error("ADAPTER ERROR:", e);
                            return null;
                        }
                    }
                };
            }
        };
    }
};

module.exports = { USER_DB_ADAPTER, FILES_DB_ADAPTER, TypedValues, runQuery, filesDriver };