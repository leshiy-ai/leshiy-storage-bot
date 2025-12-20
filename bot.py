import os
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message
from aiogram.filters import Command
from ftplib import FTP
from aiohttp import web
from aiogram.webhook.aiohttp_server import TokenBasedRequestHandler, setup_application

# --- НАСТРОЙКИ (берутся из Environment Variables на Render) ---
VERSION = "1.3.0"
TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
ALLOWED_IDS = os.getenv("ALLOWED_IDS", "").split(",")
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") # Например, https://leshiy-storage.onrender.com

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- ВЕБ-ОБРАБОТЧИК ДЛЯ RENDER (Health Check) ---
async def handle_http(request):
    return web.Response(text=f"Хранилка by Leshiy is running. Version: {VERSION}")

# --- ЛОГИКА FTP (в отдельной функции) ---
def upload_to_ftp(file_path, folder_name, file_name):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        # Проверяем, существует ли папка, если нет — создаем
        items = ftp.nlst()
        if folder_name not in items:
            ftp.mkd(folder_name)
        ftp.cwd(folder_name)
        # Загружаем файл
        with open(file_path, 'rb') as f:
            ftp.storbinary(f'STOR {file_name}', f)

# --- КОМАНДА /DEBUG ---
@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    status_ftp = "Проверка..."
    try:
        # Быстрая проверка соединения с FTP
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            status_ftp = "✅ Соединение с роутером установлено"
    except Exception as e:
        status_ftp = f"❌ Ошибка FTP: {e}"

    info = (
        f"🤖 **Бот:** Хранилка by Leshiy\n"
        f"📦 **Версия:** {VERSION}\n"
        f"🔗 **FTP Статус:** {status_ftp}\n"
        f"👤 **Твой ID:** `{message.from_user.id}`\n"
        f"🌐 **Webhook URL:** {RENDER_URL}/webhook"
    )
    await message.answer(info, parse_mode="Markdown")

# --- ОБРАБОТКА ФАЙЛОВ (Фото, Видео, Документы) ---
@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    # 1. Проверка доступа
    user_id = str(message.from_user.id)
    if user_id not in ALLOWED_IDS:
        return await message.answer(f"Доступ ограничен. Твой ID: {user_id}")

    wait_msg = await message.answer("📥 Начинаю загрузку в хранилище...")
    
    try:
        # 2. Определяем тип файла
        if message.document:
            file_obj = message.document
        elif message.video:
            file_obj = message.video
        else:
            file_obj = message.photo[-1] # Самое лучшее качество фото

        # 3. Скачиваем файл из Telegram
        file = await bot.get_file(file_obj.file_id)
        file_ext = file.file_path.split(".")[-1]
        # Используем уникальный ID файла, чтобы имена не повторялись
        file_name = f"{file_obj.file_unique_id}.{file_ext}"
        
        # Формируем имя папки: Имя_Фамилия
        first_name = message.from_user.first_name or "Unknown"
        last_name = message.from_user.last_name or ""
        user_folder = f"{first_name}_{last_name}".strip()
        
        local_path = f"temp_{file_name}"
        await bot.download_file(file.file_path, local_path)

        # 4. Отправляем на FTP (в отдельном потоке, чтобы не блокировать бота)
        await asyncio.to_thread(upload_to_ftp, local_path, user_folder, file_name)
        
        await wait_msg.edit_text(f"✅ Файл успешно сохранен в папку:\n`{user_folder}`", parse_mode="Markdown")
        
        # 5. Чистим временный файл на сервере Render
        if os.path.exists(local_path):
            os.remove(local_path)
            
    except Exception as e:
        await wait_msg.edit_text(f"❌ Произошла ошибка: {str(e)}")

# --- ОСНОВНОЙ ЗАПУСК ---
async def main():
    port = int(os.getenv("PORT", 10000))
    webhook_path = "/webhook"
    
    # Автоматическая установка вебхука
    if RENDER_URL:
        full_webhook_url = f"{RENDER_URL}{webhook_path}"
        await bot.set_webhook(full_webhook_url)
        print(f"Webhook set to: {full_webhook_url}")

    app = web.Application()
    
    # Настройка обработчика вебхуков
    handler = TokenBasedRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=webhook_path)
    
    # Путь для проверки Render (Health Check)
    app.router.add_get("/", handle_http)

    # Интеграция aiogram с aiohttp
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    print(f"Server started on port {port}")
    await site.start()
    
    # Бесконечный цикл
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
