import os
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message
from aiogram.filters import Command
from ftplib import FTP
from aiohttp import web
from aiogram.webhook.urls import TokenBasedRequestHandler

# --- НАСТРОЙКИ ---
VERSION = "1.2.0 (Webhook Mode)"
TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
ALLOWED_IDS = os.getenv("ALLOWED_IDS", "").split(",")
# URL твоего сервиса на Render (напр. https://my-bot.onrender.com)
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL") 

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- МИНИ ВЕБ-СЕРВЕР (Health Check) ---
async def handle_http(request):
    return web.Response(text=f"Хранилка by Leshiy is running. Version: {VERSION}")

# --- ЛОГИКА FTP ---
def upload_to_ftp(file_path, folder_name, file_name):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        items = ftp.nlst()
        if folder_name not in items:
            ftp.mkd(folder_name)
        ftp.cwd(folder_name)
        with open(file_path, 'rb') as f:
            ftp.storbinary(f'STOR {file_name}', f)

# --- КОМАНДЫ ---
@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    status_ftp = "Проверка..."
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            status_ftp = "✅ Соединение установлено"
    except Exception as e:
        status_ftp = f"❌ Ошибка: {e}"

    info = (
        f"🤖 **Бот:** Хранилка by Leshiy\n"
        f"📦 **Версия:** {VERSION}\n"
        f"🔗 **FTP Статус:** {status_ftp}\n"
        f"👤 **Твой ID:** `{message.from_user.id}`"
    )
    await message.answer(info, parse_mode="Markdown")

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    if str(message.from_user.id) not in ALLOWED_IDS:
        return await message.answer("Доступ ограничен 🛑")

    wait_msg = await message.answer("📥 Загружаю...")
    
    if message.document:
        file_obj = message.document
    elif message.video:
        file_obj = message.video
    else:
        file_obj = message.photo[-1]

    file = await bot.get_file(file_obj.file_id)
    file_ext = file.file_path.split(".")[-1]
    file_name = f"{file_obj.file_unique_id}.{file_ext}"
    user_folder = f"{message.from_user.first_name}_{message.from_user.last_name or ''}".strip()
    
    local_path = f"temp_{file_name}"
    await bot.download_file(file.file_path, local_path)

    try:
        await asyncio.to_thread(upload_to_ftp, local_path, user_folder, file_name)
        await wait_msg.edit_text(f"✅ Сохранено в папку: {user_folder}")
    except Exception as e:
        await wait_msg.edit_text(f"❌ Ошибка сохранения: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

# --- ЗАПУСК ЧЕРЕЗ WEBHOOK ---
async def main():
    # Настройка порта (Render дает его сам)
    port = int(os.getenv("PORT", 10000))
    webhook_path = "/webhook"
    webhook_url = f"{RENDER_URL}{webhook_path}"

    # Установка вебхука в Telegram
    await bot.set_webhook(webhook_url)
    print(f"Webhook set to: {webhook_url}")

    # Создание приложения aiohttp
    app = web.Application()
    
    # Регистрация обработчика вебхука
    handler = TokenBasedRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=webhook_path)

    # Добавляем обычный хендлер для главной страницы (чтобы Render видел порт)
    app.router.add_get("/", handle_http)

    # Запуск сервера
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    print(f"Starting server on port {port}...")
    await site.start()
    
    # Бесконечное ожидание
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
