import os
import asyncio
import platform
import aiogram
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message
from aiogram.filters import Command
from ftplib import FTP
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# --- НАСТРОЙКИ ---
VERSION = "1.4.0"
TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
ALLOWED_IDS = os.getenv("ALLOWED_IDS", "").split(",")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- HTTP ОБРАБОТЧИКИ (для браузера) ---

async def handle_root(request):
    return web.Response(text=f"Хранилка by Leshiy. Version: {VERSION}", content_type='text/html')

async def handle_debug_url(request):
    """Отображает системную информацию при переходе на /debug в браузере"""
    debug_info = (
        f"<h1>System Debug Info</h1>"
        f"<ul>"
        f"<li><b>Project:</b> Leshiy Storage Bot</li>"
        f"<li><b>Version:</b> {VERSION}</li>"
        f"<li><b>Python:</b> {platform.python_version()}</li>"
        f"<li><b>Aiogram:</b> {aiogram.__version__}</li>"
        f"<li><b>Aiohttp:</b> {aiohttp.__version__}</li>"
        f"<li><b>OS:</b> {platform.system()} {platform.release()}</li>"
        f"</ul>"
    )
    return web.Response(text=debug_info, content_type='text/html')

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

# --- КОМАНДЫ БОТА (в Телеграм) ---
@dp.message(Command("debug"))
async def cmd_debug_bot(message: Message):
    status_ftp = "Проверка..."
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            status_ftp = "✅ Соединение установлено"
    except Exception as e:
        status_ftp = f"❌ Ошибка: {e}"

    await message.answer(
        f"🤖 Бот онлайн\n📦 Версия: {VERSION}\n🔗 FTP: {status_ftp}\n👤 Твой ID: `{message.from_user.id}`",
        parse_mode="Markdown"
    )

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    if str(message.from_user.id) not in ALLOWED_IDS:
        return await message.answer(f"Доступ ограничен. ID: {message.from_user.id}")

    wait_msg = await message.answer("📥 Загрузка...")
    
    try:
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
        await asyncio.to_thread(upload_to_ftp, local_path, user_folder, file_name)
        
        await wait_msg.edit_text(f"✅ Сохранено в папку: {user_folder}")
        
        if os.path.exists(local_path):
            os.remove(local_path)
    except Exception as e:
        await wait_msg.edit_text(f"❌ Ошибка: {str(e)}")

# --- ЗАПУСК ---
async def main():
    port = int(os.getenv("PORT", 10000))
    webhook_path = "/webhook"
    
    # Мы УБРАЛИ bot.set_webhook. 
    # Теперь тебе нужно самому запустить ссылку в браузере:
    # https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://leshiy-storage-bot.onrender.com/webhook

    app = web.Application()
    
    # Роуты для браузера
    app.router.add_get("/", handle_root)
    app.router.add_get("/debug", handle_debug_url)
    
    # Обработчик вебхука от Telegram
    handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=webhook_path)
    
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    print(f"Server started on port {port}")
    await site.start()
    
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
