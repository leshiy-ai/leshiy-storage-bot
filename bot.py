import os
import asyncio
import sys
import aiogram
from ftplib import FTP
from datetime import datetime

# Импорты для веб-сервера и вебхуков
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# Импорты aiogram
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FOLDER = os.getenv("FTP_FOLDER")
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
VERSION = "1.4.1" # Фиксируем версию для дебага

# Превращаем строку "ID1,ID2" в список чисел
ALLOWED_IDS = [int(i.strip()) for i in os.getenv("ALLOWED_IDS", "").split(",") if i.strip()]

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- ЛОГИКА FTP ---
def upload_to_ftp(file_path, user_folder, file_name):
    with FTP() as ftp:
        ftp.connect(FTP_HOST, 21, timeout=30)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        ftp.set_pasv(True)
        
        # 1. Основная папка
        if FTP_FOLDER and FTP_FOLDER.strip():
            if FTP_FOLDER not in ftp.nlst():
                ftp.mkd(FTP_FOLDER)
            ftp.cwd(FTP_FOLDER)
        
        # 2. Папка пользователя
        if user_folder not in ftp.nlst():
            ftp.mkd(user_folder)
        ftp.cwd(user_folder)
        
        # 3. Загрузка
        with open(file_path, 'rb') as f:
            ftp.storbinary(f'STOR {file_name}', f)

# --- ВЕБ-СТРАНИЦЫ (БРАУЗЕР) ---
async def handle_index(request):
    html = f"""
    <html>
        <head><title>Хранилка by Leshiy</title></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
            <h1>🚀 Телеграм-бот "Хранилка" by Leshiy v{VERSION} активен!</h1>
            <p>Бот: <a href="https://t.me/leshiy_storage_bot">@leshiy_storage_bot</a></p>
            <p>Статус: <b>ONLINE ✅</b></p>
        </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

async def handle_debug_page(request):
    status_ftp = "Проверка..."
    try:
        # Проверка без блокировки (быстрая)
        with FTP() as ftp:
            ftp.connect(FTP_HOST, 21, timeout=5)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            status_ftp = "✅ Соединение установлено"
    except Exception:
        status_ftp = "❌ Ошибка (проверьте логи Render)"
    
    html = f"""
    <html>
        <head><title>System Debug</title></head>
        <body style="font-family: sans-serif; padding: 20px; line-height: 1.6;">
            <h2>🖥 Системная диагностика</h2>
            <p><b>Статус FTP:</b> {status_ftp}</p>
            <hr>
            <h3>Информация о среде:</h3>
            <ul>
                <li><b>Python:</b> {sys.version.split()[0]}</li>
                <li><b>Aiogram:</b> {aiogram.__version__}</li>
                <li><b>Версия бота:</b> {VERSION}</li>
            </ul>
            <p style="color: gray; font-size: 0.8em;">⚠️ Конфиденциальные данные (IP/Пароли) скрыты.</p>
            <p><a href="/">⬅ На главную</a></p>
        </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

# --- ОБРАБОТЧИКИ ТЕЛЕГРАМ ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я твоя личная FTP-хранилка.\n\n"
        "📁 Просто пришли мне любой файл, фото или видео, и я закину их на сервер.\n"
        "⚙️ Используй /debug чтобы проверить статус подключения."
    )

@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    status_ftp = "Проверка..."
    try:
        # Запускаем в потоке, чтобы бот не "тупил" при долгой проверке
        def check():
            with FTP() as ftp:
                ftp.connect(FTP_HOST, 21, timeout=10)
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                return "✅ Соединение установлено"
        status_ftp = await asyncio.to_thread(check)
    except Exception as e:
        status_ftp = f"❌ Ошибка: {e}"
    
    await message.answer(
        f"🤖 <b>Бот онлайн</b>\n"
        f"📦 <b>Версия:</b> {VERSION}\n"
        f"🔗 <b>FTP:</b> {status_ftp}\n"
        f"👤 <b>Твой ID:</b> <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    if message.from_user.id not in ALLOWED_IDS:
        await message.answer("🚫 У вас нет прав на сохранение.")
        return

    file_id, file_name = None, None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if message.photo:
        file_id = message.photo[-1].file_id
        file_name = f"photo_{timestamp}.jpg"
    elif message.video:
        file_id = message.video.file_id
        file_name = f"video_{timestamp}.mp4"
    elif message.document:
        mime = message.document.mime_type
        if mime and (mime.startswith('image/') or mime.startswith('video/')):
            file_id = message.document.file_id
            file_name = message.document.file_name # ИСХОДНОЕ ИМЯ
        else:
            await message.answer("⚠️ Только фото или видео!")
            return

    if not file_id: return

    msg = await message.answer("⏳ Загружаю на сервер...")
    try:
        file_info = await bot.get_file(file_id)
        temp_path = f"temp_{file_name}"
        await bot.download_file(file_info.file_path, temp_path)
        
        user_folder = message.from_user.full_name.replace(" ", "_")
        await asyncio.to_thread(upload_to_ftp, temp_path, user_folder, file_name)
        
        os.remove(temp_path)
        await msg.edit_text(f"✅ Файл \"{file_name}\" успешно сохранен!")
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}")

@dp.message()
async def reject_other(message: Message):
    await message.answer("⚠️ Присылайте только фото или видео.")

# --- ЗАПУСК ---
async def on_startup(bot: Bot):
    webhook_url = f"{RENDER_URL}/webhook"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)

def main():
    port = int(os.getenv("RENDER_PORT", 10000))
    app = web.Application()
    
    app.router.add_get("/", handle_index)
    app.router.add_get("/debug", handle_debug_page)
    
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path="/webhook")
    setup_application(app, dp, bot=bot)
    
    dp.startup.register(on_startup)
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
