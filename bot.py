import os
import asyncio
from ftplib import FTP
from datetime import datetime
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ContentType
from aiogram.filters import Command
from aiogram.utils.token import TokenValidationError

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FOLDER = os.getenv("FTP_FOLDER")
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
        
        # 1. Основная папка (если задана)
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

# --- ВЕБ-СТРАНИЦЫ ДЛЯ БРАУЗЕРА ---

async def handle_index(request):
    html = """
    <html>
        <head><title>Хранилка by Leshiy</title></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
            <h1>🚀 Телеграм-бот "Хранилка" by Leshiy активен!</h1>
            <p>Бот доступен по адресу: <a href="https://t.me/leshiy_storage_bot">@leshiy_storage_bot</a></p>
            <p>Статус системы: <b>ONLINE ✅</b></p>
        </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

async def handle_debug_page(request):
    status_ftp = "Проверка..."
    try:
        with FTP() as ftp:
            ftp.connect(FTP_HOST, 21, timeout=5)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            status_ftp = "✅ FTP Соединение установлено"
    except Exception as e:
        status_ftp = f"❌ Ошибка FTP: {e}"
    
    html = f"""
    <html>
        <body style="font-family: sans-serif; padding: 20px;">
            <h2>🖥 Диагностика системы</h2>
            <p><b>Бот:</b> @leshiy_storage_bot ✅</p>
            <p><b>FTP Host:</b> {FTP_HOST}</p>
            <p><b>Статус связи:</b> {status_ftp}</p>
            <hr>
            <p><a href="/">На главную</a></p>
        </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')
    
# --- ОБРАБОТЧИКИ КОМАНД ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("👋 Бот-хранилка готов к работе! Присылай фото или видео.")

@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    status_ftp = "Проверка..."
    try:
        with FTP() as ftp:
            ftp.connect(FTP_HOST, 21, timeout=10)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.set_pasv(True)
            status_ftp = "✅ Соединение установлено"
    except Exception as e:
        status_ftp = f"❌ Ошибка: {e}"
    
    await message.answer(f"🤖 Бот онлайн\n🔗 FTP: {status_ftp}\n👤 Твой ID: {message.from_user.id}")

# Универсальный обработчик фото и видео
@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    user_id = message.from_user.id
    
    # 1. Проверка на право сохранения
    if user_id not in ALLOWED_IDS:
        await message.answer("🚫 У вас нет прав на сохранение файлов в хранилище.")
        return

    file_id = None
    file_name = None
    
    # 2. Логика имен и типов
    if message.photo:
        # Сжатое фото: генерируем имя по дате
        file_id = message.photo[-1].file_id
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"photo_{timestamp}.jpg"
    
    elif message.video:
        # Видео (обычно сжатое): по дате
        file_id = message.video.file_id
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"video_{timestamp}.mp4"
        
    elif message.document:
        # Документ (несжатое): проверяем, фото это или видео
        mime = message.document.mime_type
        if mime and (mime.startswith('image/') or mime.startswith('video/')):
            file_id = message.document.file_id
            file_name = message.document.file_name # ИСХОДНОЕ ИМЯ
        else:
            await message.answer("⚠️ Файл не принимается. Разрешены только фото и видео.")
            return

    if not file_id:
        return

    # 3. Процесс загрузки
    msg = await message.answer("⏳ Загружаю на сервер...")
    try:
        file = await bot.get_file(file_id)
        file_path = f"temp_{file_name}"
        await bot.download_file(file.file_path, file_path)
        
        user_folder = message.from_user.full_name.replace(" ", "_")
        
        await asyncio.to_thread(upload_to_ftp, file_path, user_folder, file_name)
        
        os.remove(file_path)
        await msg.edit_text(f"✅ Файл \"{file_name}\" успешно сохранен в папку {user_folder}!")
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка загрузки: {e}")

# Запрет всего остального (голосовые, стикеры, локации и т.д.)
@dp.message()
async def reject_other(message: Message):
    if not (message.photo or message.video or message.document):
        await message.answer("⚠️ Этот тип сообщений не поддерживается. Присылайте только фото или видео.")

async def on_startup(bot: Bot):
    # Устанавливаем вебхук при запуске
    webhook_url = os.getenv("RENDER_EXTERNAL_URL") + "/webhook"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)

def main():
    # Render сам подставляет PORT, если его нет — берем 10000
    port = int(os.getenv("RENDER_PORT", 10000))
    app = web.Application()
    
# Маршруты для браузера
    app.router.add_get("/", handle_index)
    app.router.add_get("/debug", handle_debug_page)
    
    # Маршрут для Телеграма
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path="/webhook")
    
    setup_application(app, dp, bot=bot)
    dp.startup.register(on_startup)
    
    web.run_app(app, host="0.0.0.0", port=port)
    
if __name__ == "__main__":
    main()
