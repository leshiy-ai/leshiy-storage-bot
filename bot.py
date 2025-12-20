import os
import asyncio
import sys      # Для вывода версии в веб-дебаг
import aiogram  # Для вывода версии в веб-дебаг
from ftplib import FTP
from datetime import datetime

# Импорты для веб-сервера и вебхуков
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# Импорты aiogram
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.utils.token import TokenValidationError # Возвращено

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FOLDER = os.getenv("FTP_FOLDER")
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
VERSION = "1.4.1"

# Превращаем строку "ID1,ID2" в список чисел (строгая проверка)
try:
    ALLOWED_IDS = [int(i.strip()) for i in os.getenv("ALLOWED_IDS", "").split(",") if i.strip()]
except Exception as e:
    print(f"Ошибка парсинга ALLOWED_IDS: {e}")
    ALLOWED_IDS = []

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- ЛОГИКА FTP (С ПОЛНЫМИ КОММЕНТАРИЯМИ) ---
def upload_to_ftp(file_path, user_folder, file_name):
    """
    Функция для загрузки файла на FTP сервер.
    Создает структуру папок, если они отсутствуют.
    """
    with FTP() as ftp:
        ftp.connect(FTP_HOST, 21, timeout=30)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        ftp.set_pasv(True)
        
        # 1. Основная папка (если задана в настройках)
        if FTP_FOLDER and FTP_FOLDER.strip():
            if FTP_FOLDER not in ftp.nlst():
                ftp.mkd(FTP_FOLDER)
            ftp.cwd(FTP_FOLDER)
        
        # 2. Персональная папка пользователя (Alexandr_Ogoreltsev и т.д.)
        if user_folder not in ftp.nlst():
            ftp.mkd(user_folder)
        ftp.cwd(user_folder)
        
        # 3. Загрузка бинарного файла
        with open(file_path, 'rb') as f:
            ftp.storbinary(f'STOR {file_name}', f)

# --- ВЕБ-СТРАНИЦЫ (БРАУЗЕР) ---
async def handle_index(request):
    html = f"""
    <html>
        <head><title>Хранилка by Leshiy</title></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f4f4f9;">
            <h1 style="color: #333;">🚀 Телеграм-бот "Хранилка" by Leshiy v{VERSION}</h1>
            <p>Бот доступен по адресу: <a href="https://t.me/leshiy_storage_bot">@leshiy_storage_bot</a></p>
            <p>Статус системы: <span style="color: green; font-weight: bold;">ONLINE ✅</span></p>
        </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

async def handle_debug_page(request):
    """Безопасная страница диагностики для браузера"""
    status_ftp = "Checking..."
    try:
        with FTP() as ftp:
            ftp.connect(FTP_HOST, 21, timeout=5)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            status_ftp = "Connected ✅"
    except Exception:
        status_ftp = "Disconnected ❌"
    
    html = f"""
    <html>
        <head><title>System Status</title></head>
        <body style="font-family: monospace; padding: 20px; background-color: #1e1e1e; color: #d4d4d4;">
            <h2 style="color: #569cd6;">🖥 System Diagnostics</h2>
            <p><b>Storage Status:</b> {status_ftp}</p>
            <hr style="border: 0.5px solid #444;">
            <p><b>Version:</b> {VERSION}</p>
            <p><b>Environment:</b> Python {sys.version.split()[0]} | Aiogram {aiogram.__version__}</p>
            <hr style="border: 0.5px solid #444;">
            <p><a href="/" style="color: #ce9178;">[ Back to Home ]</a></p>
        </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

# --- ОБРАБОТЧИКИ КОМАНД TELEGRAM ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я твоя личная FTP-хранилка.\n\n"
        "📁 Просто пришли мне любой файл, фото или видео, и я закину их на сервер.\n"
        "⚙️ Используй /debug чтобы проверить статус подключения."
    )

@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    """Команда для проверки связи с FTP из чата"""
    def check_ftp():
        try:
            with FTP() as ftp:
                ftp.connect(FTP_HOST, 21, timeout=10)
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                return "✅ Соединение установлено"
        except Exception as e:
            return f"❌ Ошибка: {e}"
    
    status_ftp = await asyncio.to_thread(check_ftp)
    await message.answer(
        f"🤖 <b>Бот онлайн</b>\n"
        f"📦 <b>Версия:</b> {VERSION}\n"
        f"🔗 <b>FTP:</b> {status_ftp}\n"
        f"👤 <b>Твой ID:</b> <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

# --- ОСНОВНАЯ ЛОГИКА ФАЙЛОВ ---

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    user_id = message.from_user.id
    
    # 1. Проверка прав доступа
    if user_id not in ALLOWED_IDS:
        await message.answer("🚫 У вас нет прав на сохранение файлов.")
        return

    file_id = None
    file_name = None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 2. Определяем тип контента и формируем имя
    if message.photo:
        file_id = message.photo[-1].file_id
        file_name = f"photo_{timestamp}.jpg"
    
    elif message.video:
        file_id = message.video.file_id
        file_name = f"video_{timestamp}.mp4"
        
    elif message.document:
        mime = message.document.mime_type
        # Проверка, что документ является медиа-файлом
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
        file_info = await bot.get_file(file_id)
        temp_path = f"temp_{file_name}"
        await bot.download_file(file_info.file_path, temp_path)
        
        # Формируем имя папки (заменяем пробелы)
        user_folder = message.from_user.full_name.replace(" ", "_")
        
        # Выполняем блокирующую операцию FTP в отдельном потоке
        await asyncio.to_thread(upload_to_ftp, temp_path, user_folder, file_name)
        
        os.remove(temp_path)
        await msg.edit_text(f"✅ Файл \"{file_name}\" успешно сохранен в папку {user_folder}!")
        
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка загрузки: {str(e)}")

# Обработка неподдерживаемых типов
@dp.message()
async def reject_other(message: Message):
    if not (message.photo or message.video or message.document):
        await message.answer("⚠️ Этот тип сообщений не поддерживается. Присылайте только фото или видео.")

# --- ЗАПУСК СЕРВЕРА ---

async def on_startup(bot: Bot):
    """Установка вебхука при старте"""
    webhook_url = f"{RENDER_URL}/webhook"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)

def main():
    try:
        # Порт для Render
        port = int(os.getenv("RENDER_PORT", 10000))
        app = web.Application()
        
        # Маршруты для браузера
        app.router.add_get("/", handle_index)
        app.router.add_get("/debug", handle_debug_page)
        
        # Настройка вебхука
        webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
        webhook_handler.register(app, path="/webhook")
        
        setup_application(app, dp, bot=bot)
        dp.startup.register(on_startup)
        
        # Запуск сервера
        web.run_app(app, host="0.0.0.0", port=port)
        
    except TokenValidationError:
        print("Ошибка: Неверный BOT_TOKEN!")
    except Exception as e:
        print(f"Критическая ошибка запуска: {e}")

if __name__ == "__main__":
    main()
