import os
import asyncio
import sys      
import aiogram  
import logging
import io
from ftplib import FTP
from datetime import datetime

# Расширенные протоколы передачи данных
import paramiko
from webdav3.client import Client as WebDavClient

# Веб-сервер и работа с вебхуками
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# Основные компоненты aiogram
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.utils.token import TokenValidationError

# Настройка логирования (важно для отладки на Render)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- КОНФИГУРАЦИЯ И ПЕРЕМЕННЫЕ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
# FTP_HOST может быть: 1.2.3.4, ftp://host, sftp://host, davs://host
FTP_HOST_RAW = os.getenv("FTP_HOST", "") 
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FOLDER = os.getenv("FTP_FOLDER", "").strip()
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
VERSION = "1.4.4"

# Список разрешенных пользователей Telegram ID
try:
    ALLOWED_IDS = [int(i.strip()) for i in os.getenv("ALLOWED_IDS", "").split(",") if i.strip()]
except Exception as e:
    logger.error(f"Ошибка парсинга ALLOWED_IDS: {e}")
    ALLOWED_IDS = []

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- Вспомогательная функция очистки хоста ---
def get_clean_host():
    return FTP_HOST_RAW.replace("ftp://", "").replace("sftp://", "").replace("dav://", "").replace("davs://", "")

# --- УНИВЕРСАЛЬНАЯ ЛОГИКА ЗАГРУЗКИ (FTP / SFTP / WebDAV) ---

def upload_file_universal(local_path, user_folder, file_name):
    """
    Выбирает нужный протокол на основе префикса в FTP_HOST и выполняет загрузку.
    """
    host = get_clean_host()
    
    # 1. Работа через SFTP (SSH File Transfer Protocol)
    if FTP_HOST_RAW.startswith("sftp://"):
        logger.info(f"Использую SFTP для {file_name}")
        transport = paramiko.Transport((host, 22))
        transport.connect(username=FTP_USER, password=FTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Проверка и создание структуры папок
        if FTP_FOLDER:
            try:
                sftp.chdir(FTP_FOLDER)
            except IOError:
                sftp.mkdir(FTP_FOLDER)
                sftp.chdir(FTP_FOLDER)
        
        try:
            sftp.chdir(user_folder)
        except IOError:
            sftp.mkdir(user_folder)
            sftp.chdir(user_folder)
            
        sftp.put(local_path, file_name)
        sftp.close()
        transport.close()

    # 2. Работа через WebDAV (Облачные хранилища)
    elif "dav" in FTP_HOST_RAW:
        logger.info(f"Использую WebDAV для {file_name}")
        target_url = FTP_HOST_RAW.replace("dav://", "http://").replace("davs://", "https://")
        options = {
            'webdav_hostname': target_url,
            'webdav_login':    FTP_USER,
            'webdav_password': FTP_PASS
        }
        client = WebDavClient(options)
        
        # Построение пути в облаке
        base_path = ""
        if FTP_FOLDER:
            base_path = f"{FTP_FOLDER}/"
            if not client.check(base_path):
                client.mkdir(base_path)
        
        full_remote_path = f"{base_path}{user_folder}/"
        if not client.check(full_remote_path):
            client.mkdir(full_remote_path)
            
        client.upload_sync(remote_path=f"{full_remote_path}{file_name}", local_path=local_path)

    # 3. Работа через классический FTP
    else:
        logger.info(f"Использую стандартный FTP для {file_name}")
        with FTP() as ftp:
            ftp.connect(host, 21, timeout=30)
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.set_pasv(True)
            
            if FTP_FOLDER:
                if FTP_FOLDER not in ftp.nlst():
                    ftp.mkd(FTP_FOLDER)
                ftp.cwd(FTP_FOLDER)
                
            if user_folder not in ftp.nlst():
                ftp.mkd(user_folder)
            ftp.cwd(user_folder)
            
            with open(local_path, 'rb') as f:
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
    status_storage = "Checking..."
    host = get_clean_host()
    try:
        if "dav" in FTP_HOST_RAW:
            status_storage = "WebDAV Mode Active ✅"
        elif "sftp" in FTP_HOST_RAW:
            status_storage = "SFTP Mode Active ✅"
        else:
            with FTP() as ftp:
                ftp.connect(host, 21, timeout=5)
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                status_storage = "Connected ✅"
    except Exception as e:
        status_storage = f"Disconnected ❌ ({type(e).__name__})"
    
    html = f"""
    <html>
        <head><title>System Status</title></head>
        <body style="font-family: monospace; padding: 20px; background-color: #1e1e1e; color: #d4d4d4;">
            <h2 style="color: #569cd6;">🖥 System Diagnostics</h2>
            <p><b>Storage Status:</b> {status_storage}</p>
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
        "👋 Привет! Я твоя личная FTP/SFTP/WebDAV хранилка.\n"
        "📁 Просто пришли мне любой файл, фото или видео, и я закину их на сервер.\n"
        "⚙️ Используй /debug чтобы проверить статус подключения."
    )

@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    """Команда для проверки связи из чата"""
    host = get_clean_host()
    def check_connection():
        try:
            if "dav" in FTP_HOST_RAW: return "✅ WebDAV Ready"
            if "sftp" in FTP_HOST_RAW: return "✅ SFTP Ready"
            with FTP() as ftp:
                ftp.connect(host, 21, timeout=10)
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                return "✅ FTP Соединение установлено"
        except Exception as e:
            return f"❌ Ошибка: {e}"
    
    status = await asyncio.to_thread(check_connection)
    await message.answer(
        f"🤖 <b>Бот онлайн</b>\n"
        f"📦 <b>Версия:</b> {VERSION}\n"
        f"🔗 <b>Статус:</b> {status}\n"
        f"👤 <b>Твой ID:</b> <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

# --- ОСНОВНАЯ ЛОГИКА ОБРАБОТКИ ФАЙЛОВ ---

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    if message.from_user.id not in ALLOWED_IDS:
        await message.answer("🚫 У вас нет прав на сохранение файлов.")
        return

    file_id, file_name = None, None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Определение типа файла
    if message.photo:
        file_id = message.photo[-1].file_id
        file_name = f"photo_{timestamp}.jpg"
    elif message.video:
        file_id = message.video.file_id
        file_name = f"video_{timestamp}.mp4"
    elif message.document:
        m = message.document.mime_type
        # Разрешаем все изображения и видео (включая webm)
        if m and (m.startswith('image/') or m.startswith('video/')):
            file_id = message.document.file_id
            file_name = message.document.file_name
        else:
            await message.answer("⚠️ Бот принимает только фото и видео контент.")
            return

    if not file_id:
        return

    msg = await message.answer("⏳ Загружаю на сервер...")
    
    try:
        # Скачивание файла во временное хранилище Render
        file_info = await bot.get_file(file_id)
        temp_path = f"temp_{file_name}"
        await bot.download_file(file_info.file_path, temp_path)
        
        # Формирование имени папки пользователя
        user_folder = message.from_user.full_name.replace(" ", "_")
        
        # Выполнение загрузки в отдельном потоке, чтобы не блокировать бота
        await asyncio.to_thread(upload_file_universal, temp_path, user_folder, file_name)
        
        # Удаление временного файла
        if os.path.exists(temp_path):
            os.remove(temp_path)
            
        await msg.edit_text(f"✅ Файл \"{file_name}\" успешно сохранен в папку {user_folder}!")
        
    except Exception as e:
        logger.error(f"Критическая ошибка загрузки: {e}")
        await msg.edit_text(f"❌ Ошибка при сохранении: {e}")

@dp.message()
async def reject_other_content(message: Message):
    await message.answer("⚠️ Пожалуйста, присылайте только фото или видео.")

# --- СИСТЕМНЫЕ ФУНКЦИИ ЗАПУСКА ---

async def on_startup(bot: Bot):
    logger.info(f"Установка Webhook на адрес: {RENDER_URL}/webhook")
    await bot.set_webhook(f"{RENDER_URL}/webhook", drop_pending_updates=True)

def main():
    try:
        # Твоя переменная порта для Render
        port = int(os.getenv("RENDER_PORT", 10000))
        
        app = web.Application()
        
        # Маршруты для веб-интерфейса
        app.router.add_get("/", handle_index)
        app.router.add_get("/debug", handle_debug_page)
        
        # Настройка обработчика входящих вебхуков от Telegram
        webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
        webhook_handler.register(app, path="/webhook")
        
        setup_application(app, dp, bot=bot)
        dp.startup.register(on_startup)
        
        logger.info(f"Запуск сервера на порту {port}...")
        web.run_app(app, host="0.0.0.0", port=port)
        
    except TokenValidationError:
        logger.error("Ошибка: Токен BOT_TOKEN указан неверно.")
    except Exception as e:
        logger.critical(f"Бот упал при запуске: {e}")

if __name__ == "__main__":
    main()
