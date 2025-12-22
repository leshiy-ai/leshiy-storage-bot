import os
import asyncio
import sys      
import aiogram  
import logging
import io
from ftplib import FTP
from datetime import datetime

# Расширенные протоколы для работы с хранилищами
import paramiko
from webdav3.client import Client as WebDavClient

# Библиотеки для реализации веб-интерфейса и вебхуков
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# Основные компоненты aiogram
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.utils.token import TokenValidationError

# Детальная настройка логирования (важно для мониторинга в Render)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- НАСТРОЙКИ И КОНФИГУРАЦИЯ ---

BOT_TOKEN = os.getenv("BOT_TOKEN")
# FTP_HOST_RAW принимает: sftp://host, davs://host, ftp://host или просто IP
FTP_HOST_RAW = os.getenv("FTP_HOST", "") 
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FOLDER = os.getenv("FTP_FOLDER", "").strip()
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
VERSION = "1.4.5"

# НОВАЯ ПЕРЕМЕННАЯ ДЛЯ АДМИНА
try:
    admin_env = os.getenv("ADMIN_ID", "")
    ADMIN_ID = int(admin_env.strip()) if admin_env else None
except Exception as e:
    logger.error(f"Ошибка парсинга ADMIN_ID: {e}")
    ADMIN_ID = None
    
# Список разрешенных пользователей
try:
    ALLOWED_IDS = [int(i.strip()) for i in os.getenv("ALLOWED_IDS", "").split(",") if i.strip()]
except Exception as e:
    logger.error(f"Ошибка парсинга ALLOWED_IDS: {e}")
    ALLOWED_IDS = []

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_clean_host():
    """Очищает строку хоста от всех возможных префиксов протоколов"""
    return FTP_HOST_RAW.replace("ftp://", "").replace("sftp://", "").replace("dav://", "").replace("davs://", "")

# --- УНИВЕРСАЛЬНАЯ ЛОГИКА ЗАГРУЗКИ (SFTP / WebDAV / FTP) ---

def upload_file_universal(local_path, user_folder, file_name):
    """
    Функция-комбайн для работы с разными типами серверов.
    Тип подключения определяется автоматически.
    """
    host = get_clean_host()
    logger.info(f"Запуск процесса загрузки файла: {file_name}")
    
    # 1. Сценарий загрузки через SFTP (порт 22)
    if FTP_HOST_RAW.startswith("sftp://"):
        logger.info(f"Выбран протокол SFTP. Подключение к {host}")
        transport = paramiko.Transport((host, 22))
        transport.connect(username=FTP_USER, password=FTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Создание структуры каталогов
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

    # 2. Сценарий загрузки через WebDAV (Яндекс, Keenetic и др.)
    elif "dav" in FTP_HOST_RAW:
        logger.info(f"Выбран протокол WebDAV. Подключение к {FTP_HOST_RAW}")
        # Приведение dav:// к http:// для совместимости с библиотекой
        target_url = FTP_HOST_RAW.replace("dav://", "http://").replace("davs://", "https://")
        
        options = {
            'webdav_hostname': target_url,
            'webdav_login':    FTP_USER,
            'webdav_password': FTP_PASS
        }
        client = WebDavClient(options)
        
        # Построение и проверка путей в облаке
        base_path = ""
        if FTP_FOLDER:
            base_path = f"{FTP_FOLDER}/"
            if not client.check(base_path):
                client.mkdir(base_path)
        
        full_remote_path = f"{base_path}{user_folder}/"
        if not client.check(full_remote_path):
            client.mkdir(full_remote_path)
            
        client.upload_sync(remote_path=f"{full_remote_path}{file_name}", local_path=local_path)

    # 3. Сценарий загрузки через стандартный FTP (порт 21)
    else:
        logger.info(f"Выбран протокол FTP. Подключение к {host}")
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
    """Главная страница бота в браузере"""
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
    status_storage = "Проверка..."
    host = get_clean_host()
    
    try:
        if "dav" in FTP_HOST_RAW:
            status_storage = "WebDAV Mode ✅"
        elif "sftp" in FTP_HOST_RAW:
            status_storage = "SFTP Mode ✅"
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
    """Обработка команды /start"""
    await message.answer(
        "👋 Привет! Я твоя личная FTP/SFTP/WebDAV хранилка.\n\n"
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
    # Засекаем время начала обработки для определения "холодного старта"
    start_time = datetime.now()
    
    """Главный обработчик входящего медиаконтента"""
    
    # ПРОВЕРКА ДОСТУПА С УВЕДОМЛЕНИЕМ АДМИНА
    if message.from_user.id not in ALLOWED_IDS:
        await message.answer("🚫 У вас нет прав на сохранение файлов.")
        
        # Отправляем отчет тебе, если ADMIN_ID настроен
        if ADMIN_ID:
            user_name = message.from_user.full_name
            user_folder_name = user_name.replace(" ", "_")
            alert_text = (
                f"🚨 <b>Попытка несанкционированного доступа!</b>\n\n"
                f"👤 <b>Пользователь:</b> {user_name}\n"
                f"🆔 <b>ID:</b> <code>{message.from_user.id}</code>\n"
                f"🌐 <b>Username:</b> @{message.from_user.username}\n"
                f"📂 <b>Предполагаемая папка:</b> <code>{user_folder_name}</code>"
            )
            await bot.send_message(ADMIN_ID, alert_text, parse_mode="HTML")
        return
        
    # Отправляем статус "загрузки", чтобы пользователь видел активность в заголовке чата
    await bot.send_chat_action(message.chat.id, action="upload_document")

    file_id, file_name = None, None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_id, file_name = None, None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Логика определения типа медиа
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

    status_msg = await message.answer("⏳ Загружаю на сервер...")
    # Проверяем, долго ли бот "просыпался"
    process_delay = (datetime.now() - start_time).total_seconds()
    wake_up_note = " 💤 (Проснулся после спячки)" if process_delay > 3 else ""
    msg = await message.answer(f"⏳ Начинаю загрузку...{wake_up_note}")
    
    try:
        # Скачивание файла в локальную временную папку Render
        file_info = await bot.get_file(file_id)
        temp_path = f"temp_{file_name}"
        await bot.download_file(file_info.file_path, temp_path)
        
        # Формирование имени папки на сервере
        user_folder = message.from_user.full_name.replace(" ", "_")
        
        # Загрузка на удаленное хранилище
        await asyncio.to_thread(upload_file_universal, temp_path, user_folder, file_name)
        
        # Очистка временных данных
        if os.path.exists(temp_path):
            os.remove(temp_path)
            
        await status_msg.edit_text(f"✅ Файл \"{file_name}\" успешно сохранен!")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке файла: {e}")
        await status_msg.edit_text(f"❌ Критическая ошибка: {e}")

@dp.message()
async def reject_other_content(message: Message):
    """Отклонение текстовых сообщений и прочего контента"""
    await message.answer("⚠️ Пожалуйста, присылайте только фото или видео.")

# --- СИСТЕМНЫЕ ФУНКЦИИ ЗАПУСКА ---

async def on_startup(bot: Bot):
    """Действия при запуске: установка вебхука"""
    logger.info(f"Установка Webhook: {RENDER_URL}/webhook")
    await bot.set_webhook(f"{RENDER_URL}/webhook", drop_pending_updates=True)

def main():
    """Точка входа в приложение"""
    try:
        # Получение порта из переменных окружения Render
        port_env = os.getenv("RENDER_PORT", "10000")
        port = int(port_env)
        
        app = web.Application()
        
        # Настройка маршрутов веб-сервера
        app.router.add_get("/", handle_index)
        app.router.add_get("/debug", handle_debug_page)
        
        # Регистрация обработчика вебхуков
        webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
        webhook_handler.register(app, path="/webhook")
        
        setup_application(app, dp, bot=bot)
        dp.startup.register(on_startup)
        
        logger.info(f"Запуск веб-сервера на порту {port}")
        web.run_app(app, host="0.0.0.0", port=port)
        
    except TokenValidationError:
        logger.error("Ошибка: Токен бота невалиден. Проверьте BOT_TOKEN.")
    except Exception as e:
        logger.critical(f"Непредвиденная ошибка при запуске: {e}")

if __name__ == "__main__":
    main()
