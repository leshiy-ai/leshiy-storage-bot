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
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
FTP_HOST_RAW = os.getenv("FTP_HOST", "") 
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_FOLDER = os.getenv("FTP_FOLDER", "").strip()
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
VERSION = "1.5.1"

# Имя файла базы данных пользователей на твоем сервере
DB_FILE = "allowed_ids.txt"

# Глобальный список разрешенных ID в памяти бота
ALLOWED_IDS = []

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_clean_host():
    """Очищает хост от префиксов протоколов"""
    return FTP_HOST_RAW.replace("ftp://", "").replace("sftp://", "").replace("dav://", "").replace("davs://", "")

def get_webdav_client():
    """Создает клиент для работы с WebDAV"""
    target_url = FTP_HOST_RAW.replace("dav://", "http://").replace("davs://", "https://")
    return WebDavClient({
        'webdav_hostname': target_url,
        'webdav_login':    FTP_USER,
        'webdav_password': FTP_PASS
    })

# --- ЛОГИКА СИНХРОНИЗАЦИИ СПИСКА ДОСТУПА ЧЕРЕЗ ХРАНИЛИЩЕ ---

async def sync_db_from_storage():
    """Подтягивает список разрешенных ID из твоего облака при запуске бота"""
    global ALLOWED_IDS
    host = get_clean_host()
    local_path = DB_FILE
    
    try:
        if "dav" in FTP_HOST_RAW:
            client = get_webdav_client()
            remote_path = f"{FTP_FOLDER}/{DB_FILE}" if FTP_FOLDER else DB_FILE
            if client.check(remote_path):
                client.download_sync(remote_path=remote_path, local_path=local_path)
        else:
            with FTP() as ftp:
                ftp.connect(host, 21, timeout=10)
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                if FTP_FOLDER: ftp.cwd(FTP_FOLDER)
                if DB_FILE in ftp.nlst():
                    with open(local_path, "wb") as f:
                        ftp.retrbinary(f"RETR {DB_FILE}", f.write)

        if os.path.exists(local_path):
            with open(local_path, "r") as f:
                content = f.read().strip()
                if content:
                    # Убираем дубликаты и пустые значения
                    ALLOWED_IDS = list(set([int(i) for i in content.split(",") if i.strip()]))
            logger.info(f"✅ Список доступа синхронизирован: {ALLOWED_IDS}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки базы ID: {e}")

async def save_id_to_storage(new_id):
    """Добавляет новый ID в список и заливает обновленный файл обратно в облако"""
    global ALLOWED_IDS
    if new_id in ALLOWED_IDS:
        return False
    
    ALLOWED_IDS.append(new_id)
    content = ",".join(map(str, ALLOWED_IDS))
    local_path = DB_FILE
    host = get_clean_host()
    
    try:
        with open(local_path, "w") as f:
            f.write(content)
            
        if "dav" in FTP_HOST_RAW:
            client = get_webdav_client()
            remote_path = f"{FTP_FOLDER}/{DB_FILE}" if FTP_FOLDER else DB_FILE
            client.upload_sync(remote_path=remote_path, local_path=local_path)
        else:
            with FTP() as ftp:
                ftp.connect(host, 21, timeout=10)
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                if FTP_FOLDER:
                    if FTP_FOLDER not in ftp.nlst(): ftp.mkd(FTP_FOLDER)
                    ftp.cwd(FTP_FOLDER)
                with open(local_path, "rb") as f:
                    ftp.storbinary(f"STOR {DB_FILE}", f)
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка записи базы ID в облако: {e}")
        return False

# --- УНИВЕРСАЛЬНАЯ ЛОГИКА ЗАГРУЗКИ МЕДИА ---

def upload_file_universal(local_path, user_folder, file_name):
    host = get_clean_host()
    
    if FTP_HOST_RAW.startswith("sftp://"):
        transport = paramiko.Transport((host, 22))
        transport.connect(username=FTP_USER, password=FTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        if FTP_FOLDER:
            try: sftp.chdir(FTP_FOLDER)
            except IOError: sftp.mkdir(FTP_FOLDER); sftp.chdir(FTP_FOLDER)
        try: sftp.chdir(user_folder)
        except IOError: sftp.mkdir(user_folder); sftp.chdir(user_folder)
        sftp.put(local_path, file_name)
        sftp.close(); transport.close()
        
    elif "dav" in FTP_HOST_RAW:
        client = get_webdav_client()
        base = f"{FTP_FOLDER}/" if FTP_FOLDER else ""
        if base and not client.check(base): client.mkdir(base)
        path = f"{base}{user_folder}/"
        if not client.check(path): client.mkdir(path)
        client.upload_sync(remote_path=f"{path}{file_name}", local_path=local_path)
        
    else:
        with FTP() as ftp:
            ftp.connect(host, 21, timeout=30); ftp.login(user=FTP_USER, passwd=FTP_PASS); ftp.set_pasv(True)
            if FTP_FOLDER:
                if FTP_FOLDER not in ftp.nlst(): ftp.mkd(FTP_FOLDER)
                ftp.cwd(FTP_FOLDER)
            if user_folder not in ftp.nlst(): ftp.mkd(user_folder)
            ftp.cwd(user_folder)
            with open(local_path, 'rb') as f: ftp.storbinary(f'STOR {file_name}', f)

# --- ВЕБ-ИНТЕРФЕЙС ---

async def handle_index(request):
    html = f"""
    <html>
        <head><title>Хранилка by Leshiy</title></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f4f4f9;">
            <h1 style="color: #333;">🚀 Телеграм-бот "Хранилка" by Leshiy v{VERSION}</h1>
            <p>Статус: <span style="color: green; font-weight: bold;">ONLINE ✅</span></p>
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

# --- ОБРАБОТЧИКИ КОМАНД ---

@dp.callback_query(F.data.startswith("adm_allow_"))
async def callback_allow_user(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    target_id = int(callback.data.split("_")[2])
    
    if await save_id_to_storage(target_id):
        await callback.message.edit_text(f"{callback.message.text}\n\n✅ <b>Доступ успешно предоставлен!</b>", parse_mode="HTML")
        await bot.send_message(target_id, "🎉 Администратор предоставил вам доступ к хранилищу!")
    else:
        await callback.answer("Ошибка или пользователь уже есть в списке.")

@dp.message(Command("start"))
async def cmd_start(message: Message):
    # ВОЗВРАЩЕНО ТВОЁ ПОЛНОЕ ПРИВЕТСТВИЕ
    await message.answer(
        "👋 Привет! Я твоя личная хранилка.\n"
        "📁 Просто пришли мне фото или видео, и я закину их на сервер.\n"
        "⚙️ Используй /debug чтобы проверить статус подключения."
    )

@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    host = get_clean_host()
    def check():
        try:
            if "dav" in FTP_HOST_RAW: return "✅ WebDAV соединение установлено"
            if "sftp" in FTP_HOST_RAW: return "✅ SFTP соединение установлено"
            with FTP() as ftp:
                ftp.connect(host, 21, timeout=10); ftp.login(user=FTP_USER, passwd=FTP_PASS)
                return "✅ FTP Соединение установлено"
        except Exception as e: return f"❌ Ошибка: {e}"
    
    status = await asyncio.to_thread(check)
    # ВОЗВРАЩЕН ТВОЙ ПОЛНЫЙ ФОРМАТ ОТВЕТА
    await message.answer(
        f"🤖 <b>Бот онлайн</b>\n"
        f"📦 <b>Версия:</b> {VERSION}\n"
        f"🔗 <b>Статус:</b> {status}\n"
        f"👤 <b>Твой ID:</b> <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    # Принудительно добавляем админа, если его нет в списке
    if ADMIN_ID not in ALLOWED_IDS:
        await save_id_to_storage(ADMIN_ID)

    if message.from_user.id not in ALLOWED_IDS:
        await message.answer("🚫 У вас нет прав на сохранение файлов. Запрос отправлен администратору.")
        if ADMIN_ID:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Разрешить доступ", callback_data=f"adm_allow_{message.from_user.id}")
            ]])
            alert = (
                f"🚨 <b>Попытка несанкционированного доступа!</b>\n\n"
                f"👤 <b>Пользователь:</b> {message.from_user.full_name}\n"
                f"🆔 <b>ID:</b> <code>{message.from_user.id}</code>\n"
                f"🌐 <b>Username:</b> @{message.from_user.username}\n"
                f"📂 <b>Предполагаемая папка:</b> <code>{message.from_user.full_name.replace(' ', '_')}</code>"
            )
            await bot.send_message(ADMIN_ID, alert, parse_mode="HTML", reply_markup=kb)
        return

    # ЛОГИКА "ПРОБУЖДЕНИЯ" И ЗАГРУЗКИ
    start_t = datetime.now()
    await bot.send_chat_action(message.chat.id, action="upload_document")
    
    file_id, file_name = None, None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if message.photo: file_id, file_name = message.photo[-1].file_id, f"photo_{ts}.jpg"
    elif message.video: file_id, file_name = message.video.file_id, f"video_{ts}.mp4"
    elif message.document:
        if message.document.mime_type.startswith(('image/', 'video/')):
            file_id, file_name = message.document.file_id, message.document.file_name

    if not file_id: return
    
    delay = (datetime.now() - start_t).total_seconds()
    wake_note = " 💤 (Проснулся после спячки)" if delay > 2.5 else ""
    status_msg = await message.answer(f"⏳ Начинаю загрузку...{wake_note}")

    try:
        f_info = await bot.get_file(file_id)
        temp_path = f"temp_{file_name}"
        await bot.download_file(f_info.file_path, temp_path)
        
        user_folder = message.from_user.full_name.replace(" ", "_")
        await asyncio.to_thread(upload_file_universal, temp_path, user_folder, file_name)
        
        if os.path.exists(temp_path): os.remove(temp_path)
        await status_msg.edit_text(f"✅ Файл \"{file_name}\" успешно сохранен!{wake_note}")
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка при сохранении: {e}")

@dp.message()
async def reject_other(message: Message):
    await message.answer("⚠️ Пожалуйста, присылайте только фото или видео.")

# --- ЗАПУСК СИСТЕМЫ ---

async def on_startup(bot: Bot):
    await sync_db_from_storage() # Подгружаем базу ID из облака
    await bot.set_webhook(f"{RENDER_URL}/webhook", drop_pending_updates=True)

def main():
    try:
        port = int(os.getenv("RENDER_PORT", 10000))
        app = web.Application()
        app.router.add_get("/", handle_index)
        app.router.add_get("/debug", handle_debug_page)
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
        setup_application(app, dp, bot=bot)
        dp.startup.register(on_startup)
        web.run_app(app, host="0.0.0.0", port=port)
    except Exception as e: logger.critical(f"Ошибка старта: {e}")

if __name__ == "__main__":
    main()
