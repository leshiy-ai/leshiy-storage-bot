import os
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message
from aiogram.filters import Command
from ftplib import FTP
from aiohttp import web

# --- НАСТРОЙКИ ---
VERSION = "1.1.0"
TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
ALLOWED_IDS = os.getenv("ALLOWED_IDS", "").split(",")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- МИНИ ВЕБ-СЕРВЕР (чтобы Render не спал) ---
async def handle_http(request):
    return web.Response(text=f"Хранилка by Leshiy is running. Version: {VERSION}")

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", handle_http)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 10000)
    await site.start()

# --- ЛОГИКА FTP ---
def upload_to_ftp(file_path, folder_name, file_name):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        # Проверяем наличие папки
        items = ftp.nlst()
        if folder_name not in items:
            ftp.mkd(folder_name)
        ftp.cwd(folder_name)
        with open(file_path, 'rb') as f:
            ftp.storbinary(f'STOR {file_name}', f)

# --- КОМАНДЫ ---
@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    status_ftp = "Доступен"
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
    
    # Сбор данных файла
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
        upload_to_ftp(local_path, user_folder, file_name)
        await wait_msg.edit_text(f"✅ Сохранено в папку: {user_folder}")
    except Exception as e:
        await wait_msg.edit_text(f"❌ Ошибка сохранения: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

# --- ЗАПУСК ---
async def main():
    # Получаем порт из переменной окружения Render или используем 10000
    port = int(os.getenv("PORT", 10000))
    
    # Запускаем веб-сервер
    app = web.Application()
    app.router.add_get("/", handle_http)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    print(f"Starting webserver on port {port}...")
    await site.start()
    
    print("Starting bot polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
