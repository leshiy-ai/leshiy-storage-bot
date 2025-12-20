import os
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message
from ftplib import FTP

# Загрузка настроек из секретов (Environment Variables)
TOKEN = os.getenv("BOT_TOKEN")
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
ALLOWED_IDS = os.getenv("ALLOWED_IDS", "").split(",") # Список ID через запятую

bot = Bot(token=TOKEN)
dp = Dispatcher()

def upload_to_ftp(file_path, folder_name, file_name):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        # Проверка и создание папки пользователя
        if folder_name not in ftp.nlst():
            ftp.mkd(folder_name)
        ftp.cwd(folder_name)
        # Загрузка файла
        with open(file_path, 'rb') as f:
            ftp.storbinary(f'STOR {file_name}', f)

@dp.message(F.photo | F.video | F.document)
async def handle_files(message: Message):
    # Проверка доступа (только для своих)
    if str(message.from_user.id) not in ALLOWED_IDS:
        return await message.answer("У вас нет доступа к этой хранилке 🛑")

    # Определяем имя папки (Имя_Фамилия или Username)
    user_folder = f"{message.from_user.first_name}_{message.from_user.last_name or ''}".strip()
    
    # Получаем файл (берем самое лучшее качество)
    file_id = message.document.file_id if message.document else (message.video.file_id if message.video else message.photo[-1].file_id)
    file = await bot.get_file(file_id)
    file_name = file.file_path.split("/")[-1]
    
    # Скачиваем временно на сервер бота
    local_path = f"temp_{file_name}"
    await bot.download_file(file.file_path, local_path)

    # Отправляем на FTP
    try:
        upload_to_ftp(local_path, user_folder, file_name)
        await message.answer(f"✅ Сохранено в папку: {user_folder}")
    except Exception as e:
        await message.answer(f"❌ Ошибка FTP: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path) # Удаляем временный файл

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
