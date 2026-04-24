# -*- coding: utf-8 -*-
"""
Простой эхо-бот для Telegram
Повторяет все полученные сообщения
"""

import telebot
# Создаем бота
bot = telebot.TeleBot(API_TOKEN)


@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    """Обработчик команд /start и /help"""
    bot.reply_to(message, "Привет! Я эхо-бот. Отправь мне любое сообщение, и я его повторю!")


@bot.message_handler(func=lambda message: True)
def echo_all(message):
    """Обработчик всех сообщений - просто повторяет их с улыбкой"""
    # Повторяем текст сообщения с улыбкой
    if message.text:
        bot.send_message(message.chat.id, f"{message.text} 😊")
    # Если есть фото/видео/другие медиа - пересылаем их
    elif message.photo:
        bot.send_photo(message.chat.id, message.photo[-1].file_id)
    elif message.voice:
        bot.send_voice(message.chat.id, message.voice.file_id)
    elif message.video:
        bot.send_video(message.chat.id, message.video.file_id)
    elif message.sticker:
        bot.send_sticker(message.chat.id, message.sticker.file_id)
    else:
        # Для остальных типов сообщений - пересылаем как есть
        bot.forward_message(message.chat.id, message.chat.id, message.message_id)


if __name__ == '__main__':
    print("Эхо-бот запущен...")
    bot.infinity_polling()
