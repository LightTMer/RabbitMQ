import os
import pika
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import signal

stop_consuming = False

def signal_handler():
    global stop_consuming
    stop_consuming = True

signal.signal(signal.SIGINT, signal_handler)

def get_internal_links(url):
    internal_links = set()
    domain = urlparse(url).netloc

    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        for tag in soup.find_all(['a', 'img', 'video', 'audio']):
            if tag.name == 'a':
                link = tag.get('href')
                if link:
                    full_url = urljoin(url, link)
                    if urlparse(full_url).netloc == domain:
                        internal_links.add(full_url)
                        print(f'Найдена ссылка: {full_url} (тег: {tag.name})')
            else:
                media_url = tag.get('src')
                if media_url:
                    full_media_url = urljoin(url, media_url)
                    if urlparse(full_media_url).netloc == domain:
                        internal_links.add(full_media_url)
                        print(f'Найдено медиа: {full_media_url} (тип: {tag.name})')

    except Exception as e:
        print(f'Ошибка при обработке URL {url}: {e}')
    
    return internal_links

def callback(ch, method, properties, body):
    url = body.decode()
    print(f'Обрабатывается: {url}')
    get_internal_links(url)
    

def main():
   
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    channel.queue_declare(queue='links')

    print('Ожидание сообщений. Нажмите Ctrl+C для выхода.')

    try:
        while not stop_consuming:
            method_frame, header_frame, body = channel.basic_get(queue='links', auto_ack=True)
            if body:
                callback(channel, method_frame, header_frame, body)
            else:
                time.sleep(1)  
    except pika.exceptions.AMQPConnectionError:
        print("Проблема с подключением к RabbitMQ. Повторное подключение...")
        time.sleep(5)
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        print("Закрытие соединения...")
        connection.close()


if __name__ == "__main__":
    main()

#http://localhost:15672
#https://ru.wikipedia.org/wiki/Коэффициент_детерминации