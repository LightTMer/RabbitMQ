import sys
import os
import pika
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

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

def main():
    if len(sys.argv) != 2:
        print("Использование: python producer.py <URL>")
        sys.exit(1)

    url = sys.argv[1]
    
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    channel.queue_declare(queue='links')

    internal_links = get_internal_links(url)

    for link in internal_links:
        channel.basic_publish(exchange='', routing_key='links', body=link)
        print(f'Отправлено в очередь: {link}')

    connection.close()

if __name__ == "__main__":
    main()
