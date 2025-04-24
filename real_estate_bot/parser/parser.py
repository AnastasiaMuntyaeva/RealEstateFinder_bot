# -*- coding: utf-8 -*-
import sys
import locale
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import time
import random
import logging
from webdriver_manager.chrome import ChromeDriverManager

# Настройка кодировки
locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
sys.stdout.reconfigure(encoding='utf-8')

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных из .env
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5433"),
    "client_encoding": "utf8"
}


def setup_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ru-RU")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    # Для отладки (раскомментируйте для визуального контроля)
    # options.add_argument("--headless")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Изменяем свойства navigator.webdriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver


def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        logger.info("Успешное подключение к БД")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {str(e)}")
        raise


def save_rental(conn, address, price, rooms, area, link):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                INSERT INTO rental (address, price, rooms, area, link) 
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (address) DO NOTHING
            """)
            cur.execute(query, (
                address.encode('utf-8').decode('utf-8') if address else None,
                price.encode('utf-8').decode('utf-8') if price else None,
                rooms.encode('utf-8').decode('utf-8') if rooms else None,
                area.encode('utf-8').decode('utf-8') if area else None,
                link.encode('utf-8').decode('utf-8') if link else None
            ))
            conn.commit()
            logger.info(f"Сохранено в аренду: {address}, {price}, {link}")
    except Exception as e:
        logger.error(f"Ошибка сохранения аренды: {str(e)}")
        conn.rollback()


def solve_captcha_manually(driver):
    logger.warning("Обнаружена капча. Решите её вручную в браузере...")
    input("После решения капчи нажмите Enter в консоли...")
    # После решения капчи обновляем страницу
    driver.refresh()
    time.sleep(5)
    return True


def parse_avito_rent():
    driver = None
    conn = None
    try:
        driver = setup_driver()
        url = "https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok-ASgBAgICAkSSA8gQ8AeQUg"

        logger.info("Открываем страницу...")
        driver.get(url)
        time.sleep(random.uniform(5, 10))

        # Проверка на капчу
        if any(word in driver.page_source.lower() for word in ["captcha", "капча"]):
            if not solve_captcha_manually(driver):
                return

        # Прокрутка страницы для имитации поведения пользователя
        for i in range(3):
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            scroll_point = scroll_height * (i + 1) / 4
            driver.execute_script(f"window.scrollTo(0, {scroll_point});")
            time.sleep(random.uniform(2, 4))

        # Ожидание загрузки объявлений (новый селектор)
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '[data-marker="item"], div[itemprop="itemListElement"]')))
        except:
            logger.warning("Не удалось найти объявления на странице")
            return

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Альтернативные селекторы для объявлений
        items = soup.find_all('div', {'data-marker': 'item'})
        if not items:
            items = soup.find_all('div', itemprop="itemListElement")

        if not items:
            logger.warning("Не найдено объявлений на странице")
            # Сохраняем скриншот для отладки
            driver.save_screenshot("debug_screenshot.png")
            logger.info("Скриншот страницы сохранен как debug_screenshot.png")
            return

        conn = connect_db()
        parsed_count = 0

        for item in items[:50]:
            try:
                # Извлечение ссылки на объявление
                link_elem = item.find('a', {'data-marker': 'item-title'}) or \
                            item.find('a', {'itemprop': 'url'})
                link = "https://www.avito.ru" + link_elem['href'] if link_elem and link_elem.has_attr('href') else None

                # Основные данные с альтернативными селекторами
                title_elem = (item.find('h3', {'itemprop': 'name'}) or
                              item.find('h3', class_='title-root') or
                              item.find('a', {'data-marker': 'item-title'}))

                price_elem = (item.find('meta', {'itemprop': 'price'}) or
                              item.find('span', {'data-marker': 'item-price'}) or
                              item.find('span', {'itemprop': 'price'}))

                address_elem = (item.find('div', {'data-marker': 'item-address'}) or
                                item.find('span', class_='geo-address') or
                                item.find('div', class_='geo-root'))

                if not all([title_elem, price_elem, address_elem]):
                    logger.warning("Не все обязательные элементы найдены в объявлении")
                    continue

                title = title_elem.get_text(strip=True)

                # Обработка цены
                if price_elem.name == 'meta':
                    price = f"{price_elem['content']} ₽" if price_elem.has_attr('content') else "0 ₽"
                else:
                    price = price_elem.get_text(strip=True)
                    if '₽' not in price:
                        price += " ₽"

                address = address_elem.get_text(strip=True)

                # Основные данные
                title_elem = (item.find('h3', {'itemprop': 'name'}) or
                              item.find('h3', class_='title-root') or
                              item.find('a', {'data-marker': 'item-title'}))
                title = title_elem.get_text(strip=True) if title_elem else "N/A"

                # Обработка комнат и площади из заголовка
                try:
                    title_parts = [part.strip() for part in title.split(', ')]
                    rooms = title_parts[0]  # Первая часть - комнаты
                    area = next((part for part in title_parts if 'м²' in part), "N/A")

                    # Очистка площади (если нужно только число)
                    area_value = area.replace(' м²', '').strip() if area != "N/A" else "N/A"
                except Exception as e:
                    logger.error(f"Ошибка обработки заголовка: {str(e)}")
                    rooms = "N/A"
                    area_value = "N/A"

                logger.info(
                    f"Найдено: {title} | {price} | {address} | Комнаты: {rooms} | Площадь: {area_value} | Ссылка: {link}")
                save_rental(conn, address, price, rooms, area_value, link)
                parsed_count += 1

            except Exception as e:
                logger.error(f"Ошибка обработки объявления: {str(e)}", exc_info=True)
                continue

        logger.info(f"Успешно обработано объявлений: {parsed_count}/{len(items[:5])}")

    except Exception as e:
        logger.error(f"Ошибка при парсинге: {str(e)}", exc_info=True)
    finally:
        try:
            if driver:
                driver.quit()
                logger.info("Драйвер успешно закрыт")
        except Exception as e:
            logger.error(f"Ошибка при закрытии драйвера: {str(e)}")

        try:
            if conn:
                conn.close()
                logger.info("Соединение с БД успешно закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с БД: {str(e)}")


def save_sale(conn, address, property_type, price, rooms, area, link):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                INSERT INTO sale (address, property_type, price, rooms, area, link) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (address) DO NOTHING
            """)
            cur.execute(query, (
                address.encode('utf-8').decode('utf-8') if address else None,
                property_type.encode('utf-8').decode('utf-8') if property_type else None,
                price.encode('utf-8').decode('utf-8') if price else None,
                rooms.encode('utf-8').decode('utf-8') if rooms else None,
                area.encode('utf-8').decode('utf-8') if area else None,
                link.encode('utf-8').decode('utf-8') if link else None,

            ))
            conn.commit()
            logger.info(f"Сохранено в продажу: {address}, {price}, {property_type}, {link}")
    except Exception as e:
        logger.error(f"Ошибка сохранения продажи: {str(e)}")
        conn.rollback()


def parse_avito_sale():
    driver = None
    conn = None
    try:
        driver = setup_driver()
        url = "https://www.avito.ru/sankt-peterburg/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_wEtANL_YToxOntzOjg6ImZyb21QYWdlIjtzOjE2OiJzZWFyY2hGb3JtV2lkZ2V0Ijt9F_yIfi0AAAA"

        logger.info("Открываем страницу продаж...")
        driver.get(url)
        time.sleep(random.uniform(5, 10))

        # Проверка на капчу
        if any(word in driver.page_source.lower() for word in ["captcha", "капча"]):
            if not solve_captcha_manually(driver):
                return

        # Прокрутка страницы
        for i in range(3):
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            scroll_point = scroll_height * (i + 1) / 4
            driver.execute_script(f"window.scrollTo(0, {scroll_point});")
            time.sleep(random.uniform(2, 4))

        # Ожидание загрузки объявлений
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '[data-marker="item"], div[itemprop="itemListElement"]')))
        except:
            logger.warning("Не удалось найти объявления на странице")
            return

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        items = soup.find_all('div', {'data-marker': 'item'})
        if not items:
            items = soup.find_all('div', itemprop="itemListElement")

        if not items:
            logger.warning("Не найдено объявлений на странице")
            driver.save_screenshot("debug_sale_screenshot.png")
            logger.info("Скриншот страницы сохранен как debug_sale_screenshot.png")
            return

        conn = connect_db()
        parsed_count = 0

        for item in items[:50]:
            try:
                # Извлечение ссылки на объявление
                link_elem = item.find('a', {'data-marker': 'item-title'}) or \
                            item.find('a', {'itemprop': 'url'})
                link = "https://www.avito.ru" + link_elem['href'] if link_elem and link_elem.has_attr('href') else None

                title_elem = (item.find('h3', {'itemprop': 'name'}) or
                              item.find('h3', class_='title-root') or
                              item.find('a', {'data-marker': 'item-title'}))

                price_elem = (item.find('meta', {'itemprop': 'price'}) or
                              item.find('span', {'data-marker': 'item-price'}) or
                              item.find('span', {'itemprop': 'price'}))

                address_elem = (item.find('div', {'data-marker': 'item-address'}) or
                                item.find('span', class_='geo-address') or
                                item.find('div', class_='geo-root'))

                if not all([title_elem, price_elem, address_elem]):
                    logger.warning("Не все обязательные элементы найдены в объявлении")
                    continue

                title = title_elem.get_text(strip=True)

                # Обработка цены
                if price_elem.name == 'meta':
                    price = f"{price_elem['content']} ₽" if price_elem.has_attr('content') else "0 ₽"
                else:
                    price = price_elem.get_text(strip=True)
                    if '₽' not in price:
                        price += " ₽"

                address = address_elem.get_text(strip=True)

                # Основные данные
                title_elem = (item.find('h3', {'itemprop': 'name'}) or
                              item.find('h3', class_='title-root') or
                              item.find('a', {'data-marker': 'item-title'}))
                title = title_elem.get_text(strip=True) if title_elem else "N/A"

                # Определяем тип недвижимости
                description_elem = item.find('div', class_='iva-item-description')
                description = description_elem.get_text(strip=True).lower() if description_elem else ""

                if 'новостр' in description:
                    property_type = "новостройка"
                elif 'вторич' in description or 'апартамент' in description.lower():
                    property_type = "вторичка"
                else:
                    property_type = "вторичка"  # Значение по умолчанию

                # Обработка комнат и площади из заголовка
                try:
                    title_parts = [part.strip() for part in title.split(', ')]
                    rooms = title_parts[0]  # Первая часть - комнаты
                    area = next((part for part in title_parts if 'м²' in part), "N/A")

                    # Очистка площади (если нужно только число)
                    area_value = area.replace(' м²', '').strip() if area != "N/A" else "N/A"
                except Exception as e:
                    logger.error(f"Ошибка обработки заголовка: {str(e)}")
                    rooms = "N/A"
                    area_value = "N/A"
                    area = area_value
                logger.info(
                    f"Найдено: {title} | {price} | {address} | Тип: {property_type} | Комнаты: {rooms} | Площадь: {area} | Ссылка: {link}")
                save_sale(conn, address, property_type, price, rooms, area, link)
                parsed_count += 1

            except Exception as e:
                logger.error(f"Ошибка обработки объявления: {str(e)}", exc_info=True)
                continue

            logger.info(f"Успешно обработано объявлений: {parsed_count}/{len(items[:5])}")

    except Exception as e:
        logger.error(f"Ошибка при парсинге продаж: {str(e)}", exc_info=True)
    finally:
        try:
            if driver:
                driver.quit()
                logger.info("Драйвер успешно закрыт")
        except Exception as e:
            logger.error(f"Ошибка при закрытии драйвера: {str(e)}")

        try:
            if conn:
                conn.close()
                logger.info("Соединение с БД успешно закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с БД: {str(e)}")


if __name__ == "__main__":
    logger.info("🔍 Начинаем парсинг аренды...")
    parse_avito_rent()

    logger.info("\n🔍 Начинаем парсинг продаж...")
    parse_avito_sale()

    logger.info("✅ Весь парсинг завершен")