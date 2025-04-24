from flask import Flask, request, render_template
import psycopg2
from dotenv import load_dotenv
import os
import requests

load_dotenv()
app = Flask(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5433")
}

TELEGRAM_API_URL = f"https://api.telegram.org/bot{os.getenv('BOT_TOKEN')}/sendMessage"


@app.route("/")
def index():
    user_id = request.args.get("user_id")
    return render_template("index.html", user_id=user_id)


@app.route("/rent", methods=["GET", "POST"])
def rent():
    user_id = request.args.get("user_id")
    if request.method == "POST":
        rooms = request.form.get("rooms")
        area = request.form.get("area")

        query = "SELECT address, price, rooms, area, link FROM rental WHERE TRUE"
        params = []

        if rooms:
            room_mapping = {
                "0": "Квартира-студия",
                "1": "1-к. квартира",
                "2": "2-к. квартира",
                "3": "3-к. квартира",
                "4": "4-к. квартира",
                "5": "5-к. квартира"
            }
            query += " AND rooms = %s"
            params.append(room_mapping[rooms])

        if area:
            query += """
            AND CAST(
                NULLIF(
                    REPLACE(
                        REGEXP_REPLACE(
                            REPLACE(area, ' м²', ''),
                            '[^0-9,]', '', 'g'
                        ),
                        ',', '.'
                    ),
                    ''
                ) AS numeric
            ) >= %s
            """
            params.append(float(area.replace(',', '.')))

        results = get_results(query, params)

        if not results:
            return render_template("results.html", user_id=user_id)

        if user_id:
            send_to_telegram(user_id, results)

        return render_template("results.html", has_results=bool(results))

    return render_template("rent_filters.html", user_id=user_id)


@app.route("/buy", methods=["GET", "POST"])
def buy():
    user_id = request.args.get("user_id")
    if request.method == "POST":
        rooms = request.form.get("rooms")
        area = request.form.get("area")
        property_type = request.form.get("type")

        query = "SELECT address, price, rooms, area, link FROM sale WHERE TRUE"
        params = []

        if rooms:
            room_mapping = {
                "0": "Квартира-студия",
                "1": "1-к. квартира",
                "2": "2-к. квартира",
                "3": "3-к. квартира",
                "4": "4-к. квартира",
                "5": "5-к. квартира"
            }
            query += " AND rooms = %s"
            params.append(room_mapping[rooms])

        if area:
            query += """
            AND CAST(
                NULLIF(
                    REPLACE(
                        REGEXP_REPLACE(
                            REPLACE(area, ' м²', ''),
                            '[^0-9,]', '', 'g'
                        ),
                        ',', '.'
                    ),
                    ''
                ) AS numeric
            ) >= %s
            """
            params.append(float(area.replace(',', '.')))

        if property_type and property_type != "any":
            query += " AND property_type = %s"
            params.append(property_type)

        # Остальной код остается без изменений
        results = get_results(query, params)

        if not results:
            return render_template("results.html", user_id=user_id)

        if user_id:
            send_to_telegram(user_id, results)

        return render_template("results.html", has_results=bool(results))

    return render_template("buy_filters.html", user_id=user_id)

def get_results(query, params=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(query, params or ())
    rows = cur.fetchall()
    conn.close()
    return rows


def send_to_telegram(user_id, results):
    print(f"Attempting to send to Telegram. User ID: {user_id}")  # Отладочная печать
    print(f"Results count: {len(results)}")  # Отладочная печать

    if not results:
        text = "🔍 По вашим фильтрам ничего не найдено.\n\nПопробуйте изменить параметры поиска."
    else:
        text = "🏡 *Найдены подходящие предложения:*\n\n"
        for r in results[:5]:
            address, price, rooms, area, link = r
            text += f"📍 *Адрес:* {address}\n"
            text += f"💵 *Цена:* {price}\n"
            text += f"🛏 *Комнат:* {rooms}\n"
            text += f"📏 *Площадь:* {area}\n"
            if link:
                text += f"🔗 [Ссылка на объявление]({link})\n"
            text += "\n"



    payload = {
        "chat_id": user_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(TELEGRAM_API_URL, data=payload)
        print(f"Telegram API response: {response.status_code}, {response.text}")  # Отладочная печать
    except Exception as e:
        print(f"Error sending to Telegram: {e}")  # Отладочная печать


if __name__ == "__main__":
    app.run(debug=True, port=8000)