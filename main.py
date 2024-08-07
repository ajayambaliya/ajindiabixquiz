import logging
import asyncio
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import urllib3
from pymongo import MongoClient
from deep_translator import GoogleTranslator
from deep_translator.exceptions import RequestError
from telegram import Bot
from telegram.constants import PollType, ParseMode
from telegram.error import TelegramError
from datetime import datetime
import os
import pytz
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import time

# Disable SSL/TLS-related warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
MONGO_CONNECTION_STRING = os.environ.get('MONGO_CONNECTION_STRING')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_USERNAME = os.environ.get('TELEGRAM_CHANNEL_USERNAME')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleTranslatorWrapper:
    def __init__(self):
        self.translator = GoogleTranslator(source="auto", target="gu")

    def translate(self, text):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return self.translator.translate(text)
            except RequestError as e:
                logger.error(f"Translation error (attempt {attempt + 1}): {e}")
                time.sleep(2)  # Wait before retrying
            except Exception as e:
                logger.error(f"Unexpected error in translation (attempt {attempt + 1}): {e}")
                time.sleep(2)  # Wait before retrying
        return text  # Return original text if all attempts fail

class MongoDBManager:
    def __init__(self):
        self.client = MongoClient(MONGO_CONNECTION_STRING)
        self.db = self.client["current_affairs"]

    def get_or_create_collection(self, year, month):
        return self.db[str(year)][str(month)]

    def insert_question(self, collection, question_doc):
        collection.insert_one(question_doc)

    def get_question_collections(self):
        return self.db.list_collection_names()

    def get_questions_from_collection(self, collection_name):
        return list(self.db[collection_name].find())

    def close_connection(self):
        self.client.close()

class TelegramQuizBot:
    def __init__(self, token, channel_username):
        self.bot = Bot(token=token)
        self.channel_username = channel_username

    def truncate_text(self, text, max_length):
        return text[:max_length-3] + '...' if len(text) > max_length else text

    async def send_poll(self, question_doc):
        question = self.truncate_text(question_doc["question"], 300)
        options = [self.truncate_text(opt, 100) for opt in question_doc["options"]]
        correct_option = question_doc["value_in_braces"]
        explanation = self.truncate_text(question_doc["explanation"], 200)

        option_mapping = {chr(65+i): i for i in range(len(options))}  # Mapping 'A'->0, 'B'->1, etc.

        try:
            correct_option_id = option_mapping.get(correct_option)
            if correct_option_id is None:
                logger.error(f"Correct option '{correct_option}' not found in options: {options}")
                return

            await self.bot.send_poll(
                chat_id=self.channel_username,
                question=question,
                options=options,
                is_anonymous=True,
                type=PollType.QUIZ,
                correct_option_id=correct_option_id,
                explanation=explanation
            )
            logger.info(f"Sent poll: {question}")
        except TelegramError as e:
            logger.error(f"Failed to send poll: {e.message}")

def get_current_month():
    ist = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(ist)
    return f"{current_date.month:02d}"

def scrape_questions_to_mongodb():
    try:
        url = "https://www.indiabix.com/current-affairs/questions-and-answers/"
        month_digit = get_current_month()

        response = requests.get(url, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        link_elements = soup.find_all("a", class_="text-link me-3")

        valid_links = []
        for link_element in link_elements:
            href = link_element.get("href")
            if f"/current-affairs/2024-{month_digit}-" in href:
                full_url = urljoin("https://www.indiabix.com/", href)
                valid_links.append(full_url)

        translator = GoogleTranslatorWrapper()
        mongo_manager = MongoDBManager()

        new_questions = []

        for full_url in valid_links:
            _, year, month, day = full_url.split("/")[-4:]
            day = day.rstrip('/')

            collection = mongo_manager.get_or_create_collection(year, month)
            existing_question = collection.find_one({"day": day})
            
            if existing_question:
                logger.info(f"Data for {year}-{month}-{day} already exists. Skipping.")
                continue

            response = requests.get(full_url, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            question_divs = soup.find_all("div", class_="bix-div-container")

            for question_div in question_divs:
                try:
                    qtxt = question_div.find("div", class_="bix-td-qtxt").text.strip()
                    options_div = question_div.find("div", class_="bix-tbl-options")
                    option_rows = options_div.find_all("div", class_="bix-opt-row")
                    options = [option_row.find("div", class_="bix-td-option-val").text.strip() for option_row in option_rows]

                    hidden_input = question_div.find("input", class_="jq-hdnakq")
                    value_in_braces = hidden_input['value'].split('{', 1)[-1].rsplit('}', 1)[0] if hidden_input and 'value' in hidden_input.attrs else ""

                    answer_div = question_div.find("div", class_="bix-div-answer")
                    explanation = answer_div.find("div", class_="bix-ans-description").text.strip()

                    translated_qtxt = translator.translate(qtxt)
                    translated_options = [translator.translate(option) for option in options]
                    translated_explanation = translator.translate(explanation)

                    option_map = {'A': 0, 'B': 1, 'C': 2, 'D': 3}
                    correct_option = value_in_braces.upper()
                    correct_option_id = option_map.get(correct_option, 0)

                    question_doc = {
                        "question": translated_qtxt,
                        "options": translated_options,
                        "value_in_braces": value_in_braces,
                        "explanation": translated_explanation,
                        "correct_option_id": correct_option_id,
                        "day": day
                    }

                    mongo_manager.insert_question(collection, question_doc)
                    new_questions.append(question_doc)

                except Exception as e:
                    logger.error(f"Error scraping content: {e}")

        mongo_manager.close_connection()
        return new_questions

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching initial URL: {e}")
        return []

async def send_new_questions_to_telegram(new_questions):
    bot = TelegramQuizBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_USERNAME)
    for question in new_questions:
        await bot.send_poll(question)
        await asyncio.sleep(3)

async def main():
    new_questions = scrape_questions_to_mongodb()
    if new_questions:
        await send_new_questions_to_telegram(new_questions)
    else:
        logger.info("No new questions found.")

def run_server():
    httpd = HTTPServer(('', 8080), SimpleHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == "__main__":
    # Start the web server in a separate thread
    server_thread = threading.Thread(target=run_server)
    server_thread.start()

    # Run the main scraping function
    asyncio.run(main())

    # Keep the script running
    server_thread.join()
