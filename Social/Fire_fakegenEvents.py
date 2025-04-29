import random
import time
from datetime import datetime
import json
import logging
import re
import sys
import os

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[
        logging.FileHandler("fire_disaster_generator.log"),
        logging.StreamHandler()
    ]
)

# --- Basic data ---
sources = ['X', 'TikTok', 'Telegram']
locations = [
    "Los Angeles", "San Diego", "San Francisco", "Sacramento", "Santa Barbara",
    "Fresno", "Palm Springs", "San Jose", "Redding", "Bakersfield"
]
tags_pool = [
    "#fire", "#emergency", "#evacuation", "#California", "#flames", "#blackout",
    "#help", "#rescue", "#smoke", "#alert"
]
types = ['panic', 'conspiracy', 'spam', 'fake_alert', 'generic']

# --- Load templates from JSON file ---
with open('california_fire_templates_en.json', 'r') as f:
    templates = json.load(f)

# --- Add noise to text ---
def add_noise(text, level):
    if level == 0:
        return text

    noisy = text

    # Typo simulation (letter swaps)
    if level > 0.2:
        words = noisy.split()
        for i in range(len(words)):
            if len(words[i]) > 3 and random.random() < level:
                idx = random.randint(1, len(words[i]) - 2)
                word = list(words[i])
                word[idx], word[idx+1] = word[idx+1], word[idx]
                words[i] = ''.join(word)
        noisy = ' '.join(words)

    # Random emojis
    if level > 0.4:
        emojis = ['🔥', '😱', '🚨', '⚠️', '❗', '😭']
        noisy += ' ' + ''.join(random.choices(emojis, k=random.randint(1, 3)))

    # Corrupted characters
    if level > 0.6:
        insert_pos = random.randint(0, len(noisy))
        noisy = noisy[:insert_pos] + ' � ' + noisy[insert_pos:]

    # Slang and abbreviations
    if level > 0.3:
        slang_dict = {
            "emergency": "emergz",
            "evacuation": "evac",
            "help": "hlp",
            "flames": "flmz",
            "rescue": "rsc",
            "attention": "attn",
            "danger": "dngr",
            "run": "gooo!!",
            "fire": "🔥FIRE",
            "alert": "alrt"
        }
        for k, v in slang_dict.items():
            if random.random() < level:
                noisy = re.sub(rf'\b{k}\b', v, noisy, flags=re.IGNORECASE)

    # Word repetition
    if level > 0.5:
        noisy += ' ' + ' '.join([random.choice(noisy.split()) for _ in range(2)])

    return noisy

# --- Generate message ---
def generate_message():
    source = random.choice(sources)
    location = random.choice(locations)
    msg_type = random.choice(types)
    template = random.choice(templates[msg_type])
    tags = ' '.join(random.sample(tags_pool, k=random.randint(1, 3)))
    content = template.replace("{location}", location)
    noise_level = round(random.uniform(0, 1), 2)
    noisy_content = add_noise(content, noise_level)

    return {
        'source': source,
        'timestamp': datetime.now().isoformat(),
        'location': location,
        'content': noisy_content,
        'noise_level': noise_level,
        'tags': tags,
        'type': msg_type
    }

# --- Preview mode ---
def preview_mode(n=5):
    logging.info("PREVIEW MODE - Generating sample messages")
    preview_messages = []
    for _ in range(n):
        msg = generate_message()
        preview_messages.append(msg)
        logging.info(f"Preview: [{msg['type']}] {msg['location']} via {msg['source']} | noise={msg['noise_level']}\nContent: {msg['content']}")

    # Save to file
    preview_path = "preview_messages.json"
    with open(preview_path, 'w') as f:
        json.dump(preview_messages, f, indent=2)
    logging.info(f"Preview messages saved to {preview_path}")

# --- Loop ---
if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'preview':
        preview_mode()
    else:
        while True:
            try:
                msg = generate_message()
                logging.info(f"Message sent: [{msg['type']}] {msg['location']} via {msg['source']} | noise={msg['noise_level']}\nContent: {msg['content']}")
                time.sleep(random.uniform(1.0, 3.0))
            except Exception as e:
                logging.error(f"Error while sending message: {e}")
