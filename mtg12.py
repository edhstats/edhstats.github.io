import sqlite3
import pandas as pd
import argparse
from tabulate import tabulate
import os
from datetime import datetime
import requests
from rapidfuzz import process
import uuid
from jinja2 import Template
from functools import lru_cache
from rapidfuzz import process, fuzz
import json
from collections import defaultdict
import urllib.parse
from datetime import datetime
import re


color_names = {
    'W': 'Mono White',
    'U': 'Mono Blue',
    'B': 'Mono Black',
    'R': 'Mono Red',
    'G': 'Mono Green',
    'WU': 'Azorius',
    'UW': 'Azorius',
    'WB': 'Orzhov',
    'BW': 'Orzhov',
    'UB': 'Dimir',
    'BU': 'Dimir',
    'UR': 'Izzet',
    'RU': 'Izzet',
    'BR': 'Rakdos',
    'RB': 'Rakdos',
    'BG': 'Golgari',
    'GB': 'Golgari',
    'RG': 'Gruul',
    'GR': 'Gruul',
    'WG': 'Selesnya',
    'GW': 'Selesnya',
    'WR': 'Boros',
    'RW': 'Boros',
    'UG': 'Simic',
    'GU': 'Simic',
    'WUB': 'Esper',
    'WBU': 'Esper',
    'UWB': 'Esper',
    'UBW': 'Esper',
    'BWU': 'Esper',
    'BUW': 'Esper',
    'UBR': 'Grixis',
    'URB': 'Grixis',
    'BUR': 'Grixis',
    'BRU': 'Grixis',
    'RUB': 'Grixis',
    'RBU': 'Grixis',
    'BRG': 'Jund',
    'BGR': 'Jund',
    'RBG': 'Jund',
    'RGB': 'Jund',
    'GBR': 'Jund',
    'GRB': 'Jund',
    'RGW': 'Naya',
    'RWG': 'Naya',
    'GRW': 'Naya',
    'GWR': 'Naya',
    'WRG': 'Naya',
    'WGR': 'Naya',
    'GWU': 'Bant',
    'GUW': 'Bant',
    'WUG': 'Bant',
    'WGU': 'Bant',
    'UWG': 'Bant',
    'UGW': 'Bant',
    'WUR': 'Jeskai',
    'WRU': 'Jeskai',
    'UWR': 'Jeskai',
    'URW': 'Jeskai',
    'RWU': 'Jeskai',
    'RUW': 'Jeskai',
    'URG': 'Temur',
    'UGR': 'Temur',
    'GRU': 'Temur',
    'GUR': 'Temur',
    'RUG': 'Temur',
    'RGU': 'Temur',
    'WBG': 'Abzan',
    'WGB': 'Abzan',
    'BWG': 'Abzan',
    'BGW': 'Abzan',
    'GWB': 'Abzan',
    'GBW': 'Abzan',
    'UBG': 'Sultai',
    'UGB': 'Sultai',
    'BUG': 'Sultai',
    'BGU': 'Sultai',
    'GUB': 'Sultai',
    'GBU': 'Sultai',
    'WBR': 'Mardu',
    'WRB': 'Mardu',
    'BWR': 'Mardu',
    'BRW': 'Mardu',
    'RWB': 'Mardu',
    'RBW': 'Mardu',
    'WUBRG': '5-Color',
    'WUBGR': '5-Color',
    'WU BRG': '5-Color',
    'WUGBR': '5-Color',
    'WUGRB': '5-Color',
    'WURBG': '5-Color',
    'WURGB': '5-Color',
    'WUBRG': '5-Color',
    'WUBGR': '5-Color',
    'WUBRG': '5-Color',
    'WRUBG': '5-Color',
    'WRGBU': '5-Color',
  
}



# Percorso del file database persistente
DB_PATH = 'edh_stats.db'

timestamp = datetime.now().isoformat(timespec='minutes')  # es. "2025-04-16T14:03"
# Creiamo o connettiamo a un database SQLite
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Cache for validated commander names
COMMANDER_CACHE = {}

def normalize_name(name):
    """
    Normalizes a name by:
    - Converting to lowercase
    - Removing extra spaces
    - Removing special characters
    - Standardizing apostrophes and diacritics
    """
    if not name:  # Controlla se name è None o una stringa vuota
        return None  
        
    # Convert to lowercase
    name = name.lower()
    
    # Remove extra spaces
    name = " ".join(name.split())
    
    # Remove special characters but keep apostrophes and spaces
    name = re.sub(r'[^a-z0-9\' ]', '', name)
    
    # Standardize apostrophes
    name = name.replace("'", "'")
    
    return name

def normalize_commander_name(name):
    """
    Normalizza specificamente i nomi dei comandanti per prevenire duplicati.
    Questa funzione deve essere usata SEMPRE quando si inserisce un comandante nel database.
    """
    if not name or not name.strip():
        return None
    
    # Rimuovi spazi iniziali e finali
    name = name.strip()
    
    # Converti in lowercase per consistenza
    name = name.lower()
    
    # Normalizza spazi multipli in spazi singoli
    name = " ".join(name.split())
    
    # Standardizza apostrofi
    name = name.replace("'", "'").replace("`", "'")
    
    # Rimuovi caratteri speciali problematici ma mantieni quelli necessari per i nomi MTG
    # Manteniamo: lettere, numeri, spazi, apostrofi, virgole, trattini, slash per le carte doppie
    name = re.sub(r'[^\w\s\',\-/]', '', name, flags=re.UNICODE)
    
    # Normalizza i separatori per le carte doppie
    name = re.sub(r'\s*//\s*', ' // ', name)  # Standardizza " // " per le carte doppie
    
    # Normalizza le virgole: aggiungi spazio dopo virgola se mancante
    name = re.sub(r',(?!\s)', ', ', name)  # Aggiungi spazio dopo virgola se non c'è
    
    return name

def find_existing_commander_by_normalized_name(normalized_name):
    """
    Cerca un comandante esistente usando il nome normalizzato.
    Restituisce l'ID del comandante se trovato, None altrimenti.
    """
    if not normalized_name:
        return None
        
    cursor.execute("SELECT id FROM commanders WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))", (normalized_name,))
    result = cursor.fetchone()
    return result[0] if result else None

@lru_cache(maxsize=1000)
def get_cached_commander_info(name):
    """
    Cached version of commander info fetch to avoid repeated API calls.
    Uses Python's built-in LRU cache decorator.

    """
    normalized_name = normalize_name(name)
    if not normalized_name:
        return None  # Evita di chiamare fetch_commander_info con None
    return fetch_commander_info(normalized_name)

def find_similar_commander(name, threshold=80):
    """
    Finds similar commander names in the database using fuzzy matching.
    Returns the closest match above the threshold, or None if no match is found.
    """

    cursor.execute("SELECT name FROM commanders")
    existing_commanders = [row[0] for row in cursor.fetchall()]
    
    if not existing_commanders:
        return None
        
    # Find the best match
    best_match, score = process.extractOne(
        name,
        existing_commanders,
        scorer=fuzz.ratio
    )
    
    if score >= threshold:
        return best_match
    return None

def validate_commander_name(name):
    """
    Validates a commander name and suggests corrections if needed.
    Returns (normalized_name, is_valid, suggestion)
    """
    normalized_name = normalize_name(name)
    
    # Check cache first
    if normalized_name in COMMANDER_CACHE:
        return COMMANDER_CACHE[normalized_name], True, None
    
    # Try to fetch from Scryfall
    commander_info = get_cached_commander_info(normalized_name)
    
    if commander_info:
        COMMANDER_CACHE[normalized_name] = commander_info["name"]
        return commander_info["name"], True, None
    
    # If not found, try fuzzy matching with existing commanders
    suggestion = find_similar_commander(normalized_name)
    return normalized_name, False, suggestion

# Nota: Esisteva un duplicato di get_or_create_commander che connetteva a 'database.db'.
# È stato rimosso per evitare inconsistenze: ora si utilizza solo DB_PATH e la versione completa sotto.

def create_tables():
    cursor.execute('''CREATE TABLE IF NOT EXISTS players (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL
                      )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS commanders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        color_identity TEXT,
                        mana_cost TEXT,
                        cmc REAL
                      )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS matches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        player_id INTEGER NOT NULL,
                        commander_id INTEGER NOT NULL,
                        win INTEGER NOT NULL,
                        game_id TEXT NOT NULL,
                        used_themed_deck INTEGER DEFAULT 0,
                        FOREIGN KEY (player_id) REFERENCES players(id),
                        FOREIGN KEY (commander_id) REFERENCES commanders(id)
                      )''')
    # Lightweight migrations to add missing columns on existing DBs
    try:
        cursor.execute("PRAGMA table_info(commanders)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'mana_cost' not in cols:
            cursor.execute("ALTER TABLE commanders ADD COLUMN mana_cost TEXT")
        if 'cmc' not in cols:
            cursor.execute("ALTER TABLE commanders ADD COLUMN cmc REAL")
    except Exception:
        pass
    try:
        cursor.execute("PRAGMA table_info(matches)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'used_themed_deck' not in cols:
            cursor.execute("ALTER TABLE matches ADD COLUMN used_themed_deck INTEGER DEFAULT 0")
    except Exception:
        pass
    conn.commit()

def fetch_commander_info(name):
    normalized_name = name.lower().replace(" ", "+")
    url = f"https://api.scryfall.com/cards/named?exact={normalized_name}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return {
            "name": data["name"],
            "mana_cost": data.get("mana_cost", ""),
            "cmc": data.get("cmc", 0),
            "color_identity": data.get("color_identity", []),
            "type_line": data.get("type_line", ""),
            "oracle_text": data.get("oracle_text", "")
        }
    else:
        print(f"Errore nel recupero per '{name}'. Codice: {response.status_code}")
        return None

def get_or_create_player(name):
    cursor.execute("SELECT id FROM players WHERE name = ?", (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO players (name) VALUES (?)", (name,))
    conn.commit()
    return cursor.lastrowid

def get_or_create_commander(name):
    """
    Ottiene o crea un comandante nel database con normalizzazione automatica del nome.
    Previene duplicati usando la normalizzazione consistente.
    """
    if not name or not name.strip():
        print(f"Errore: nome comandante vuoto o non valido.")
        return None
    
    # Normalizza il nome del comandante
    normalized_name = normalize_commander_name(name)
    if not normalized_name:
        print(f"Errore: impossibile normalizzare il nome '{name}'.")
        return None
    
    # Controlla se esiste già un comandante con questo nome normalizzato
    existing_id = find_existing_commander_by_normalized_name(normalized_name)
    if existing_id:
        return existing_id
    
    # Prova a ottenere informazioni da Scryfall usando il nome originale
    commander_info = fetch_commander_info(name)
    if commander_info:
        # Usa il nome normalizzato da Scryfall se disponibile, altrimenti quello normalizzato localmente
        scryfall_normalized = normalize_commander_name(commander_info["name"])
        final_name = scryfall_normalized if scryfall_normalized else normalized_name
        
        # Controlla di nuovo se esiste con il nome da Scryfall
        existing_id = find_existing_commander_by_normalized_name(final_name)
        if existing_id:
            return existing_id
        
        color_identity = ''.join(commander_info["color_identity"])
        mana_cost = commander_info["mana_cost"]
        cmc = commander_info["cmc"]
    else:
        # Se Scryfall non trova il comandante, usa il nome normalizzato e valori di default
        print(f"Attenzione: comandante '{name}' non trovato su Scryfall, inserimento con dati limitati.")
        final_name = normalized_name
        color_identity = ""
        mana_cost = ""
        cmc = 0
    
    # Inserisci il nuovo comandante
    cursor.execute("""
        INSERT INTO commanders (name, color_identity, mana_cost, cmc)
        VALUES (?, ?, ?, ?)
    """, (final_name, color_identity, mana_cost, cmc))
    
    conn.commit()
    print(f"✅ Comandante creato: '{final_name}' (ID: {cursor.lastrowid})")
    return cursor.lastrowid

def get_or_create_commander_bulk(name, cursor):
    """
    Versione ottimizzata per bulk upload che usa la stessa normalizzazione
    ma evita chiamate API multiple e usa la connessione passata.
    """
    if not name or not name.strip():
        return None
    
    # Normalizza il nome del comandante
    normalized_name = normalize_commander_name(name)
    if not normalized_name:
        return None
    
    # Controlla se esiste già un comandante con questo nome normalizzato
    cursor.execute("SELECT id FROM commanders WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))", (normalized_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Per il bulk upload, inserisci con dati minimi (senza chiamate API)
    cursor.execute("""
        INSERT INTO commanders (name, color_identity, mana_cost, cmc)
        VALUES (?, '', '', 0)
    """, (normalized_name,))
    
    return cursor.lastrowid


# Funzione per registrare una partita
def record_match(date, players):
    # Genera un identificativo univoco per la partita
    game_id = str(uuid.uuid4())
    
    for player in players:
        player_id = get_or_create_player(player['name'])
        commander_id = get_or_create_commander(player['commander'])
        win = 1 if player['win'] else 0
        cursor.execute(
            "INSERT INTO matches (date, player_id, commander_id, win, game_id) VALUES (?, ?, ?, ?, ?)",
            (date, player_id, commander_id, win, game_id)
        )
    conn.commit()
    print(f"Partita registrata con successo. ID partita: {game_id}")

# Funzione per caricare partite in blocco da file
def bulk_upload_matches(filename):
    """
    Carica partite da un file di testo nel database.
    Formato del file:
    09.09.25
    Giulia: Muldrotha, the Gravetide [T]
    Marco: Niv-Mizzet, Parun
    Luca: Kaalia of the Vast [T]
    W: Muldrotha, the Gravetide
    """
    # Usa una connessione dedicata con timeout e WAL per ridurre i lock
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ Errore: Il file '{filename}' non è stato trovato.")
        return

    date = None
    players_data = []
    
    # Inizializza la transazione per una maggiore efficienza e sicurezza
    conn.execute('BEGIN TRANSACTION')

    try:
        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Riconosce la data solo se la riga corrisponde ESATTAMENTE al formato dd.mm.yy o dd/mm/yy
            if re.fullmatch(r"\d{2}[./]\d{2}[./]\d{2}", line):
                try:
                    # Gestisce sia separatore '.' che '/'
                    if '.' in line:
                        date = datetime.strptime(line, '%d.%m.%y').strftime('%Y-%m-%d')
                    else:
                        date = datetime.strptime(line, '%d/%m/%y').strftime('%Y-%m-%d')
                except ValueError:
                    print(f"⚠️ Formato data non valido: {line}. Salto la riga.")
                    date = None
                # Se arriva una nuova data ma abbiamo ancora giocatori pendenti senza 'W:', resettare in sicurezza
                if players_data:
                    print("⚠️ Nuova data trovata mentre una partita precedente non era stata chiusa con 'W:'. Salto quella partita parziale.")
                    players_data = []
                continue
            
            # Riconosce il vincitore della partita
            if line.startswith('W:'):
                winner_name_raw = line.split(':', 1)[1].strip().replace('[T]', '').strip()
                winner_name_normalized = normalize_commander_name(winner_name_raw)
                
                if not date or not players_data:
                    print(f"❌ Errore: 'W:' trovato senza una data o giocatori precedenti. Salto la partita.")
                    players_data = [] # Reset for next valid match
                    continue

                # Verifica che il numero di giocatori sia 3 o 4
                if len(players_data) not in (3, 4):
                    print(f"⚠️ Numero giocatori non valido ({len(players_data)}). Attese partite da 3 o 4 giocatori. Salto la partita del {date}.")
                    players_data = []
                    continue

                game_id = str(uuid.uuid4())

                # Processa ogni giocatore e inserisci i dati della partita
                for player_data in players_data:
                    player_name, commander_name, is_themed = player_data
                    
                    # Cerca o crea il player
                    cursor.execute("INSERT OR IGNORE INTO players (name) VALUES (?)", (player_name,))
                    cursor.execute("SELECT id FROM players WHERE name = ?", (player_name,))
                    player_id = cursor.fetchone()[0]

                    # Cerca o crea il commander usando la normalizzazione sicura
                    commander_id = get_or_create_commander_bulk(commander_name, cursor)
                    
                    # Normalizza il nome del comandante per il confronto con il vincitore
                    commander_name_normalized = normalize_commander_name(commander_name)

                    # Inserisce la partita nella tabella 'matches'
                    is_win = 1 if commander_name_normalized == winner_name_normalized else 0
                    cursor.execute(
                        """
                        INSERT INTO matches (game_id, player_id, commander_id, win, date, used_themed_deck)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (game_id, player_id, commander_id, is_win, date, is_themed)
                    )
                
                print(f"✅ Partita del {date} (ID: {game_id}) registrata correttamente.")
                players_data = [] # Reset for next match

            # Riconosce i giocatori e i loro commander
            elif ':' in line:
                parts = line.split(':', 1)
                player_name = parts[0].strip()
                commander_full = parts[1].strip()
                
                is_themed = 0
                if '[T]' in commander_full:
                    is_themed = 1
                    commander_name = commander_full.replace('[T]', '').strip()
                else:
                    commander_name = commander_full.strip()

                players_data.append((player_name, commander_name, is_themed))

    except Exception as e:
        conn.execute('ROLLBACK')
        print(f"❌ Si è verificato un errore critico durante l'upload: {e}")
        conn.close()
        return

    conn.execute('COMMIT')
    conn.close()
    print("✨ Caricamento in blocco completato.")


# Funzione per ottenere il link Scryfall per il comandante
def get_commander_scryfall_link(commander_name):

    if commander_name is None:
        print("ERROR: commander_name is None")
        return None
    normalized_name = commander_name.lower().replace(" ", "-")
    url = f"https://api.scryfall.com/cards/named?exact={normalized_name}"
    response = requests.get(url)
    
    # Verifica se la risposta Ã¨ corretta
    if response.status_code == 200:
        data = response.json()
        # Controlla se 'url' Ã¨ presente nella risposta
        if 'scryfall_uri' in data:
            return data['scryfall_uri']
        else:
            print(f"Errore: Nessun URL trovato per {commander_name}. Risposta API: {data}")
            return f"Link non disponibile per {commander_name}."
    else:
        print(f"Errore nella richiesta API per {commander_name}. Status code: {response.status_code}")
        return f"Errore nel trovare il link per {commander_name}."

def linkify_commander_names(df):
    df = df.copy()
    df['Comandante'] = df['Comandante'].apply(lambda name: f'<a href="https://scryfall.com/search?q={urllib.parse.quote(name)}" target="_blank">{name}</a>')
    return df

# === HTML Helper ===
def dataframe_to_table(df, table_id):
    return df.to_html(classes="display", table_id=table_id, index=False, border=0)
css_style = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');

  body {
    font-family: 'Inter', 'Roboto', 'Helvetica Neue', sans-serif;
    margin: 20px;
    background-color: #f5f7fa;
    color: #2e2e2e;
    line-height: 1.6;
    transition: background-color 0.3s, color 0.3s;
  }

  h1, h2, h3 {
    color: #1a1a1a;
    font-weight: 600;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 24px;
    background-color: #fff;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 2px 4px rgba(0,0,0,0.06);
    color: #2e2e2e; /* Ensure readable text on white background */
  }

  th, td {
    padding: 14px;
    border: 1px solid #e0e0e0;
    text-align: center;
    font-size: 0.95rem;
  }

  th {
    background-color: #5865f2;
    color: #fff;
    text-transform: uppercase;
    font-size: 0.85rem;
    letter-spacing: 0.05em;
  }

  tr:nth-child(even) {
    background-color: #f1f3f5;
  }

  tr:hover {
    background-color: #e4e8ee;
  }

  .player-section {
    margin-bottom: 30px;
    border: 1px solid #d8dee9;
    padding: 20px;
    border-radius: 12px;
    background-color: #ffffff;
    box-shadow: 0 2px 8px rgba(0,0,0,0.03);
    color: #2e2e2e; /* Force text color on white cards */
  }

  .player-section h3 {
    margin-top: 0;
    background-color: #f0f2f5;
    padding: 12px;
    border-radius: 6px;
    font-weight: 600;
    color: #3b5bdb;
  }

  .hidden {
    display: none;
  }

  /* ðŸŒ™ ModalitÃ  Scura */
  @media (prefers-color-scheme: dark) {
    body {
      background-color: #121212;
      color: #e4e4e4;
    }

    h1, h2, h3 {
      color: #f1f1f1;
    }

    table {
      background-color: #1e1e1e;
      box-shadow: 0 2px 4px rgba(0,0,0,0.4);
      color: #f1f5f9; /* Ensure readable text in dark tables */
    }

    th {
      background-color: #3b5bdb;
      color: #ffffff;
    }

    td, th {
      border-color: #2a2a2a;
    }

    tr:nth-child(even) {
      background-color: #222;
    }

    tr:hover {
      background-color: #2c2c2c;
    }

    .player-section {
      background-color: #1c1c1c;
      border-color: #2a2a2a;
    }

    .player-section h3 {
      background-color: #2a2a2a;
      color: #8faaff;
    }
  }
</style>
"""

# Fixed HTML Report Generator for Magic EDH Stats

def generate_enhanced_html_report(
    player_winrate_over_time,
    player_commander_stats,
    color_stats_most_played,
    color_stats_best_winrate,
    player_stats,
    victory_streak,
    commander_stats,
    player_vs_others,
    player_list,
    cmc_medio_totale,
    num_players,
    num_commanders,
    top_commanders_played,
    top_commanders_winrate,
    total_games,
    season_commanders_stats,
    conn
):
    """
    Genera un report HTML statico e funzionante
    """
    import pandas as pd
    import json
    from datetime import datetime
    
    timestamp = datetime.now().strftime('%d/%m/%Y alle %H:%M')
    
    # Query aggiuntive per insights avanzati
    most_versatile_player = pd.read_sql_query("""
        SELECT p.name as Giocatore, 
               COUNT(DISTINCT c.name) as Comandanti_Diversi,
               COUNT(m.id) as Partite_Totali,
               ROUND(COUNT(DISTINCT c.name) * 1.0 / COUNT(m.id) * 100, 2) as Versatilita_Perc
        FROM players p
        JOIN matches m ON p.id = m.player_id
        JOIN commanders c ON m.commander_id = c.id
        GROUP BY p.name
        HAVING COUNT(m.id) >= 10
        ORDER BY Comandanti_Diversi DESC
        LIMIT 5
    """, conn)
    
    meta_dominance = pd.read_sql_query("""
        WITH commander_stats AS (
            SELECT c.name,
                   COUNT(*) as games_played,
                   SUM(m.win) as wins,
                   ROUND(SUM(m.win) * 100.0 / COUNT(*), 2) as winrate
            FROM matches m
            JOIN commanders c ON m.commander_id = c.id
            GROUP BY c.name
            HAVING COUNT(*) >= 3
        )
        SELECT name as Comandante,
               games_played as Partite,
               wins as Vittorie,
               winrate as Winrate_Perc,
               CASE 
                   WHEN winrate >= 70 AND games_played >= 5 THEN 'Meta Dominante'
                   WHEN winrate >= 60 AND games_played >= 5 THEN 'Forte'
                   WHEN winrate >= 50 THEN 'Bilanciato'
                   ELSE 'Sottoperformante'
               END as Status_Meta
        FROM commander_stats
        ORDER BY winrate DESC, games_played DESC
        LIMIT 10
    """, conn)
    
    # Season 1 standings (01/10 to 01/01) with special commander bonus
    special_commanders = [
        'Beluna Grandsquall // Seek Thrills',
        'Gimbal, Gremlin Prodigy',
        'Isu the Abominable',
        'Licia, Sanguine Tribune',
        'Lynde, Cheerful Tormentor',
        'Mr. House, President and CEO',
        'Obeka, Brute Chronologist',
        'Pramikon, Sky Rampart',
        'Rienne, Angel of Rebirth',
        'Sigurd, Jarl of Ravensthorpe',
        "Sin, Spira's Punishment",
        'Sophia, Dogged Detective',
        'Sydri, Galvanic Genius',
        'Tatsunari, Toad Rider',
        'The Celestial Toymaker',
        'Xira, the Golden Sting',
        'Yurlok of Scorch Thrash',
        'Zedruu the Greathearted'
    ]
    placeholders = ",".join(["?"] * len(special_commanders))
    season_start = '2025-10-01'
    season_end = '2026-01-01'
    season_query = f"""
        SELECT p.name AS Giocatore,
               SUM(CASE WHEN m.win = 1 THEN CASE WHEN c.name IN ({placeholders}) THEN 2 ELSE 1 END ELSE 0 END) AS Punti,
               SUM(m.win) AS Vittorie,
               COUNT(*) AS Partite
        FROM matches m
        JOIN players p ON m.player_id = p.id
        JOIN commanders c ON m.commander_id = c.id
        WHERE m.date >= ? AND m.date < ?
        GROUP BY p.name
        ORDER BY Punti DESC, Vittorie DESC, Partite DESC
    """
    season_standings = pd.read_sql_query(season_query, conn, params=(special_commanders + [season_start, season_end]))
    
    # Season commanders statistics - include ALL season commanders, even unplayed ones
    # First, create a temporary table with all season commanders
    cursor.execute("DROP TABLE IF EXISTS temp_season_commanders")
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_season_commanders (
            name TEXT PRIMARY KEY,
            original_name TEXT
        )
    """)
    
    # Insert all season commanders (both existing and non-existing)
    for commander in special_commanders:
        # Usa sempre la normalizzazione per consistenza
        normalized_name = normalize_commander_name(commander)
        cursor.execute("INSERT OR IGNORE INTO temp_season_commanders (name, original_name) VALUES (?, ?)", 
                      (normalized_name, commander))
    
    season_commanders_query = f"""
        SELECT tsc.original_name AS Comandante,
               COALESCE(stats.Partite, 0) AS Partite,
               COALESCE(stats.Vittorie, 0) AS Vittorie,
               COALESCE(stats.Winrate, 0.0) AS "Winrate (%)"
        FROM temp_season_commanders tsc
        LEFT JOIN (
            SELECT LOWER(TRIM(REPLACE(REPLACE(c.name, '.', ''), '  ', ' '))) as normalized_name,
                   COUNT(*) AS Partite,
                   SUM(m.win) AS Vittorie,
                   ROUND(SUM(m.win) * 100.0 / COUNT(*), 2) AS Winrate
            FROM matches m
            JOIN commanders c ON m.commander_id = c.id
            WHERE m.date >= ? AND m.date < ?
            GROUP BY normalized_name
        ) stats ON LOWER(TRIM(tsc.name)) = stats.normalized_name
        ORDER BY Partite DESC, Vittorie DESC, Comandante ASC
    """
    season_commanders_stats = pd.read_sql_query(season_commanders_query, conn, params=[season_start, season_end])
    
    head_to_head = pd.read_sql_query("""
        WITH matchups AS (
            SELECT 
                p1.name as Giocatore1,
                p2.name as Giocatore2,
                SUM(CASE WHEN m1.win = 1 THEN 1 ELSE 0 END) as Vittorie_G1,
                SUM(CASE WHEN m2.win = 1 THEN 1 ELSE 0 END) as Vittorie_G2,
                COUNT(*) as Scontri_Totali
            FROM matches m1
            JOIN matches m2 ON m1.game_id = m2.game_id AND m1.player_id < m2.player_id
            JOIN players p1 ON m1.player_id = p1.id
            JOIN players p2 ON m2.player_id = p2.id
            GROUP BY p1.name, p2.name
            HAVING COUNT(*) >= 5
        )
        SELECT Giocatore1,
               Giocatore2,
               Vittorie_G1,
               Vittorie_G2,
               Scontri_Totali,
               ROUND(ABS(Vittorie_G1 - Vittorie_G2) * 100.0 / Scontri_Totali, 1) as Squilibrio_Perc
        FROM matchups
        ORDER BY Scontri_Totali DESC, Squilibrio_Perc DESC
        LIMIT 8
    """, conn)
    
    cmc_distribution = pd.read_sql_query("""
        SELECT c.cmc, COUNT(*) as frequency
        FROM matches m
        JOIN commanders c ON m.commander_id = c.id
        GROUP BY c.cmc
        ORDER BY c.cmc
    """, conn)
    
    # Prepara dati per JavaScript
    chart_data_js = {player: df.to_dict(orient='records') for player, df in player_winrate_over_time.items()}
    cmc_data_js = cmc_distribution.to_dict(orient='records')
    
    # Funzione helper per generare le tabelle
    def table_to_html(df, table_id):
        return df.to_html(classes="display", table_id=table_id, index=False, border=0, escape=False)
    
    # Helper: genera un semplice grafico SVG (statico) per l'andamento winrate (0-100)
    def winrate_svg(data_points, width=640, height=220, margin=30):
        # data_points: DataFrame pandas con colonne ['data','winrate'] oppure lista di dict
        records = []
        try:
            import pandas as pd  # type: ignore
        except Exception:
            pd = None  # type: ignore
        if data_points is None:
            records = []
        elif pd is not None and isinstance(data_points, pd.DataFrame):
            records = data_points.to_dict(orient='records')
        elif isinstance(data_points, (list, tuple)):
            records = list(data_points)
        else:
            try:
                records = list(data_points)
            except Exception:
                records = []
        if not records:
            return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"><text x="{margin}" y="{height/2}" fill="#555">Nessun dato</text></svg>'
        # Normalizza dati
        xs = list(range(len(records)))
        ys = [max(0.0, min(100.0, float(dp.get('winrate', 0) or 0))) for dp in records]
        labels = [str(dp.get('data', '')) for dp in records]
        # Area disegno
        plot_w = width - 2*margin
        plot_h = height - 2*margin
        def scale_x(i):
            return margin + (plot_w * (i / max(1, len(xs)-1)))
        def scale_y(v):
            # y 0 in basso, 100 in alto
            return margin + plot_h * (1 - (v/100.0))
        # Costruisci polyline
        points = ' '.join([f"{scale_x(i):.1f},{scale_y(ys[i]):.1f}" for i in range(len(xs))])
        # Assi semplici e griglia 0,25,50,75,100
        grid = []
        for val in [0,25,50,75,100]:
            y = scale_y(val)
            grid.append(f'<line x1="{margin}" y1="{y:.1f}" x2="{width-margin}" y2="{y:.1f}" stroke="#eee" stroke-width="1" />')
            grid.append(f'<text x="{5}" y="{y+4:.1f}" fill="#777" font-size="10">{val}%</text>')
        # Etichette X (solo primo, metà, ultimo per compattezza)
        xlabels = []
        if labels:
            idxs = sorted(set([0, len(labels)//2, len(labels)-1]))
            for i in idxs:
                if 0 <= i < len(labels):
                    x = scale_x(i)
                    xlabels.append(f'<text x="{x:.1f}" y="{height-5}" fill="#777" font-size="10" text-anchor="middle">{labels[i]}</text>')
        svg = f'''<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" />
  {''.join(grid)}
  <polyline fill="none" stroke="#2563eb" stroke-width="2" points="{points}" />
  {''.join(xlabels)}
</svg>'''
        return svg
    
    # Genera una sezione compatta a tendina per i comandanti dei giocatori
    player_commander_sections = "<details><summary><strong>Analisi Comandanti per Giocatore</strong> (clicca per espandere)</summary>"
    for player, df in player_commander_stats.items():
        pid = player.replace(" ", "-").replace("'", "")
        player_commander_sections += f'''
        <details>
            <summary><strong>{player}</strong></summary>
            <div id="player-{pid}" class="player-section">
                {table_to_html(df, f"commanderStats-{pid}")}
            </div>
        </details>
        '''
    player_commander_sections += "</details>"
    
    # Template HTML completo
    html_content = f'''<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EDH Stats Dashboard - Gruppo Magic</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/responsive/2.5.0/css/responsive.dataTables.min.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    
    <style>
        :root {{
            --primary-color: #2563eb;
            --secondary-color: #64748b;
            --success-color: #059669;
            --warning-color: #d97706;
            --danger-color: #dc2626;
            --bg-primary: #ffffff;
            --bg-secondary: #f8fafc;
            --text-primary: #1e293b;
            --text-secondary: #64748b;
            --border-color: #e2e8f0;
            --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
            --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1);
            --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1);
        }}
        
        @media (prefers-color-scheme: dark) {{
            :root {{
                --bg-primary: #0f172a;
                --bg-secondary: #1e293b;
                --text-primary: #f1f5f9;
                --text-secondary: #94a3b8;
                --border-color: #334155;
            }}
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-secondary);
            color: var(--text-primary);
            line-height: 1.6;
        }}
        
        .header {{
            background: linear-gradient(135deg, var(--primary-color), #3b82f6);
            color: white;
            padding: 2rem;
            text-align: center;
            box-shadow: var(--shadow-lg);
        }}
        
        .header h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }}
        
        .last-updated {{
            background: rgba(255,255,255,0.2);
            padding: 0.5rem 1rem;
            border-radius: 20px;
            display: inline-block;
            margin-top: 1rem;
            backdrop-filter: blur(10px);
            font-weight: 500;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        
        .stat-card {{
            background: var(--bg-primary);
            padding: 1.5rem;
            border-radius: 12px;
            box-shadow: var(--shadow-md);
            border: 1px solid var(--border-color);
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .stat-card:hover {{
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
        }}
        
        .stat-card h3 {{
            color: var(--text-secondary);
            font-size: 0.875rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }}
        
        .stat-value {{
            font-size: 2rem;
            font-weight: 700;
            color: var(--primary-color);
        }}
        
        .stat-icon {{
            float: right;
            font-size: 1.5rem;
            color: var(--text-secondary);
            opacity: 0.7;
        }}
        
        .section {{
            background: var(--bg-primary);
            border-radius: 12px;
            margin-bottom: 2rem;
            box-shadow: var(--shadow-md);
            border: 1px solid var(--border-color);
            overflow: hidden;
        }}
        
        .section-header {{
            padding: 1.5rem;
            border-bottom: 1px solid var(--border-color);
            background: var(--bg-secondary);
        }}
        
        .section-header h2 {{
            font-size: 1.5rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        .section-content {{
            padding: 1.5rem;
        }}
        
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
        }}
        
        .grid-3 {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 2rem;
        }}
        
        @media (max-width: 768px) {{
            .grid-2, .grid-3 {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 2rem;
            }}
            .container {{
                padding: 1rem;
                max-width: 100%;
            }}
            .stat-card .stat-value {{
                font-size: 1.6rem;
            }}
            .section-content {{
                padding: 1rem;
            }}
            .chart-container {{
                height: 320px;
            }}
        }}
        
        table.display {{
            width: 100% !important;
            margin: 0;
        }}
        
        table.display thead th {{
            background: var(--primary-color);
            color: white;
            font-weight: 600;
            padding: 12px 8px;
            font-size: 0.875rem;
        }}
        
        table.display tbody td {{
            padding: 10px 8px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        table.display tbody tr:hover {{
            background: var(--bg-secondary);
        }}
        /* Ensure tables are scrollable on small screens */
        .section-content {{
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}
        table.dataTable td, table.dataTable th {{
            white-space: nowrap;
        }}
        
        .meta-badge {{
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}
        
        .meta-dominante {{ background: #fecaca; color: #991b1b; }}
        .meta-forte {{ background: #fed7aa; color: #9a3412; }}
        .meta-bilanciato {{ background: #bbf7d0; color: #166534; }}
        .meta-sottoperformante {{ background: #e5e7eb; color: #374151; }}
        
        .chart-container {{
            position: relative;
            height: 400px;
            margin: 1rem 0;
        }}
        
        .insight-box {{
            background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
            border-left: 4px solid var(--primary-color);
            padding: 1rem;
            margin: 1rem 0;
            border-radius: 8px;
        }}
        
        .insight-box h4 {{
            color: var(--primary-color);
            margin-bottom: 0.5rem;
        }}
        
        .player-selector {{
            margin-bottom: 1rem;
        }}
        
        .player-selector select {{
            padding: 0.5rem 1rem;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            background: var(--bg-primary);
            color: var(--text-primary);
            font-size: 1rem;
            min-width: 200px;
        }}
        
        .activity-indicator {{
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            background: var(--success-color);
            color: white;
            font-size: 0.875rem;
            font-weight: 600;
            margin-bottom: 1rem;
        }}
        
        .color-analysis-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            margin-bottom: 2rem;
        }}
        
        @media (max-width: 768px) {{
            .color-analysis-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1><i class="fas fa-magic"></i> EDH Stats Dashboard</h1>
        <p>Statistiche complete del gruppo Magic EDH</p>
        <div class="last-updated">
            <i class="fas fa-clock"></i> Ultimo aggiornamento: {timestamp}
        </div>
    </div>
    
    <div class="container">
        <!-- Overview Stats -->
        <div class="stats-grid">
            <div class="stat-card">
                <i class="fas fa-gamepad stat-icon"></i>
                <h3>Partite Totali</h3>
                <div class="stat-value">{total_games}</div>
            </div>
            <div class="stat-card">
                <i class="fas fa-users stat-icon"></i>
                <h3>Giocatori Attivi</h3>
                <div class="stat-value">{num_players}</div>
            </div>
            <div class="stat-card">
                <i class="fas fa-chess-king stat-icon"></i>
                <h3>Comandanti Giocati</h3>
                <div class="stat-value">{num_commanders}</div>
            </div>
            <div class="stat-card">
                <i class="fas fa-chart-line stat-icon"></i>
                <h3>CMC Medio</h3>
                <div class="stat-value">{cmc_medio_totale}</div>
            </div>
        </div>

        <!-- Season 1 Standings (01 Ottobre - 01 Gennaio) -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-trophy"></i> Classifica Season 1 (01 Ottobre - 01 Gennaio)</h2>
            </div>
            <div class="section-content">
                {table_to_html(season_standings, "season1Standings")}
            </div>
        </div>

        <!-- Season Commanders Statistics -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-star"></i> Statistiche Comandanti Season (+2 Punti)</h2>
            </div>
            <div class="section-content">
                <div class="insight-box">
                    <h4><i class="fas fa-info-circle"></i> Comandanti Season</h4>
                    <p>Questi comandanti danno <strong>+2 punti</strong> invece di +1 quando vincono durante la Season 1 (01 Ottobre - 01 Gennaio). 
                    Sono comandanti meno giocati o con tematiche specifiche per incentivare la diversità nel meta.</p>
                </div>
                {table_to_html(season_commanders_stats, "seasonCommandersStats")}
            </div>
        </div>

        <!-- Performance Giocatori -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-trophy"></i> Performance Giocatori</h2>
            </div>
            <div class="section-content">
                <div class="grid-2">
                    <div>
                        <h3>Classifica Generale</h3>
                        {table_to_html(player_stats, "playerStats")}
                    </div>
                    <div>
                        <h3>Versatilita Giocatori</h3>
                        <div class="insight-box">
                            <h4><i class="fas fa-lightbulb"></i> Insight</h4>
                            <p>I giocatori piu versatili utilizzano una varieta maggiore di comandanti, 
                            dimostrando conoscenza del formato e capacita di adattamento.</p>
                        </div>
                        {table_to_html(most_versatile_player, "versatilePlayer")}
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Meta Analysis -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-chess"></i> Analisi Meta</h2>
            </div>
            <div class="section-content">
                <div class="insight-box">
                    <h4><i class="fas fa-chart-bar"></i> Meta Insight</h4>
                    <p>Comandanti dominanti possono indicare strategie vincenti o power level elevato. 
                    Un meta bilanciato favorisce la diversita e partite piu coinvolgenti.</p>
                </div>
                {table_to_html(meta_dominance, "metaDominance")}
            </div>
        </div>
        
        <!-- Color Identity Analysis -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-palette"></i> Analisi Identita Colore</h2>
            </div>
            <div class="section-content">
                <div class="color-analysis-grid">
                    <div>
                        <h3>Piu Giocate</h3>
                        {table_to_html(color_stats_most_played, "colorMostPlayed")}
                    </div>
                    <div>
                        <h3>Piu Vincenti</h3>
                        {table_to_html(color_stats_best_winrate, "colorBestWinrate")}
                    </div>
                </div>
                
                <!-- Chart rimosso per HTML statico senza JS -->
            </div>
        </div>
        
        <!-- Head-to-Head -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-swords"></i> Scontri Diretti</h2>
            </div>
            <div class="section-content">
                <div class="insight-box">
                    <h4><i class="fas fa-balance-scale"></i> Rivalry Tracker</h4>
                    <p>Analizza le rivalita piu interessanti del gruppo. 
                    Un alto squilibrio puo indicare matchup favorevoli o skill gap.</p>
                </div>
                {table_to_html(head_to_head, "headToHead")}
            </div>
        </div>
        
        <!-- Victory Streaks -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-fire"></i> Serie Vittorie</h2>
            </div>
            <div class="section-content">
                {table_to_html(victory_streak, "victoryStreak")}
            </div>
        </div>
        
        <!-- Commander Deep Dive -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-crown"></i> Analisi Comandanti</h2>
            </div>
            <div class="section-content">
                {player_commander_sections}
                
                <div class="grid-2">
                    <div>
                        <h3>Top Comandanti per Partite</h3>
                        {table_to_html(top_commanders_played, "topCommandersPlayed")}
                    </div>
                    <div>
                        <h3>Top Comandanti per Vittorie</h3>
                        {table_to_html(top_commanders_winrate, "topCommandersWinrate")}
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Player vs Others -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-users"></i> Statistiche Giocatore vs Avversario</h2>
            </div>
            <div class="section-content">
                <details>
                    <summary><strong>Mostra/Nascondi Tabella Giocatore vs Avversario</strong></summary>
                    {table_to_html(player_vs_others, "playerVsOthers")}
                </details>
            </div>
        </div>

        <!-- Andamento Winrate per Giocatore (statico, per-player) -->
        <div class="section">
            <div class="section-header">
                <h2><i class="fas fa-chart-line"></i> Andamento Winrate per Giocatore</h2>
            </div>
            <div class="section-content">
                <details>
                    <summary><strong>Seleziona Giocatore</strong> (clicca per espandere)</summary>
                    {''.join([f'<details><summary><strong>{player}</strong></summary>' + winrate_svg(player_winrate_over_time.get(player, [])) + '</details>' for player in player_list])}
                </details>
            </div>
        </div>
    </div>
</body>
</html>'''
    
    # Scrivi il file
    with open("edh_report.html", "w", encoding="utf-8") as report_file:
        report_file.write(html_content)
    
    print("Report HTML generato con successo: edh_report.html")

# Sostituisci la funzione nel tuo mtg12.py
# Sostituisci la funzione esistente nel tuo mtg12.py
# generate_html_report = generate_enhanced_html_report
# === Statistiche dal DB ===
# Funzione generate_report aggiornata per il nuovo template
def generate_report():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_games = pd.read_sql_query("SELECT COUNT(DISTINCT game_id) AS total FROM matches;", conn).iloc[0]["total"]

    player_list = pd.read_sql("SELECT name FROM players ORDER BY name", conn)['name'].tolist()
    player_winrate_over_time = {}
    for player in player_list:
        df = pd.read_sql_query("""
            SELECT strftime('%Y-%m', m.date) AS data,
                   ROUND(SUM(m.win)*100.0 / COUNT(*), 2) AS winrate
            FROM matches m
            JOIN players p ON p.id = m.player_id
            WHERE p.name = ?
            GROUP BY data
            ORDER BY data
        """, conn, params=(player,))
        player_winrate_over_time[player] = df

    player_commander_stats = {}
    for player in player_list:
        df = pd.read_sql_query("""
            SELECT c.name AS Comandante,
                   c.color_identity AS Colori,
                   c.cmc AS CMC,
                   COUNT(m.id) AS Partite,
                   SUM(m.win) AS Vittorie
            FROM matches m
            JOIN players p ON p.id = m.player_id
            JOIN commanders c ON c.id = m.commander_id
            WHERE p.name = ?
            GROUP BY c.name
            ORDER BY Partite DESC, Vittorie DESC
        """, conn, params=(player,))
        player_commander_stats[player] = df

    player_stats = pd.read_sql_query("""
            SELECT p.name AS Giocatore,
                   COUNT(m.id) AS Partite,
                   SUM(m.win) AS Vittorie,
                   ROUND(SUM(m.win) * 100.0 / COUNT(m.id), 2) AS "Winrate (%)"
            FROM players p
            LEFT JOIN matches m ON m.player_id = p.id
            GROUP BY p.name
            HAVING COUNT(m.id) >= 20
            ORDER BY "Winrate (%)" DESC, Vittorie DESC
        """, conn)

    # Color stats queries (same as before)
    color_stats_most_played = pd.read_sql_query("""
        SELECT
          c.color_identity,
          COUNT(*) AS total_games,
          SUM(m.win) AS total_wins,
          ROUND(SUM(m.win) * 1.0 / COUNT(*) * 100, 2) AS win_rate
        FROM matches m
        JOIN commanders c ON m.commander_id = c.id
        GROUP BY c.color_identity
        HAVING COUNT(*) >= 5
        ORDER BY total_games DESC
        LIMIT 5;
    """, conn)

    color_stats_best_winrate = pd.read_sql_query("""
        SELECT
          c.color_identity,
          COUNT(*) AS total_games,
          SUM(m.win) AS total_wins,
          ROUND(SUM(m.win) * 1.0 / COUNT(*) * 100, 2) AS win_rate
        FROM matches m
        JOIN commanders c ON m.commander_id = c.id
        GROUP BY c.color_identity
        HAVING COUNT(*) >= 5
        ORDER BY win_rate DESC, total_games DESC
        LIMIT 5;
    """, conn)

    # Mana symbols and color processing (same as before)
    mana_symbols = {'W': '⚪', 'U': '🔵', 'B': '⚫', 'R': '🔴', 'G': '🟢'}
    mana_order = ['W', 'U', 'B', 'R', 'G']

    def convert_identity_to_icons(identity):
        if not identity:
            return ''
        ordered = [c for c in mana_order if c in identity]
        return ''.join(mana_symbols.get(c, c) for c in ordered)
    
    def convert_identity_to_name(identity):
        if not identity:
            return 'Colorless'
        key = ''.join([c for c in mana_order if c in identity])
        return color_names.get(key, key)

    # Add color columns
    for df in [color_stats_most_played, color_stats_best_winrate]:
        # Gestisci valori nulli in color_identity (es. comandanti non ancora arricchiti)
        df["color_identity"] = df["color_identity"].fillna('').apply(
            lambda cid: ''.join([c for c in mana_order if c in cid])
        )
        df["color_visual"] = df["color_identity"].apply(convert_identity_to_icons)
        df["color_name"] = df["color_identity"].apply(convert_identity_to_name)

    # Victory streak query (same as before)
    victory_streak = pd.read_sql_query("""
        WITH sorted_matches AS (
          SELECT
            m.id,
            m.player_id,
            m.date,
            m.commander_id,
            m.win,
            ROW_NUMBER() OVER (PARTITION BY m.player_id ORDER BY m.date) AS rn_all,
            ROW_NUMBER() OVER (PARTITION BY m.player_id, m.win ORDER BY m.date) AS rn_by_win
          FROM matches m
        ),
        win_streaks_raw AS (
          SELECT
            *,
            rn_all - rn_by_win AS grp
          FROM sorted_matches
          WHERE win = 1
        ),
        win_streaks_base AS (
          SELECT
            player_id,
            grp,
            COUNT(*) AS streak_length,
            MIN(date) AS streak_start,
            MAX(date) AS streak_end
          FROM win_streaks_raw
          GROUP BY player_id, grp
          HAVING COUNT(*) >= 2
        ),
        commander_wins_per_streak AS (
          SELECT
            w.player_id,
            w.grp,
            c.name AS commander_name,
            COUNT(*) AS wins_with_commander
          FROM win_streaks_raw w
          JOIN commanders c ON w.commander_id = c.id
          GROUP BY w.player_id, w.grp, w.commander_id
        ),
        commander_summary AS (
          SELECT
            player_id,
            grp,
            GROUP_CONCAT(commander_name || ' (' || wins_with_commander || ')') AS commanders_used
          FROM commander_wins_per_streak
          GROUP BY player_id, grp
        )
        SELECT
          p.name AS player,
          ws.streak_length,
          ws.streak_start,
          ws.streak_end,
          cs.commanders_used
        FROM win_streaks_base ws
        JOIN commander_summary cs ON ws.player_id = cs.player_id AND ws.grp = cs.grp
        JOIN players p ON ws.player_id = p.id
        ORDER BY streak_length DESC, streak_start ASC
        LIMIT 5;
    """, conn)

    commander_stats = pd.read_sql_query("""
        SELECT c.name AS Comandante,
               COUNT(m.id) AS Partite,
               SUM(m.win) AS Vittorie
        FROM commanders c
        JOIN matches m ON m.commander_id = c.id
        GROUP BY c.name
        HAVING COUNT(m.id) >= 5
        ORDER BY Vittorie DESC, Partite DESC
    """, conn)

    player_vs_others = pd.read_sql_query("""
        WITH match_data AS (
            SELECT m1.player_id AS player_id,
                   p1.name AS Giocatore,
                   m2.player_id AS opponent_id,
                   p2.name AS Avversario,
                   m1.win AS Vittorie
            FROM matches m1
            JOIN matches m2 ON m1.game_id = m2.game_id AND m1.player_id != m2.player_id
            JOIN players p1 ON m1.player_id = p1.id
            JOIN players p2 ON m2.player_id = p2.id
        )
        SELECT Giocatore,
               Avversario,
               COUNT(*) AS Partite,
               SUM(Vittorie) AS Vittorie,
               ROUND(SUM(Vittorie)*100.0 / COUNT(*), 2) AS "Winrate (%)"
        FROM match_data
        GROUP BY Giocatore, Avversario
        ORDER BY Giocatore, "Winrate (%)" DESC
    """, conn)

    # Additional metrics
    cmc_medio_totale = pd.read_sql_query(
        """
        SELECT ROUND(AVG(c.cmc), 2) AS cmc_medio
        FROM matches m
        JOIN commanders c ON m.commander_id = c.id
        """,
        conn
    ).iloc[0]["cmc_medio"]
    num_players = len(player_list)
    num_commanders = pd.read_sql_query("SELECT COUNT(DISTINCT commander_id) AS n FROM matches", conn).iloc[0]["n"]

    top_commanders_played = pd.read_sql_query("""
        SELECT c.name AS Comandante,
               COUNT(m.id) AS Partite
        FROM commanders c
        JOIN matches m ON c.id = m.commander_id
        GROUP BY c.name
        ORDER BY Partite DESC
        LIMIT 5
    """, conn)

    top_commanders_winrate = pd.read_sql_query("""
        SELECT c.name AS Comandante,
               COUNT(m.id) AS Partite,
               SUM(m.win) AS Vittorie
        FROM commanders c
        JOIN matches m ON c.id = m.commander_id
        GROUP BY c.name
        HAVING COUNT(m.id) >= 5
        ORDER BY Vittorie DESC, Partite DESC
        LIMIT 10
    """, conn)

    # Season commanders statistics
    special_commanders = [
        'Beluna Grandsquall // Seek Thrills',
        'Gimbal, Gremlin Prodigy',
        'Isu the Abominable',
        'Licia, Sanguine Tribune',
        'Lynde, Cheerful Tormentor',
        'Mr. House, President and CEO',
        'Obeka, Brute Chronologist',
        'Pramikon, Sky Rampart',
        'Rienne, Angel of Rebirth',
        'Sigurd, Jarl of Ravensthorpe',
        "Sin, Spira's Punishment",
        'Sophia, Dogged Detective',
        'Sydri, Galvanic Genius',
        'Tatsunari, Toad Rider',
        'The Celestial Toymaker',
        'Xira, the Golden Sting',
        'Yurlok of Scorch Thrash',
        'Zedruu the Greathearted'
    ]
    season_start = '2025-10-01'
    season_end = '2026-01-01'
    
    # Season commanders statistics - include ALL season commanders, even unplayed ones
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS temp_season_commanders")
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_season_commanders (
            name TEXT PRIMARY KEY,
            original_name TEXT
        )
    """)
    
    # Insert all season commanders (both existing and non-existing)
    for commander in special_commanders:
        # Usa sempre la normalizzazione per consistenza
        normalized_name = normalize_commander_name(commander)
        cursor.execute("INSERT OR IGNORE INTO temp_season_commanders (name, original_name) VALUES (?, ?)", 
                      (normalized_name, commander))
    
    season_commanders_query = f"""
        SELECT tsc.original_name AS Comandante,
               COALESCE(stats.Partite, 0) AS Partite,
               COALESCE(stats.Vittorie, 0) AS Vittorie,
               COALESCE(stats.Winrate, 0.0) AS "Winrate (%)"
        FROM temp_season_commanders tsc
        LEFT JOIN (
            SELECT LOWER(TRIM(REPLACE(REPLACE(c.name, '.', ''), '  ', ' '))) as normalized_name,
                   COUNT(*) AS Partite,
                   SUM(m.win) AS Vittorie,
                   ROUND(SUM(m.win) * 100.0 / COUNT(*), 2) AS Winrate
            FROM matches m
            JOIN commanders c ON m.commander_id = c.id
            WHERE m.date >= ? AND m.date < ?
            GROUP BY normalized_name
        ) stats ON LOWER(TRIM(tsc.name)) = stats.normalized_name
        ORDER BY Partite DESC, Vittorie DESC, Comandante ASC
    """
    season_commanders_stats = pd.read_sql_query(season_commanders_query, conn, params=[season_start, season_end])

    # Usa la nuova funzione con connessione passata
    generate_enhanced_html_report(
        player_winrate_over_time,
        player_commander_stats,
        color_stats_most_played,
        color_stats_best_winrate,
        player_stats,
        victory_streak,
        commander_stats,
        player_vs_others,
        player_list,
        cmc_medio_totale,
        num_players,
        num_commanders,
        top_commanders_played,
        top_commanders_winrate,
        total_games,
        season_commanders_stats,
        conn  # Aggiunto parametro connessione
    )

    conn.close()


def main():

    create_tables()

    parser = argparse.ArgumentParser(description="Gestione Partite Magic EDH")
    subparsers = parser.add_subparsers(dest="command")

    # Comando per registrare una partita in modalitÃ  interattiva
    record_parser = subparsers.add_parser("record", help="Registra una partita in modalitÃ  interattiva")

    # Comando per visualizzare la dashboard delle statistiche
    dashboard_parser = subparsers.add_parser("dashboard", help="Mostra la dashboard delle statistiche")

    # Comando per il caricamento in blocco di partite da un file
    bulk_upload_parser = subparsers.add_parser("bulk_upload", help="Carica partite da file")
    bulk_upload_parser.add_argument("filename", type=str, help="Nome del file per il caricamento in blocco")

    # Comando per generare il report delle statistiche in formato HTML
    report_parser = subparsers.add_parser("generate_report", help="Genera un report delle statistiche in formato HTML")

    # Parsing degli argomenti
    args = parser.parse_args()

    # Eseguiamo il comando selezionato
    if args.command == "record":
        interactive_record()
    elif args.command == "dashboard":
        show_dashboard()
    elif args.command == "bulk_upload":
        bulk_upload_matches(args.filename)
    elif args.command == "generate_report":
        generate_report()
    else:
        parser.print_help()



if __name__ == "__main__":
    main()
