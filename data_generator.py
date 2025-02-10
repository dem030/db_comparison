import csv
import random
from faker import Faker
import threading
from tqdm import tqdm
import pandas as pd
import os
from datetime import datetime, timedelta
from faker.providers.date_time import Provider as DateTimeProvider

# Configurazione Faker
fake = Faker()
DateTimeProvider.DEFAULT_TZINFO = None  # Disabilita i problemi legati al fuso orario

# Costanti
NUM_PERSONS = 60000
NUM_COMPANIES = 25000
NUM_ACCOUNTS = 75000
NUM_DIRECTORS = 50000
NUM_TRANSACTIONS = 150000
NUM_SHAREHOLDERS = 100000

# Eventi per sincronizzazione
events = {
    "persons": threading.Event(),
    "companies": threading.Event(),
    "accounts": threading.Event()
}

# Funzioni di generazione dati
def generate_persons_to_csv(filename, num):
    rows = []
    for _ in tqdm(range(num), desc="Generating persons"):
        rows.append({
            "pers_id": fake.uuid4(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "birthdate": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            "nationality": fake.country()
        })
    pd.DataFrame(rows).to_csv(filename, index=False)
    events["persons"].set()

def generate_companies_to_csv(filename, num):
    rows = []
    for _ in tqdm(range(num), desc="Generating companies"):
        rows.append({
            "comp_id": fake.uuid4(),
            "company_name": fake.company(),
            "legal_form": fake.company_suffix(),
            "legal_country": fake.country(),
            "HQ_country": fake.country(),
            "founding_date": fake.date_between_dates(
                date_start=datetime(1970, 1, 1),
                date_end=datetime.now()
            ).isoformat()
        })
    pd.DataFrame(rows).to_csv(filename, index=False)
    events["companies"].set()

def generate_accounts_to_csv(filename, persons_file,companies_file, num_accounts):
    events["persons"].wait()
    persons = pd.read_csv(persons_file)
    person_ids = persons["pers_id"].tolist()
    events["companies"].wait()
    companies  = pd.read_csv(companies_file)
    companies_ids = companies["comp_id"].tolist()
    companies_countries = companies["legal_country"].tolist()
    owner_rows = []
    comp_rows = []
    randarr = [True, False]
    for _ in tqdm(range(num_accounts), desc="Generating accounts"):
        if(random.choice(randarr)):
            owner_rows.append({
                "acc_id": fake.uuid4(),
                "card_code": fake.iban(),
                "balance": round(random.uniform(1000, 1000000), 2),
                "owner_id": random.choice(person_ids),
                "bank_country": random.choice(companies_countries)
            })
        else:
            comp_rows.append({
                "acc_id": fake.uuid4(),
                "card_code": fake.iban(),
                "balance": round(random.uniform(1000, 1000000), 2),
                "comp_id": random.choice(companies_ids),
                "bank_country": random.choice(companies_countries)
            })
    pd.DataFrame(owner_rows).to_csv(filename + "_owners.csv" , index=False)
    pd.DataFrame(comp_rows).to_csv(filename + "_companies.csv", index=False)
    events["accounts"].set()

def generate_transactions_to_csv(filename, accounts_file_owners,accounts_file_comps, num_transactions):
    events["accounts"].wait()
    accounts_owners = pd.read_csv(accounts_file_owners)
    account_ids = accounts_owners["acc_id"].tolist()
    accounts_comps = pd.read_csv(accounts_file_comps)
    account_ids += accounts_comps["acc_id"].tolist()
    # Verifica che ci siano almeno due account disponibili
    if len(account_ids) < 2:
        print("Errore: Non ci sono abbastanza account per generare le transazioni.")
        return

    rows = []
    for _ in tqdm(range(num_transactions), desc="Generating transactions"):
        try:
            sender, receiver = random.sample(account_ids, 2)
            transaction_date = fake.date_between_dates(
                date_start=datetime(2000, 1, 1),
                date_end=datetime.now()
            ).isoformat()

            rows.append({
                "trans_id": fake.uuid4(),
                "sender_id": sender,
                "receiver_id": receiver,
                "amount": round(random.uniform(10, 900000), 2),
                "transaction_date": transaction_date
            })
        except ValueError as ve:
            print(f"Errore durante la selezione degli account: {ve}")
        except Exception as e:
            print(f"Errore durante la generazione di una transazione: {e}")

    # Scrivi solo se ci sono righe valide
    if rows:
        pd.DataFrame(rows).to_csv(filename, index=False)
    else:
        print("Nessuna transazione generata.")

def generate_directors_to_csv(filename, persons_file, companies_file, num_directors):
    events["persons"].wait()
    events["companies"].wait()
    
    persons = pd.read_csv(persons_file)
    person_ids = persons["pers_id"].tolist()

    companies = pd.read_csv(companies_file)
    company_ids = companies["comp_id"].tolist()

    rows = []
    for _ in tqdm(range(num_directors), desc="Generating directors"):
        rows.append({
            "pers_id": random.choice(person_ids),
            "comp_id": random.choice(company_ids),
            "join_date": (datetime.now() - timedelta(days=random.randint(0, 365 * 5))).strftime('%Y-%m-%d')
        })
    pd.DataFrame(rows).to_csv(filename, index=False)

def generate_shares_to_csv(filename, companies_file, persons_file, num_shareholders):
    events["persons"].wait()
    events["companies"].wait()

    persons = pd.read_csv(persons_file)
    companies = pd.read_csv(companies_file)

    person_ids = persons["pers_id"].tolist()
    company_ids = companies["comp_id"].tolist()

    rows = []
    percentages = {}
    for _ in tqdm(range(num_shareholders), desc="Generating shares"):
        comp_id = random.choice(company_ids)
        perc = round(random.uniform(0.01, 27.5), 2)
        if comp_id not in percentages:
            percentages[comp_id] = perc
        else: 
            perc = min(perc, 100 - percentages[comp_id])
        if perc == 0:
            continue
        rows.append({
            "share_id": fake.uuid4(),
            "comp_id": comp_id,
            "owner_id": random.choice(person_ids),
            "percentage": perc
        })
        percentages[comp_id] += perc
    pd.DataFrame(rows).to_csv(filename, index=False)

# Esecuzione delle funzioni con threading
threads = [
    threading.Thread(target=generate_persons_to_csv, args=("persons.csv", NUM_PERSONS)),
    threading.Thread(target=generate_companies_to_csv, args=("companies.csv", NUM_COMPANIES)),
    threading.Thread(target=generate_accounts_to_csv, args=("accounts", "persons.csv", "companies.csv", NUM_ACCOUNTS)),
    threading.Thread(target=generate_transactions_to_csv, args=("transactions.csv", "accounts_owners.csv","accounts_companies.csv", NUM_TRANSACTIONS)),
    threading.Thread(target=generate_directors_to_csv, args=("directors.csv", "persons.csv", "companies.csv", NUM_DIRECTORS)),
    threading.Thread(target=generate_shares_to_csv, args=("shares.csv", "companies.csv", "persons.csv", NUM_SHAREHOLDERS)),
]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

print("Tutti i file sono stati generati con successo!")
