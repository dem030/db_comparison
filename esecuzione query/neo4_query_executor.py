import time
import csv
from neo4j import GraphDatabase

# Lista delle query
query_list = [
    """MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)
WHERE p.nationality <> c.HQ_country
RETURN 
  p.first_name + " " + p.last_name AS Full_name,
  c.company_name AS Company,
  d.join_date AS Join_date,
  p.nationality AS DirectorNationality,
  c.HQ_country AS CompanyNationality;"""
  ,
  """
MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)-[:CHAS_ACCOUNT]->(a:Account)
WHERE p.nationality <> a.bank_country
RETURN 
  p.first_name + " " + p.last_name AS Full_name,
  c.company_name AS Company,
  d.join_date AS Join_date,
  p.nationality AS DirectorNationality,
  c.HQ_country AS CompanyNationality,
  a.bank_country AS BankCountry;
 """,
 """
MATCH (p:Person)-[:OWNS]->(s:Share)-[:IS_PART]->(c:Company)
WITH c, p, sum(s.percentage) AS ownerPercentage
WITH c, 
     collect({ownerName: p.first_name + " " + p.last_name, percentage: ownerPercentage}) AS persons,
     sum(ownerPercentage) AS totalPercentage
ORDER BY totalPercentage DESC
LIMIT 10
UNWIND persons AS personData
WITH c, totalPercentage, personData
ORDER BY personData.percentage DESC
WITH c, totalPercentage, collect(personData) AS sortedPersons
RETURN 
  c.comp_id AS comp_id, 
  totalPercentage, 
  sortedPersons AS persons;

""",
"""
MATCH (p:Person)-[:OWNS]->(s:Share)-[:IS_PART]->(c:Company)
WITH p, s, c
MATCH (c)-[:CHAS_ACCOUNT]->(a:Account)
WITH p, s, c, a
MATCH (a)-[:SENT]->(t:Transaction)
WITH p, s, c, a, t
ORDER BY t.amount DESC
MATCH (t)-[:RECEIVED]->(r:Account)
WITH p, s, c, a, t, r
MATCH (dir:Person)-[:DIRECTOR_OF]->(c)
WITH p, s, c, a, t, r, dir
RETURN 
  c.comp_id AS comp_id,
  p.pers_id AS person_id,
  p.first_name AS person_first_name,
  p.last_name AS person_last_name,
  p.birthdate AS person_birthdate,
  p.nationality AS person_nationality,
  s.share_id AS share_id,
  s.percentage AS share_percentage,
  a.acc_id AS account_id,
  a.card_code AS account_card_code,
  a.balance AS account_balance,
  a.bank_country AS account_bank_country,
  t.trans_id AS transaction_id,
  t.amount AS transaction_amount,
  t.transaction_date AS transaction_date,
  r.acc_id AS receiver_account_id,
  dir.pers_id AS director_id,
  dir.first_name AS director_first_name,
  dir.last_name AS director_last_name
ORDER BY comp_id, person_id, account_id, transaction_id, director_id;

"""
]

uri = "neo4j://localhost:7687"
auth_credentials = ("neo4j", "password")

# Funzione per eseguire una query e registrare il tempo di esecuzione
def execute_query(query, query_num):
    filename = f"query_{query_num}_timer.csv"
    times = []

    for i in range(31):
        print(f"Esecuzione {i + 1} per la query {query_num}")
        driver = GraphDatabase.driver(uri, auth=auth_credentials)
        try:
            with driver.session() as session:
                start_time = time.perf_counter()
                session.run(query)
                end_time = time.perf_counter()
                execution_time = (end_time - start_time) * 1000
                times.append(execution_time)
        except Exception as e:
            print(f"Errore durante l'esecuzione della query {query_num}: {e}")
            times.append(float('nan'))  # Registra il fallimento con NaN
        finally:
            # Chiudi la connessione dopo ogni esecuzione
            driver.close()

    # Salva i tempi di esecuzione in un file CSV
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Run", "Execution Time"])
        for i, exec_time in enumerate(times):
            writer.writerow([i + 1, exec_time])

        # Calcola e salva la media delle esecuzioni (escludendo NaN e la prima esecuzione)
        valid_times = [t for t in times[1:] if not (t != t)]  # Esclude NaN
        if valid_times:
            average_time = sum(valid_times) / len(valid_times)
            writer.writerow(["Media", average_time])
        else:
            writer.writerow(["Media", "N/A"])

# Esegui le query una alla volta
for i, query in enumerate(query_list):
    execute_query(query, i + 1)

print("Script completato. I tempi di esecuzione sono stati salvati nei file CSV.")
