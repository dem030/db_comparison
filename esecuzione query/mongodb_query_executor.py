import time
import csv
from pymongo import MongoClient

# Dettagli di connessione a MongoDB
uri = "mongodb://localhost:27017/"

# Funzioni che definiscono le query (modificate per accettare il parametro 'db')
def query1(db):
    return  db.directors.aggregate([
    {
      "$lookup": {
        "from": "persons",
        "let": { "persId": "$directors.pers_id" },
        "pipeline": [
          { 
            "$match": { 
              "$expr": { 
                "$eq": [ "$persons.pers_id", "$$persId" ] 
              } 
            } 
          }
        ],
        "as": "person"
      }
    },
    { "$unwind": "$person" },
    {
      "$lookup": {
        "from": "companies",
        "let": { "compId": "$directors.comp_id" },
        "pipeline": [
          { 
            "$match": { 
              "$expr": { 
                "$eq": [ "$companies.comp_id", "$$compId" ] 
              } 
            } 
          }
        ],
        "as": "company"
      }
    },
    { "$unwind": "$company" },
    {
      "$match": {
        "$expr": {
          "$ne": [
            { "$ifNull": ["$person.nationality", ""] },
            { "$ifNull": ["$company.HQ_country", ""] }
          ]
        }
      }
    },
    {
      "$project": {
        "_id": 0,
        "Full_name": { "$concat": ["$person.first_name", " ", "$person.last_name"] },
        "Company": "$company.company_name",
        "Join_date": "$join_date",
        "DirectorNationality": "$person.nationality",
        "CompanyNationality": "$company.HQ_country"
      }
    }
  ])

def query2(db):
    return db.directors.aggregate([
  {
    "$lookup": {
      "from": "persons",
      "let": { "personId": "$pers_id" },
      "pipeline": [
        {
          "$match": { 
            "$expr": { "$eq": ["$pers_id", "$$personId"] }
          }
        },
        {
          "$project": { "first_name": 1, "last_name": 1, "nationality": 1 }
        }
      ],
      "as": "person"
    }
  },
  { "$unwind": "$person" },
  {
    "$lookup": {
      "from": "companies",
      "let": { "compId": "$comp_id" },
      "pipeline": [
        {
          "$match": { 
            "$expr": { "$eq": ["$comp_id", "$$compId"] }
          }
        },
        {
          "$project": { "company_name": 1, "HQ_country": 1 }
        }
      ],
      "as": "company"
    }
  },
  { "$unwind": "$company" },
  {
    "$lookup": {
      "from": "shares",
      "let": { "personId": "$pers_id" },
      "pipeline": [
        {
          "$match": { 
            "$expr": { "$eq": ["$owner_id", "$$personId"] }
          }
        },
        {
          "$project": { "share_id": 1, "percentage": 1, "comp_id": 1 }
        }
      ],
      "as": "shares"
    }
  },
  { "$unwind": "$shares" },

  
  {
    "$lookup": {
      "from": "companies",
      "let": { "shareCompanyId": "$shares.comp_id" },
      "pipeline": [
        {
          "$match": { 
            "$expr": { "$eq": ["$comp_id", "$$shareCompanyId"] }
          }
        },
        {
          "$project": { "company_name": 1, "HQ_country": 1 }
        }
      ],
      "as": "ownedCompany"
    }
  },
  { "$unwind": "$ownedCompany" },
  {
    "$match": {
      "$expr": {
        "$and": [
          { "$ne": [ "$person.nationality", "$company.HQ_country" ] },
          { "$ne": [ "$company.HQ_country", "$ownedCompany.HQ_country" ] }
        ]
      }
    }
  },
  {
    "$group": {
      "_id": {
        "full_name": { "$concat": [ "$person.first_name", " ", "$person.last_name" ] },
        "company": "$company.company_name",
        "join_date": "$join_date",
        "directorNationality": "$person.nationality",
        "companyNationality": "$company.HQ_country",
        "owned_companyName": "$ownedCompany.company_name",
        "share_id": "$shares.share_id"
      },
      "totalPercentage": { "$sum": "$shares.percentage" }
    }
  },

  
  {
    "$project": {
      "_id": 0,
      "Full_name": "$_id.full_name",
      "Company": "$_id.company",
      "Join_date": "$_id.join_date",
      "DirectorNationality": "$_id.directorNationality",
      "CompanyNationality": "$_id.companyNationality",
      "Percentage": "$totalPercentage",
      "owned_companyName": "$_id.owned_companyName",
      "share_id": "$_id.share_id"
    }
  }
])


def query3(db):
    return db.shares.aggregate([
    {
      "$lookup": {
        "from": "persons",
        "localField": "owner_id",
        "foreignField": "pers_id",
        "as": "ownerInfo"
      }
    },
    { 
      "$unwind": "$ownerInfo" 
    },
    {
     " $group": {
        "_id": {
          "comp_id": "$comp_id",
         " owner": "$ownerInfo"   
        },
       " ownerPercentage": { "$sum": "$percentage" }
      }
    },
    {
      "$group": {
        "_id": "$_id.comp_id",
        "totalPercentage": { "$sum": "$ownerPercentage" },
        "persons": { 
          "$push": { 
            "owner": "$_id.owner", 
            "percentage": "$ownerPercentage" 
          } 
        }
      }
    },
    { 
      "$sort": { "totalPercentage": -1 } 
    },
    { 
      "$unwind": "$persons" 
    },
    { 
      "$sort": { "persons.percentage": -1 } 
    },
    { 
      "$group": {
        "_id": "$_id",
        "totalPercentage": {" $first": "$totalPercentage" },
        "persons": { "$push": "$persons" }
      }
    },
    {
      "$project": {
        "comp_id": "$_id",
        "totalPercentage": 1,
        "persons": 1,
        "_id": 0
      }
    }
  ])

def query4(db):
    return db.shares.aggregate([
  {
    "$lookup": {
      "from": "persons",
     " localField": "owner_id",
      "foreignField": "pers_id",
      "as": "ownerInfo",
    },
  },
  { "$unwind": "$ownerInfo" },
  {
   " $group": {
      "_id": {
        "comp_id": "$comp_id",
       " owner": "$ownerInfo", 
      },
      "ownerPercentage": { "$sum": "$percentage" },
    },
  },
  {
    "$group": {
      "_id": "$_id.comp_id",
      "totalPercentage": { "$sum": "$ownerPercentage" },
      "persons": {
        "$push": {
          "owner": "$_id.owner",
          "percentage": "$ownerPercentage",
        },
      },
    },
  },
  {" $sort": { "totalPercentage": -1 } },
  {
    "$lookup": {
      "from": "Account",
      "let": { "compId": "$_id" },
      "pipeline": [
        {
          "$match": {
            "$expr": { "$eq": ["$comp_id", "$$compId"] },
          },
        },
      ],
      "as": "companyAccounts",
    },
  },
  {
    "$addFields": {
      "companyAccountIds": {
        "$map": {
          "input": "$companyAccounts",
          "as": "acc",
          "in": "$$acc.acc_id",
        },
      },
    },
  },
  {
    "$lookup": {
      "from": "transactions",
      "let": { "accountIds": "$companyAccountIds" },
      "pipeline": [
        {
          "$match": {
            "$expr": { "$in": ["$sender_id", "$$accountIds"] },
          },
        },
        { "$sort": { "amount": -1 } },
      ],
      "as": "topTransactions",
    },
  },
  {
    "$project": {
      "_id": 0,
      "comp_id": "$_id",
      "totalPercentage": 1,
      "persons": 1,
      "companyAccounts": 1,
      "topTransactions": 1,
    },
  },
])
# Lista delle query
queries = [query1, query2, query3, query4]

# Funzione per eseguire una query e misurare il tempo di esecuzione
def execute_query(query_func, query_num):
    filename = f"times_query_{query_num}.csv"
    times = []

    for i in range(31):
        print(f"Esecuzione {i + 1} per la query {query_num}")
        # Apertura di una nuova connessione per ogni iterazione
        client = MongoClient(uri)
        db = client["db2_25"]
        try:
            start_time = time.perf_counter()
            # Esecuzione della query passando l'oggetto db
            list(query_func(db))
            end_time = time.perf_counter()
            execution_time = (end_time - start_time) * 1000  # conversione in millisecondi
            times.append(execution_time)
        except Exception as e:
            print(f"Errore durante l'esecuzione della query {query_num}: {e}")
            times.append(float('nan'))
        finally:
            client.close()  # Chiusura della connessione

    # Salvataggio dei tempi di esecuzione in un file CSV
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Run", "Execution Time"])
        for run, exec_time in enumerate(times, start=1):
            writer.writerow([run, exec_time])
        # Calcola e salva la media (escludendo la prima esecuzione e gli eventuali NaN)
        valid_times = [t for t in times[1:] if not (t != t)]
        if valid_times:
            average_time = sum(valid_times) / len(valid_times)
            writer.writerow(["Media", average_time])
        else:
            writer.writerow(["Media", "N/A"])

# Esecuzione di ciascuna query e salvataggio dei tempi in file CSV separati
for i, query_func  in list(enumerate(queries, start=1)):
    execute_query(query_func, i)

print("Script completato. I tempi di esecuzione sono stati salvati nei file CSV.")
