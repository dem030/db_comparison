const query1 = db.directors.aggregate([
    //funzionante
    // Lookup per ottenere il documento di persons
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
    
    // Lookup per ottenere il documento di companies
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
    
    // Filtra i documenti in cui la nazionalità della persona è diversa dalla nazione della sede centrale della compagnia
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
    
    // Proiezione finale
    {
      "$project": {
        "_id": 0,
        "Full_name": { $concat: ["$person.first_name", " ", "$person.last_name"] },
        "Company": "$company.company_name",
        "Join_date": "$join_date",
        "DirectorNationality": "$person.nationality",
        "CompanyNationality": "$company.HQ_country"
      }
    }
  ]);
  
  //indici prima query
  db.persons.createIndex({ pers_id: 1 });
  db.companies.createIndex({ comp_id: 1 });
  db.directors.createIndex({ pers_id: 1 });
  db.directors.createIndex({ comp_id: 1 });
  db.persons.createIndex({ nationality: 1 });
  db.companies.createIndex({ HQ_country: 1 });
  db.shares.createIndex({ owner_id: 1 });
  db.shares.createIndex({ comp_id: 1 });
  



  const query = db.directors.aggregate([
    
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
    
    // Lookup per ottenere il documento di shares
    {
        "$lookup": {
            "from": "shares",
            "let": { "persId": "$directors.pers_id" },
            "pipeline": [
                { 
                    "$match": { 
                        "$expr": { 
                            "$eq": [ "$owner_id", "$$persId" ] 
                        } 
                    } 
                }
            ],
            "as": "share"
        }
    },
    { "$unwind": "$share" },
    
    // Lookup per ottenere il documento di companies legati a shares
    {
        "$lookup": {
            "from": "companies",
            "let": { "compId": "$share.comp_id" },
            "pipeline": [
                { 
                    "$match": { 
                        "$expr": { 
                            "$eq": [ "$companies.comp_id", "$$compId" ] 
                        } 
                    } 
                }
            ],
            "as": "ownedCompany"
        }
    },
    { "$unwind": "$ownedCompany" },
    
    // Filtra i documenti in cui la nazionalità della persona è diversa dalla nazione della sede centrale della compagnia
    {
        "$match": {
            "$expr": {
                "$and": [
                    { "$ne": ["$person.nationality", "$company.HQ_country"] },
                    { "$ne": ["$company.HQ_country", "$ownedCompany.HQ_country"] }
                ]
            }
        }
    },
    
    // Proiezione finale
    {
        "$project": {
            "_id": 0,
            "Full_name": { "$concat": ["$person.first_name", " ", "$person.last_name"] },
           " Company": "$company.company_name",
            "Join_date": "$join_date",
            "DirectorNationality": "$person.nationality",
            "CompanyNationality": "$company.HQ_country",
            "Percentage": "$share.percentage",
            "owned_companyName": "$ownedCompany.company_name",
            "share_id": "$share.share_id"
        }
    }
]);



// query 3

const query3 = db.shares.aggregate([
    // 1. Lookup per aggiungere le informazioni della persona (owner) b"as"andosi su owner_id
    {
      "$lookup": {
        "from": "persons",
        "localField": "owner_id",
        "foreignField": "pers_id",
        "as": "ownerInfo"
      }
    },
    // Dato che ci "as"pettiamo un solo documento per ogni persona, facciamo l'unwind
    { 
      "$unwind": "$ownerInfo" 
    },
    // 2. Raggruppiamo per azienda e per persona (così sommiamo la quota per ogni proprietario in una data azienda)
    {
     " $group": {
        "_id": {
          "comp_id": "$comp_id",
         " owner": "$ownerInfo"   // tutto il documento della persona
        },
       " ownerPercentage": { "$sum": "$percentage" }
      }
    },
    // 3. Raggruppiamo per azienda, accumulando la somma totale e l'elenco delle persone
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
    // 4. Ordiniamo le aziende per totalPercentage decrescente e limitiamo a 10 risultati
    { 
      "$sort": { "totalPercentage": -1 } 
    },
    { 
      "$limit": 10 
    },
    // 5. Per ogni azienda, ordiniamo le persone per quota in ordine decrescente
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
    // Opzionale: per una formattazione più chiara
    {
      "$project": {
        "comp_id": "$_id",
        "totalPercentage": 1,
        "persons": 1,
        "_id": 0
      }
    }
  ]);
  
  


// query 4

const query4 = db.shares.aggregate([
  // 1. Lookup per aggiungere i dati della persona (proprietario)
  {
    "$lookup": {
      "from": "persons",
      "localField": "owner_id",
      "foreignField": "pers_id",
      "as": "ownerInfo",
    },
  },
  { "$unwind": "$ownerInfo" },

  // 2. Raggruppiamo per azienda e per proprietario per sommare le percentuali possedute
  {
   "$group": {
      "_id": {
        "comp_id": "$comp_id",
        "owner": "$ownerInfo", // l'intero documento della persona
      },
      "ownerPercentage": { "$sum": "$percentage" },
    },
  },

  // 3. Raggruppiamo per azienda, accumulando la quota totale e raccogliendo i proprietari in un array
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

  // 4. Ordiniamo le aziende in b"as"e al totale della quota posseduta in ordine decrescente e limitiamo a 10 risultati
  {" $sort": { "totalPercentage": -1 } },
  { "$limit": 10 },

  // 5. Lookup per ottenere gli account "as"sociati a ci"as"cuna azienda
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

  // 6. Estraiamo, dall'array degli account, solo gli identificativi degli account
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

  // 7. Lookup per ottenere le 5 transazioni più grandi effettuate da questi account
  {
    "$lookup": {
      "from": "transactions",
      "let": { "accountIds": "$companyAccountIds" },
      "pipeline": [
        // Seleziona le transazioni in cui il campo "source_account_id" è presente nell'array di account
        {
          "$match": {
            "$expr": { "$in": ["$sender_id", "$$accountIds"] },
          },
        },
        // Ordina le transazioni in ordine decrescente per importo
        { "$sort": { "amount": -1 } },
        // Limita il risultato alle 5 transazioni più grandi
        { "$limit": 5 },
      ],
      "as": "topTransactions",
    },
  },

  // 8. Proiezione finale: restituisce l'identificativo dell'azienda, la quota totale,
  //    l'elenco dei proprietari, gli account "as"sociati e le 5 transazioni più grandi
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
]);

export const queries = [query1, query2, query3, query4]



  


