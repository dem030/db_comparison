// Creazione dei vincoli di unicità con la sintassi aggiornata


// Trovare direttori con nazionalità diversa dalla sede centrale dell'azienda
MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)
WHERE p.nationality <> c.HQ_country
RETURN 
  p.first_name + " " + p.last_name AS Full_name,
  c.company_name AS Company,
  d.join_date AS Join_date,
  p.nationality AS DirectorNationality,
  c.HQ_country AS CompanyNationality;

// Trovare direttori con nazionalità diversa dalla banca delle aziende
MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)
MATCH (p)-[o:OWNS]->(s:Share)-[:IS_PART]->(c2:Company)
WHERE p.nationality <> c.HQ_country and c.HQ_country <> c2.HQ_country
RETURN 
  p.first_name + " " + p.last_name AS Full_name,
  c.company_name AS Company,
  d.join_date AS Join_date,
  p.nationality AS DirectorNationality,
  c.HQ_country AS CompanyNationality,
  sum(s.percentage) as Percentage,
  c2.company_name as owned_companyName,
  s.share_id as share_id;
  


  

// Raggruppare per azienda e persona sommandone le percentuali
MATCH (p:Person)-[:OWNS]->(s:Share)-[:IS_PART]->(c:Company)
WITH c, p, sum(s.percentage) AS ownerPercentage
WITH c, 
     collect({ownerName: p.first_name + " " + p.last_name, percentage: ownerPercentage}) AS persons,
     sum(ownerPercentage) AS totalPercentage
ORDER BY totalPercentage DESC
UNWIND persons AS personData
WITH c, totalPercentage, personData
ORDER BY personData.percentage DESC
WITH c, totalPercentage, collect(personData) AS sortedPersons
RETURN 
  c.comp_id AS comp_id, 
  totalPercentage, 
  sortedPersons AS persons;



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
