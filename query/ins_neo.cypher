CREATE INDEX  FOR (P:Persons) ON (P.pers_id);
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row
CREATE (:Person {
    pers_id: row.pers_id,
    first_name: row.first_name,
    last_name: row.last_name,
    birthdate: row.birthdate,
    nationality: row.nationality
});

CREATE INDEX FOR (c:Company) ON (c.comp_id);

LOAD CSV WITH HEADERS FROM 'file:///companies.csv' AS row
CREATE (:Company {
    comp_id: row.comp_id,
    company_name: row.company_name,
    legal_form: row.legal_form,
    legal_country: row.legal_country,
    HQ_country: row.HQ_country,
    founding_date: row.founding_date
});


CREATE INDEX FOR (a:Account) ON (a.acc_id);

LOAD CSV WITH HEADERS FROM 'file:///accounts_owners.csv' AS row
CREATE (a:Account {
    acc_id: row.acc_id,
    card_code: row.card_code,
    balance: toFloat(row.balance),
    bank_country: row.bank_country
})
WITH a, row
MATCH (p:Person {pers_id: row.owner_id})
MERGE (p)-[:HAS_ACCOUNT]->(a);

LOAD CSV WITH HEADERS FROM 'file:///accounts_companies.csv' AS row
CREATE (a:Account {
    acc_id: row.acc_id,
    card_code: row.card_code,
    balance: toFloat(row.balance),
    bank_country: row.bank_country
})
WITH a, row
MATCH (c:Company {comp_id: row.comp_id})
MERGE (c)-[:CHAS_ACCOUNT]->(a);


LOAD CSV WITH HEADERS FROM 'file:///transactions.csv' AS row
MATCH (s:Account {acc_id: row.sender_id})
MATCH (r:Account {acc_id: row.receiver_id})
CREATE (t:Transaction {
    trans_id: row.trans_id,
    amount: toFloat(row.amount),
    transaction_date: row.transaction_date
})
CREATE (s)-[:SENT]->(t)
CREATE (t)-[:RECEIVED]->(r);


LOAD CSV WITH HEADERS FROM 'file:///directors.csv' AS row
MATCH (p:Person {pers_id: row.pers_id})
MATCH (c:Company {comp_id: row.comp_id})
CREATE (p)-[:DIRECTOR_OF {join_date: row.join_date}]->(c);


LOAD CSV WITH HEADERS FROM 'file:///shares.csv' AS row
MATCH (p:Person {pers_id: row.owner_id})
MATCH (c:Company {comp_id: row.comp_id})
CREATE (s:Share {
    share_id: row.share_id,
    percentage: toFloat(row.percentage)
})
CREATE (p)-[:OWNS]->(s)
CREATE (s)-[:IS_PART]->(c);


CREATE INDEX FOR (n:Person) ON (n.nationality);
CREATE INDEX FOR (n:Company) ON (n.HQ_country);
CREATE INDEX FOR (n:Director) ON (n.pers_id);
CREATE INDEX FOR (n:Director) ON (n.comp_id);
