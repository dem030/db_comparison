
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row
CREATE (:Person {
    pers_id: row.pers_id,
    first_name: row.first_name,
    last_name: row.last_name,
    birthdate: row.birthdate,
    nationality: row.nationality
});


LOAD CSV WITH HEADERS FROM 'file:///companies.csv' AS row
CREATE (:Company {
    comp_id: row.comp_id,
    company_name: row.company_name,
    legal_form: row.legal_form,
    legal_country: row.legal_country,
    HQ_country: row.HQ_country,
    founding_date: row.founding_date
});



LOAD CSV WITH HEADERS FROM 'file:///accounts.csv' AS row
CREATE (a:Account {
    acc_id: row.acc_id,
    card_code: row.card_code,
    balance: toFloat(row.balance),
    bank_country: row.bank_country
})
WITH a, row
MATCH (p:Person {pers_id: row.owner_id})
MATCH (c:companies {comp_id: row.comp_id})
CREATE (p)-[:HAS_ACCOUNT]->(a);
CREATE (c)-[:HAS_ACCOUNT]->(a);




LOAD CSV WITH HEADERS FROM 'file:///directors.csv' AS row
MATCH (p:Person {pers_id: row.pers_id})
MATCH (c:Company {comp_id: row.comp_id})
CREATE (p)-[:DIRECTOR_OF {join_date: row.join_date}]->(c);





LOAD CSV WITH HEADERS FROM 'file:///transactions.csv' AS row
MATCH (sender:Account {acc_id: row.sender_id})
MATCH (receiver:Account {acc_id: row.receiver_id})
CREATE (t:Transaction {
    trans_id: row.trans_id,
    amount: toFloat(row.amount),
    transaction_date: row.transaction_date
})
CREATE (sender)-[:SENT]->(t)
CREATE (t)-[:TO]->(receiver);






LOAD CSV WITH HEADERS FROM 'file:///shares.csv' AS row
MATCH (p:Person {pers_id: row.owner_id})
MATCH (c:Company {comp_id: row.comp_id})
CREATE (p)-[:SHAREHOLDER_OF {
    share_id: row.share_id,
    percentage: toFloat(row.percentage)
}]->(c);
