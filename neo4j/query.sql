LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/luisfernandosolis/openai-api/main/data_final_definitivo.csv' AS row
MERGE (e:News {id: row.id})
SET e.content = row.content,
    e.title = row.title,
    e.first_sourceurl = row.first_sourceurl,
    e.embedding = row.ml_generate_embedding_result,
    e.date = CASE
                WHEN row.DATE IS NULL OR row.DATE = '' THEN ''
                ELSE substring(row.DATE, 6, 2) + '-' + substring(row.DATE, 4, 2) + '-' + substring(row.DATE, 0, 4)
             END,
    e.tone = row.TONE

// Crear nodos de Localización y relaciones
FOREACH (location IN split(row.LOCATIONS, ';') |
    MERGE (l:Location {name: split(location, '#')[1], country: split(location, '#')[2]})
    MERGE (e)-[:OCCURRED_AT]->(l)
)

// Crear nodos de Persona y relaciones
FOREACH (person IN split(row.PERSONS, ';') |
    MERGE (p:Person {name: person})
    MERGE (e)-[:INVOLVES]->(p)
)

// Crear nodos de Organización y relaciones
FOREACH (org IN split(row.ORGANIZATIONS, ';') |
    MERGE (o:Organization {name: org})
    MERGE (e)-[:INVOLVES_ORGANIZATION]->(o)
)

// Crear nodos de Temas y relaciones
FOREACH (theme IN split(row.THEMES, ';') |
    MERGE (t:Theme {name: theme})
    MERGE (e)-[:HAS_THEME]->(t)
)

// Crear nodos de Fuente y relaciones
MERGE (s:Source {name: row.SOURCES, url: row.SOURCEURLS})
MERGE (e)-[:REPORTED_BY]->(s)