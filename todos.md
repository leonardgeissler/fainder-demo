# Todos

## Misc

- js linter/formatter in pre-commit
- java linter/formatter in pre-commit
- Find a good system name

## Fainder Backend

- Fix typing in Grammar
- Robustes error handling
  - Falsch formatierte Queries
  - Leere Ergebnisse
- Fainder pre-filter für Lucene Queries
  - Vorarbeit von Pratham nutzen
    - Prüfen, inwiefern seine Arbeit in der neuesten Lucene Version noch funktioniert
  - Allgemeine Filter-Logik für alle Operatoren implementieren
    - Mit leerem Filter starten und jeder Operator modifiziert den Filter
- migrate to FastAPI compatible logging and remove loguru
- Add semantic & syntactic similarity search to column specifiers in percentile predicates using `faiss` and `sentence-transformers`
  - <https://www.sbert.net/examples/applications/semantic-search/README.html>
  - Transformer-based language models can handle semantic and syntactic similarity
- Move grammar to a dedicated `.lark` file in `backend/grammar`
- Improve grammar for percentile predicates

### Fainder Index

- How do we handle different index types (rebinning, full precision/recall, exact) in the grammar?

## UI

- Syntax highlighting in query field
- "Load more" Button korrekt implementieren
  - Ausblenden, wenn es keine weiteren Ergebnisse gibt
  - Zusätzliche Ergebnisse on demand laden
- Better error handling
  - Show more readable error messages
- Show summary statistics for each query (number of results, execution time, etc.)

## Java

- Use Lucene to highlight matching document parts and show that in the UI
- Copilot: How do I efficiently build a Lucene index from a list of documents?
- Change paths and config in accordance with backend config

## Offline Processing

- Script that takes in Croissant files and generates a Fainder index as well as a docID -> histIDs mapping as well as the reverse
  - Options for "no-fainder" and "no-docID" mappings
  - Delete old files if they exist
