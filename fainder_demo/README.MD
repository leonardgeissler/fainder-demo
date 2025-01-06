Installation:

add kaggle.json to ~/.kaggle/ folder for downloading data from kaggle


Download example data from kaggle

```bash
python fainder/setupKaggleData.py
```

Run tests for the grammar:
```bash
pytest fainder/grammar_tests
```

Start fastapi dev server
```bash
fastapi dev server.py
```

Production:
```bash
fastapi run server.py
```
