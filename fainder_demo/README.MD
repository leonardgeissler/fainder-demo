Installation:

add kaggle.json to ~/.kaggle/ folder for downloading data from kaggle


Download example data from kaggle

```bash
python fainder_demo/setupKaggleData.py
```

Run tests for the grammar:
```bash
pytest fainder_demo/tests
```

Start fastapi dev server
```bash
fastapi dev server.py
```

Production:
```bash
fastapi run server.py
```
