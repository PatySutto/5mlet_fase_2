import requests
import base64
import json
import pandas as pd

# 1. Monta o payload como JSON e codifica em Base64
payload = {
    "language": "pt-br",
    "pageNumber": 1,
    "pageSize": 200,
    "index": "IBOV",
    "segment": "1"
}
payload_b64 = base64.b64encode(json.dumps(payload).encode("utf-8")).decode()

# 2. URL do endpoint
url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{payload_b64}"

# 3. Faz a requisição
response = requests.get(url)
response.raise_for_status()
data = response.json()

# 4. Extrai e converte os dados
df = pd.DataFrame(data["results"])
print(df.head())
df.to_csv("ibov_composicao.csv", index=False)
