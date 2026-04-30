from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict

app = FastAPI()

# In-memory registry mapping service names to lists of instance URLs
registry: Dict[str, List[str]] = {"logging-service": [], "counter-service": []}


class ServiceRegistration(BaseModel):
    name: str
    url: str


@app.post("/register")
def register_service(info: ServiceRegistration):
    if info.name not in registry:
        registry[info.name] = []
    if info.url not in registry[info.name]:
        registry[info.name].append(info.url)
    print(f"[CONFIG] Registered instance {info.url} for {info.name}", flush=True)
    return {"status": "registered", "url": info.url}


@app.get("/services/{name}")
def get_service_instances(name: str):
    return {"instances": registry.get(name, [])}


@app.get("/health")
def health_check():
    return {"status": "ok", "registry_size": len(registry)}
