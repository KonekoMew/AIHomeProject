from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config import load_worldbook, save_worldbook
from chatroom import load_chatroom_config, save_chatroom_config

from persona_evolution import (
    delete_connor_evolution_run,
    delete_main_ai_evolution_run,
    list_connor_evolution_runs,
    list_main_ai_evolution_runs,
    run_connor_persona_evolution,
    run_main_ai_persona_evolution,
)


router = APIRouter(prefix="/api/persona-evolution", tags=["persona-evolution"])


class MainAiRunTestRequest(BaseModel):
    date: Optional[str] = None


class MainAiConfigUpdate(BaseModel):
    enabled: bool


class ConnorRunTestRequest(BaseModel):
    date: Optional[str] = None


class ConnorConfigUpdate(BaseModel):
    enabled: bool


@router.post("/main-ai/run-test")
async def run_main_ai_test(body: Optional[MainAiRunTestRequest] = None):
    try:
        return await run_main_ai_persona_evolution(
            trigger="manual",
            window_mode="current",
            window_date=(body.date if body else None),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/main-ai/config")
async def get_main_ai_config():
    wb = load_worldbook()
    return {"enabled": bool(wb.get("persona_evolution_enabled"))}


@router.put("/main-ai/config")
async def update_main_ai_config(body: MainAiConfigUpdate):
    wb = load_worldbook()
    wb["persona_evolution_enabled"] = body.enabled
    save_worldbook(wb)
    return {"ok": True, "enabled": body.enabled}


@router.get("/main-ai/runs")
async def list_main_ai_runs(limit: int = Query(10, ge=1, le=50)):
    return {"runs": await list_main_ai_evolution_runs(limit)}


@router.delete("/main-ai/runs/{run_id}")
async def delete_main_ai_run(run_id: str):
    return await delete_main_ai_evolution_run(run_id)


@router.post("/connor/run-test")
async def run_connor_test(body: Optional[ConnorRunTestRequest] = None):
    try:
        return await run_connor_persona_evolution(
            trigger="manual",
            window_mode="current",
            window_date=(body.date if body else None),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/connor/config")
async def get_connor_config():
    cfg = load_chatroom_config()
    return {"enabled": bool(cfg.get("connor_persona_evolution_enabled"))}


@router.put("/connor/config")
async def update_connor_config(body: ConnorConfigUpdate):
    cfg = load_chatroom_config()
    cfg["connor_persona_evolution_enabled"] = body.enabled
    save_chatroom_config(cfg)
    return {"ok": True, "enabled": body.enabled}


@router.get("/connor/runs")
async def list_connor_runs(limit: int = Query(10, ge=1, le=50)):
    return {"runs": await list_connor_evolution_runs(limit)}


@router.delete("/connor/runs/{run_id}")
async def delete_connor_run(run_id: str):
    return await delete_connor_evolution_run(run_id)
