# main.py  (adstandard FastAPI - Step 2: 표준코드/표준단가 v1 + Price Engine v0)
from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, List, Literal
import sqlite3
import json
import os
import time
import traceback

from price_engine import price_quote

# ---------------------------------
# App
# ---------------------------------
app = FastAPI(title="adstandard-api", version="mvp-step2-price-engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 테스트 단계
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------
# DB
# ---------------------------------
DB_PATH = os.environ.get("ADSTANDARD_DB_PATH", "/tmp/adstandard.db")

def now_ms() -> int:
    return int(time.time() * 1000)

def new_id(prefix: str) -> str:
    return f"{prefix}{now_ms()}"

def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

def json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except:
        return None

def ok(data: Any) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data})

def fail(message: str, code: int = 400, extra: Optional[Dict[str, Any]] = None) -> JSONResponse:
    payload = {"ok": False, "error": {"message": message}}
    if extra:
        payload["error"].update(extra)
    return JSONResponse(payload, status_code=code)

def bool_to_int(x: bool) -> int:
    return 1 if x else 0

def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    return [r[1] for r in rows]

def ensure_column(conn: sqlite3.Connection, table: str, col: str, col_type: str) -> bool:
    cols = table_columns(conn, table)
    if col in cols:
        return False
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
    return True

def backfill_anon_user_id_from_payload(conn: sqlite3.Connection, table: str, id_col: str) -> int:
    cols = table_columns(conn, table)
    if "payload_json" not in cols or "anon_user_id" not in cols:
        return 0

    cur = conn.cursor()
    cur.execute(f"SELECT {id_col}, payload_json, anon_user_id FROM {table}")
    rows = cur.fetchall()

    updated = 0
    for r in rows:
        _id = r[0]
        payload_json = r[1]
        anon_user_id = r[2]
        if anon_user_id:
            continue

        payload = json_loads(payload_json) or {}
        anon = payload.get("anonUserId") or payload.get("anon_user_id")
        if not anon:
            continue

        cur.execute(f"UPDATE {table} SET anon_user_id = ? WHERE {id_col} = ?", (str(anon), _id))
        updated += 1

    return updated

def safe_create_index(conn: sqlite3.Connection, sql: str) -> None:
    cur = conn.cursor()
    cur.execute(sql)

def db_init_and_migrate() -> None:
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS leads (
        lead_id TEXT PRIMARY KEY,
        created_at_ms INTEGER NOT NULL,
        anon_user_id TEXT,
        industry TEXT NOT NULL,
        goal TEXT NOT NULL,
        platform TEXT NOT NULL,
        budget INTEGER NOT NULL,
        need_fast_delivery INTEGER NOT NULL DEFAULT 0,
        verified_only INTEGER NOT NULL DEFAULT 0,
        only_within_budget INTEGER NOT NULL DEFAULT 1,
        sort TEXT NOT NULL DEFAULT 'recommended',
        payload_json TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        created_at_ms INTEGER NOT NULL,
        anon_user_id TEXT,
        lead_id TEXT,
        product_id TEXT NOT NULL,
        product_snapshot_json TEXT NOT NULL,
        status TEXT NOT NULL,
        buyer_issue TEXT,
        buyer_verdict TEXT,
        admin_verdict TEXT,
        evidence_json TEXT NOT NULL,
        admin_memo TEXT,
        payload_json TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS disputes (
        dispute_id TEXT PRIMARY KEY,
        created_at_ms INTEGER NOT NULL,
        order_id TEXT NOT NULL,
        anon_user_id TEXT,
        status TEXT NOT NULL,
        reason TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        payload_json TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS resolutions (
        resolution_id TEXT PRIMARY KEY,
        created_at_ms INTEGER NOT NULL,
        dispute_id TEXT NOT NULL,
        order_id TEXT NOT NULL,
        result TEXT NOT NULL,
        memo TEXT,
        payload_json TEXT NOT NULL
    )
    """)

    conn.commit()

    changed = 0
    if ensure_column(conn, "leads", "anon_user_id", "TEXT"):
        changed += 1
    if ensure_column(conn, "orders", "anon_user_id", "TEXT"):
        changed += 1
    if ensure_column(conn, "disputes", "anon_user_id", "TEXT"):
        changed += 1

    updated = 0
    updated += backfill_anon_user_id_from_payload(conn, "leads", "lead_id")
    updated += backfill_anon_user_id_from_payload(conn, "orders", "order_id")
    updated += backfill_anon_user_id_from_payload(conn, "disputes", "dispute_id")

    conn.commit()

    safe_create_index(conn, "CREATE INDEX IF NOT EXISTS idx_leads_anon ON leads(anon_user_id)")
    safe_create_index(conn, "CREATE INDEX IF NOT EXISTS idx_orders_anon ON orders(anon_user_id)")
    safe_create_index(conn, "CREATE INDEX IF NOT EXISTS idx_disputes_order ON disputes(order_id)")
    safe_create_index(conn, "CREATE INDEX IF NOT EXISTS idx_disputes_status ON disputes(status)")

    cur.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('schema_version','2')")
    cur.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('last_migration',?)", (json_dumps({
        "changed_columns": changed,
        "backfilled_rows": updated,
        "ts_ms": now_ms(),
    }),))
    conn.commit()
    conn.close()

@app.on_event("startup")
def on_startup():
    db_init_and_migrate()

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": {
                "message": "서버 내부 오류",
                "detail": str(exc),
                "trace": traceback.format_exc(),
            }
        },
    )

# ---------------------------------
# Models
# ---------------------------------
class LeadCreate(BaseModel):
    anonUserId: str = Field(..., min_length=3)
    industry: str
    goal: str
    platform: str
    budget: int = Field(..., ge=0)
    needFastDelivery: bool = False
    verifiedOnly: bool = False
    onlyWithinBudget: bool = True
    sort: str = "recommended"
    extra: Dict[str, Any] = Field(default_factory=dict)

class OrderCreate(BaseModel):
    anonUserId: str = Field(..., min_length=3)
    leadId: Optional[str] = None
    productId: str
    # 프론트가 보내면 그대로 고정 저장. 안 보내면 카탈로그에서 찾아서 스냅샷 생성
    productSnapshot: Dict[str, Any] = Field(default_factory=dict)
    payload: Dict[str, Any] = Field(default_factory=dict)

class EvidenceSubmit(BaseModel):
    anonUserId: str
    evidence: List[Dict[str, Any]] = Field(default_factory=list)

class BuyerReview(BaseModel):
    anonUserId: str
    verdict: Literal["approve", "issue"]
    issueText: Optional[str] = None
    evidence: List[Dict[str, Any]] = Field(default_factory=list)

class AdminResolve(BaseModel):
    adminKey: str
    result: Literal["reexecute_approved", "refund_approved", "rejected"]
    memo: Optional[str] = None

# ---------------------------------
# Admin Key
# ---------------------------------
ADMIN_KEY = os.environ.get("ADSTANDARD_ADMIN_KEY", "dev-admin-key")
def require_admin_key(key: str) -> bool:
    return key == ADMIN_KEY

# ---------------------------------
# Health
# ---------------------------------
@app.get("/health")
def health():
    return ok({"status": "up", "db": DB_PATH, "version": "step2"})

# ---------------------------------
# Catalog (A: 임시 카탈로그로 빠르게)
# - 반드시 code + options 보유
# ---------------------------------
DEFAULT_CATALOG = [
    {
        "id": "P001",
        "title": "인스타 릴스 1건 집행",
        "platform": "instagram",
        "summary": "릴스 1건 + 스토리 1회",
        "code": "IG-RLS-1-V1",
        "standardPrice": 150000,
        "floorPrice": 90000,
        "ceilingPrice": 250000,
        "options": {"qty": 1, "durationDays": 0},  # 표준 옵션 구조
        "conditions": {"verifiedOnly": False, "needFastDelivery": False},
    },
    {
        "id": "P002",
        "title": "인스타 스토리 3회",
        "platform": "instagram",
        "summary": "스토리 3회 패키지",
        "code": "IG-STR-PKG-V1",
        "standardPrice": 90000,
        "floorPrice": 60000,
        "ceilingPrice": 140000,
        "options": {"qty": 1, "durationDays": 0},
        "conditions": {"verifiedOnly": False, "needFastDelivery": True},
    },
    {
        "id": "P003",
        "title": "네이버 플레이스 리뷰 10건",
        "platform": "naver",
        "summary": "플레이스 리뷰 10건 패키지",
        "code": "NV-PLC-10-V1",
        "standardPrice": 120000,
        "floorPrice": 80000,
        "ceilingPrice": 180000,
        "options": {"qty": 1, "durationDays": 0},
        "conditions": {"verifiedOnly": True, "needFastDelivery": False},
    },
]

def catalog_find(product_id: str) -> Optional[Dict[str, Any]]:
    return next((p for p in DEFAULT_CATALOG if p["id"] == product_id), None)

@app.get("/catalog/products")
def catalog_products():
    return ok(DEFAULT_CATALOG)

# ---------------------------------
# Leads
# ---------------------------------
@app.post("/leads")
def create_lead(body: LeadCreate):
    lead_id = new_id("L")
    created = now_ms()

    payload = {
        "leadId": lead_id,
        "createdAtMs": created,
        "anonUserId": body.anonUserId,
        "industry": body.industry,
        "goal": body.goal,
        "platform": body.platform,
        "budget": body.budget,
        "needFastDelivery": body.needFastDelivery,
        "verifiedOnly": body.verifiedOnly,
        "onlyWithinBudget": body.onlyWithinBudget,
        "sort": body.sort,
        "extra": body.extra or {},
    }

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO leads (
            lead_id, created_at_ms, anon_user_id,
            industry, goal, platform, budget,
            need_fast_delivery, verified_only, only_within_budget, sort,
            payload_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            lead_id, created, body.anonUserId,
            body.industry, body.goal, body.platform, int(body.budget),
            bool_to_int(body.needFastDelivery),
            bool_to_int(body.verifiedOnly),
            bool_to_int(body.onlyWithinBudget),
            body.sort,
            json_dumps(payload),
        ),
    )
    conn.commit()
    conn.close()

    return ok(payload)

@app.get("/leads/{lead_id}")
def get_lead(lead_id: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT payload_json FROM leads WHERE lead_id = ?", (lead_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return fail("리드를 찾을 수 없습니다.", 404)
    return ok(json_loads(row["payload_json"]))

def load_lead(lead_id: str) -> Optional[Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT payload_json FROM leads WHERE lead_id = ?", (lead_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return json_loads(row["payload_json"])

# ---------------------------------
# 추천 카드 (Step 3 UX에서 사용)
# /a/products?leadId=...
# 반환: 표준가/하한/상한 + 조건 요약 + 선택 CTA에 필요한 id/code/quote
# ---------------------------------
@app.get("/a/products")
def a_products(
    leadId: str = Query(...),
):
    lead = load_lead(leadId)
    if not lead:
        return fail("리드를 찾을 수 없습니다.", 404)

    lead_platform = (lead.get("platform") or "").lower()
    sort = (lead.get("sort") or "recommended").lower()

    cards: List[Dict[str, Any]] = []

    for item in DEFAULT_CATALOG:
        item_platform = (item.get("platform") or "").lower()
        # v0: 플랫폼 매칭 우선 (불일치도 보여주되 score 낮춤)
        platform_match = (lead_platform == item_platform)

        quote = price_quote(lead, item)
        score = quote.get("score", 0)
        if platform_match:
            score += 20  # 플랫폼 매칭 가산
        else:
            score -= 5

        # 조건 요약(카드에 노출할 텍스트)
        cond = []
        if lead.get("verifiedOnly"):
            cond.append("검증 판매자")
        if lead.get("needFastDelivery"):
            cond.append("긴급")
        if lead.get("onlyWithinBudget"):
            cond.append("예산 내")
        cond_text = " · ".join(cond) if cond else "기본"

        cards.append({
            "id": item["id"],
            "title": item["title"],
            "summary": item.get("summary"),
            "platform": item.get("platform"),
            "code": item.get("code"),
            "options": item.get("options", {}),
            "conditionsSummary": cond_text,
            "quote": quote,
            "score": score,
            "cta": {"label": "선택", "action": "create_order", "productId": item["id"]},
        })

    # 정렬
    if sort == "cheap":
        cards.sort(key=lambda x: x["quote"]["standardPrice"])
    elif sort == "expensive":
        cards.sort(key=lambda x: -x["quote"]["standardPrice"])
    else:
        # recommended
        cards.sort(key=lambda x: -x["score"])

    return ok({"leadId": leadId, "items": cards})

# ---------------------------------
# Orders
# - 주문 생성 시 productSnapshot에 code/options/quote를 고정 저장
# ---------------------------------
def load_order(order_id: str) -> Optional[Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT payload_json FROM orders WHERE order_id = ?", (order_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return json_loads(row["payload_json"])

def save_order_payload(order_id: str, payload: Dict[str, Any]) -> None:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE orders SET payload_json = ?, status = ?, buyer_issue = ?, buyer_verdict = ?, admin_verdict = ?, evidence_json = ?, admin_memo = ? WHERE order_id = ?",
        (
            json_dumps(payload),
            payload.get("status", "created"),
            payload.get("buyerIssue"),
            payload.get("buyerVerdict"),
            payload.get("adminVerdict"),
            json_dumps(payload.get("evidence", [])),
            payload.get("adminMemo"),
            order_id,
        ),
    )
    conn.commit()
    conn.close()

@app.post("/orders")
def create_order(body: OrderCreate):
    order_id = new_id("O")
    created = now_ms()

    # lead 로드 (가능하면 quote까지 고정 저장하기 위함)
    lead = load_lead(body.leadId) if body.leadId else None

    # product snapshot 생성
    product_snapshot = body.productSnapshot or {}
    if not product_snapshot:
        found = catalog_find(body.productId)
        if not found:
            found = {"id": body.productId, "title": body.productId, "code": "UNKNOWN", "options": {"qty": 1, "durationDays": 0},
                     "standardPrice": 0, "floorPrice": 0, "ceilingPrice": 0}
        product_snapshot = dict(found)

    # quote 고정(lead가 있으면 현재 조건으로 산출해서 저장)
    if lead:
        quote = price_quote(lead, product_snapshot)
    else:
        # lead 없이 주문 생성된 경우(테스트): 기본 quote
        quote = {
            "standardPrice": int(product_snapshot.get("standardPrice", 0)),
            "floorPrice": int(product_snapshot.get("floorPrice", 0)),
            "ceilingPrice": int(product_snapshot.get("ceilingPrice", 0)),
            "eligible": True,
            "score": 0,
            "reasons": ["leadId 없음: 기본가로 고정"],
            "applied": {"verifiedOnly": False, "needFastDelivery": False, "qty": 1, "durationDays": 0},
        }

    # 스냅샷에 quote 포함(주문 시점 고정)
    product_snapshot["quote"] = quote

    payload = {
        "orderId": order_id,
        "createdAtMs": created,
        "anonUserId": body.anonUserId,
        "leadId": body.leadId,
        "productId": body.productId,
        "productSnapshot": product_snapshot,
        "status": "created",
        "evidence": [],
        "buyerVerdict": None,
        "buyerIssue": None,
        "adminVerdict": None,
        "adminMemo": None,
        "payload": body.payload or {},
    }

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (
            order_id, created_at_ms, anon_user_id, lead_id,
            product_id, product_snapshot_json,
            status, buyer_issue, buyer_verdict, admin_verdict,
            evidence_json, admin_memo,
            payload_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            order_id, created, body.anonUserId, body.leadId,
            body.productId, json_dumps(product_snapshot),
            payload["status"], None, None, None,
            json_dumps([]), None,
            json_dumps(payload),
        ),
    )
    conn.commit()
    conn.close()

    return ok(payload)

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    payload = load_order(order_id)
    if not payload:
        return fail("주문을 찾을 수 없습니다.", 404)
    return ok(payload)

@app.post("/orders/{order_id}/seller/evidence")
def submit_evidence(order_id: str, body: EvidenceSubmit):
    payload = load_order(order_id)
    if not payload:
        return fail("주문을 찾을 수 없습니다.", 404)

    if payload.get("anonUserId") != body.anonUserId:
        return fail("권한이 없습니다.", 403)

    evidence_list = payload.get("evidence", [])
    evidence_list.extend(body.evidence or [])
    payload["evidence"] = evidence_list
    payload["status"] = "evidence_submitted"

    save_order_payload(order_id, payload)
    return ok(payload)

@app.post("/orders/{order_id}/buyer/review")
def buyer_review(order_id: str, body: BuyerReview):
    payload = load_order(order_id)
    if not payload:
        return fail("주문을 찾을 수 없습니다.", 404)

    if payload.get("anonUserId") != body.anonUserId:
        return fail("권한이 없습니다.", 403)

    payload["buyerVerdict"] = body.verdict
    payload["buyerIssue"] = body.issueText if body.verdict == "issue" else None

    if body.verdict == "approve":
        payload["status"] = "completed"
        save_order_payload(order_id, payload)
        return ok(payload)

    # issue -> dispute 생성
    dispute_id = new_id("D")
    created = now_ms()

    dispute_payload = {
        "disputeId": dispute_id,
        "createdAtMs": created,
        "orderId": order_id,
        "anonUserId": body.anonUserId,
        "status": "open",
        "reason": body.issueText or "이슈 제기",
        "evidence": body.evidence or [],
        "source": "buyer_review",
    }

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO disputes (
            dispute_id, created_at_ms, order_id, anon_user_id,
            status, reason, evidence_json, payload_json
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            dispute_id, created, order_id, body.anonUserId,
            "open", dispute_payload["reason"],
            json_dumps(dispute_payload["evidence"]),
            json_dumps(dispute_payload),
        ),
    )
    conn.commit()
    conn.close()

    payload["status"] = "disputed"
    save_order_payload(order_id, payload)

    return ok({"order": payload, "dispute": dispute_payload})

# ---------------------------------
# Admin
# ---------------------------------
@app.get("/admin/disputes")
def admin_list_disputes(
    adminKey: str = Query(...),
    processed: Optional[bool] = Query(None),
):
    if not require_admin_key(adminKey):
        return fail("관리자 키가 올바르지 않습니다.", 403)

    conn = db_conn()
    cur = conn.cursor()

    if processed is None:
        cur.execute("SELECT payload_json FROM disputes ORDER BY created_at_ms DESC LIMIT 200")
    elif processed is True:
        cur.execute("SELECT payload_json FROM disputes WHERE status = 'resolved' ORDER BY created_at_ms DESC LIMIT 200")
    else:
        cur.execute("SELECT payload_json FROM disputes WHERE status = 'open' ORDER BY created_at_ms DESC LIMIT 200")

    rows = cur.fetchall()
    conn.close()

    items = [json_loads(r["payload_json"]) for r in rows]
    return ok(items)

@app.post("/admin/disputes/{dispute_id}/resolve")
def admin_resolve(dispute_id: str, body: AdminResolve):
    if not require_admin_key(body.adminKey):
        return fail("관리자 키가 올바르지 않습니다.", 403)

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT payload_json FROM disputes WHERE dispute_id = ?", (dispute_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return fail("분쟁을 찾을 수 없습니다.", 404)

    dispute_payload = json_loads(row["payload_json"])
    if dispute_payload.get("status") == "resolved":
        conn.close()
        return fail("이미 처리된 분쟁입니다.", 400)

    order_id = dispute_payload["orderId"]

    # resolution 생성
    resolution_id = new_id("R")
    created = now_ms()

    resolution_payload = {
        "resolutionId": resolution_id,
        "createdAtMs": created,
        "disputeId": dispute_id,
        "orderId": order_id,
        "result": body.result,
        "memo": body.memo,
    }

    cur.execute(
        """
        INSERT INTO resolutions (
            resolution_id, created_at_ms, dispute_id, order_id,
            result, memo, payload_json
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (
            resolution_id, created, dispute_id, order_id,
            body.result, body.memo, json_dumps(resolution_payload),
        ),
    )

    dispute_payload["status"] = "resolved"
    dispute_payload["resolvedAtMs"] = created
    dispute_payload["resolution"] = resolution_payload

    cur.execute(
        "UPDATE disputes SET status = 'resolved', payload_json = ? WHERE dispute_id = ?",
        (json_dumps(dispute_payload), dispute_id),
    )

    conn.commit()
    conn.close()

    order_payload = load_order(order_id)
    if order_payload:
        order_payload["adminVerdict"] = body.result
        order_payload["adminMemo"] = body.memo
        if body.result == "reexecute_approved":
            order_payload["status"] = "resolved"
        elif body.result == "refund_approved":
            order_payload["status"] = "refunded"
        else:
            order_payload["status"] = "rejected"
        save_order_payload(order_id, order_payload)

    return ok({"dispute": dispute_payload, "resolution": resolution_payload, "order": order_payload})

# ---------------------------------
# (선택) 개발용 DB 초기화
# ---------------------------------
@app.post("/admin/dev/reset-db")
def admin_reset_db(adminKey: str = Query(...)):
    if not require_admin_key(adminKey):
        return fail("관리자 키가 올바르지 않습니다.", 403)

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM resolutions")
    cur.execute("DELETE FROM disputes")
    cur.execute("DELETE FROM orders")
    cur.execute("DELETE FROM leads")
    conn.commit()
    conn.close()

    return ok({"reset": True, "db": DB_PATH})
