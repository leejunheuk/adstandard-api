# price_engine.py (Price Engine v0)
# 입력: lead(dict) + catalog_item(dict)
# 출력: quote(dict) = standard/floor/ceiling + reasons + eligibility(예산 등)

from typing import Any, Dict, List, Tuple

def _to_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except:
        return default

def _to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    try:
        return int(x) == 1
    except:
        return False

def _mul_round(x: int, mul: float) -> int:
    return int(round(x * mul))

def price_quote(lead: Dict[str, Any], item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Price Engine v0 규칙:
    - catalog의 base standard/floor/ceiling을 시작점으로 사용
    - 옵션(verifiedOnly, needFastDelivery)에 따라 가감산
    - qty가 있으면 단순 곱 (v0)
    - onlyWithinBudget이면 표준가가 예산을 넘는 경우 'eligible=False'로 표시
    """

    reasons: List[str] = []

    base_std = _to_int(item.get("standardPrice", 0))
    base_floor = _to_int(item.get("floorPrice", 0))
    base_ceil = _to_int(item.get("ceilingPrice", 0))

    # lead options
    verified_only = _to_bool(lead.get("verifiedOnly", False))
    fast = _to_bool(lead.get("needFastDelivery", False))
    only_within_budget = _to_bool(lead.get("onlyWithinBudget", True))
    budget = _to_int(lead.get("budget", 0))

    # item options (주문/추천 시 공통 키로 유지)
    options = item.get("options", {}) or {}
    qty = _to_int(options.get("qty", 1), 1)
    duration_days = _to_int(options.get("durationDays", 0), 0)

    std = base_std
    floor = base_floor
    ceil = base_ceil

    reasons.append(f"기본가 적용: 표준 {base_std:,} / 하한 {base_floor:,} / 상한 {base_ceil:,}")

    # verifiedOnly 가산 (v0: +10%)
    if verified_only:
        std = _mul_round(std, 1.10)
        floor = _mul_round(floor, 1.10)
        ceil = _mul_round(ceil, 1.10)
        reasons.append("검증 판매자 옵션(+10%) 적용")

    # fast 가산 (v0: +15%)
    if fast:
        std = _mul_round(std, 1.15)
        floor = _mul_round(floor, 1.15)
        ceil = _mul_round(ceil, 1.15)
        reasons.append("긴급 집행 옵션(+15%) 적용")

    # durationDays (v0: 0이면 무시, 7일 이상이면 +5%)
    if duration_days >= 7:
        std = _mul_round(std, 1.05)
        floor = _mul_round(floor, 1.05)
        ceil = _mul_round(ceil, 1.05)
        reasons.append("기간 옵션(7일 이상 +5%) 적용")

    # qty (v0: 단순 곱)
    if qty > 1:
        std = std * qty
        floor = floor * qty
        ceil = ceil * qty
        reasons.append(f"수량(qty={qty}) 곱 적용")

    eligible = True
    if only_within_budget and budget > 0 and std > budget:
        eligible = False
        reasons.append(f"예산 초과: 표준가 {std:,} > 예산 {budget:,} (onlyWithinBudget=True)")

    # 간단 적합도 점수 (추천 정렬용 v0)
    score = 0
    # 플랫폼/상품군 적합도는 main에서 플랫폼 매칭 시 + 가산
    if eligible:
        score += 10
    # 예산에 가까우면(너무 낮거나 너무 높지 않게) + 가산
    if budget > 0:
        gap = abs(budget - std)
        # gap이 작을수록 점수 높게 (대충)
        score += max(0, 10 - int(gap / max(1, budget) * 20))

    return {
        "standardPrice": std,
        "floorPrice": floor,
        "ceilingPrice": ceil,
        "eligible": eligible,
        "score": score,
        "reasons": reasons,
        "applied": {
            "verifiedOnly": verified_only,
            "needFastDelivery": fast,
            "qty": qty,
            "durationDays": duration_days,
        }
    }
