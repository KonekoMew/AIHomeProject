"""Luckin Coffee MCP order orchestration.

The model only sees a bracket command. Credentials and MCP calls stay in the
backend, and payment is intentionally left for the user to confirm.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any

from config import SETTINGS
from mcp_client import mcp_manager


LUCKIN_SERVER_NAME = "Luckin Coffee"
LUCKIN_MCP_URL = "https://gwmcp.lkcoffee.com/order/user/mcp"
LUCKIN_CMD_PATTERN = re.compile(r"\[LUCKIN:([^\]]+)\]", re.IGNORECASE)
LUCKIN_ABILITY_TEXT = (
    "[LUCKIN:饮品和规格] — 仅当用户明确要求瑞幸咖啡下单时使用。"
    "系统会创建待支付订单并返回支付链接/二维码；不要声称已经付款。"
)


class LuckinOrderError(RuntimeError):
    pass


_CN_NUMBERS = {
    "一": 1,
    "两": 2,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}

_SPEC_ALIASES: list[tuple[str, tuple[str, ...]]] = [
    ("半糖", ("半糖", "半甜", "五分糖", "五分甜", "5分糖", "5分甜", "五成糖", "50%糖", "50%甜", "50糖", "50甜")),
    ("少糖", ("少糖", "少甜", "三分糖", "三分甜", "3分糖", "3分甜", "微糖", "微甜", "30%糖", "30%甜")),
    ("少少甜", ("少少甜", "少少糖", "轻甜", "轻糖", "微微甜", "微微糖")),
    ("不另外加糖", ("不另外加糖", "不加糖", "无糖", "去糖")),
    ("标准糖", ("标准糖", "标准甜", "正常糖", "正常甜", "全糖")),
    ("热", ("热饮", "热的", "加热")),
    ("冰", ("冰饮", "冰的", "正常冰")),
    ("少冰", ("少冰",)),
    ("去冰", ("去冰",)),
    ("常温", ("常温",)),
    ("大杯", ("大杯",)),
    ("中杯", ("中杯",)),
]
_SPEC_GROUPS = {
    "半糖": "sugar",
    "少糖": "sugar",
    "少少甜": "sugar",
    "不另外加糖": "sugar",
    "标准糖": "sugar",
    "热": "temperature",
    "冰": "temperature",
    "少冰": "temperature",
    "去冰": "temperature",
    "常温": "temperature",
    "大杯": "size",
    "中杯": "size",
}
_SPEC_GROUP_HINTS = {
    "sugar": ("糖", "甜", "甜度", "糖度"),
    "temperature": ("温度", "冷热", "冰", "热"),
    "size": ("杯型", "杯量", "规格", "容量"),
}

_FILLER_TERMS = (
    "帮我点", "点一杯", "点杯", "一杯", "杯", "瑞幸咖啡", "瑞幸", "咖啡",
    "谢谢", "麻烦", "要", "来个", "来一杯",
)


def is_luckin_enabled() -> bool:
    return bool(SETTINGS.get("luckin_mcp_enabled") and _luckin_token())


def luckin_ability_text() -> str | None:
    return LUCKIN_ABILITY_TEXT if is_luckin_enabled() else None


def _luckin_token() -> str:
    return str(SETTINGS.get("luckin_mcp_token") or "").strip()


def _auth_headers() -> dict[str, str]:
    token = _luckin_token()
    if not token:
        return {}
    if token.lower().startswith("bearer "):
        return {"Authorization": token}
    return {"Authorization": f"Bearer {token}"}


async def _ensure_luckin_server(*, reset: bool = False):
    headers = _auth_headers()
    if not headers:
        raise LuckinOrderError("还没有配置瑞幸 MCP Token。")

    if reset and mcp_manager.is_connected(LUCKIN_SERVER_NAME):
        await mcp_manager.disconnect(LUCKIN_SERVER_NAME)

    mcp_manager.upsert_server(
        LUCKIN_SERVER_NAME,
        "streamablehttp",
        LUCKIN_MCP_URL,
        headers=headers,
        enabled=True,
        visible=False,
    )
    if not mcp_manager.is_connected(LUCKIN_SERVER_NAME):
        await mcp_manager.connect(LUCKIN_SERVER_NAME)


def _loads_maybe_json(text: str) -> Any:
    raw = (text or "").strip()
    if not raw:
        return raw
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I).strip()
        raw = re.sub(r"\s*```$", "", raw).strip()
    try:
        data = json.loads(raw)
        if isinstance(data, str):
            try:
                return json.loads(data)
            except Exception:
                return data
        return data
    except Exception:
        return raw


def _decode_mcp_payload(contents: Any) -> Any:
    if not isinstance(contents, list):
        return contents
    for item in contents:
        if isinstance(item, dict) and item.get("type") == "text":
            return _loads_maybe_json(str(item.get("text") or ""))
    return contents


def _find_value(data: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(data, dict):
        for key in keys:
            if key in data:
                return data[key]
        for value in data.values():
            found = _find_value(value, keys)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_value(item, keys)
            if found is not None:
                return found
    return None


def _iter_dicts(data: Any):
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from _iter_dicts(value)
    elif isinstance(data, list):
        for item in data:
            yield from _iter_dicts(item)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def _raise_mcp_error(data: Any):
    if not isinstance(data, dict):
        return
    if data.get("success") is False or data.get("ok") is False:
        msg = _as_text(data.get("message") or data.get("msg") or data.get("error"))
        raise LuckinOrderError(msg or "MCP 返回失败。")
    code = data.get("code")
    if code is None:
        return
    code_text = str(code).strip().lower()
    if code_text in {"0", "200", "000000", "success"}:
        return
    msg = _as_text(data.get("message") or data.get("msg") or data.get("error"))
    if msg:
        raise LuckinOrderError(msg)


async def _call_luckin_tool(tool_name: str, arguments: dict[str, Any]) -> Any:
    await _ensure_luckin_server()
    contents = await mcp_manager.call_tool(LUCKIN_SERVER_NAME, tool_name, arguments)
    data = _decode_mcp_payload(contents)
    _raise_mcp_error(data)
    return data


def _resolve_coordinates() -> tuple[float, float]:
    lon = _as_float(SETTINGS.get("luckin_default_longitude"))
    lat = _as_float(SETTINGS.get("luckin_default_latitude"))
    if lon and lat:
        return lon, lat

    try:
        from location import load_location_config, load_location_status

        status = load_location_status()
        lon = _as_float(status.get("lng"))
        lat = _as_float(status.get("lat"))
        if lon and lat:
            return lon, lat

        loc_cfg = load_location_config()
        lon = _as_float(loc_cfg.get("home_lng"))
        lat = _as_float(loc_cfg.get("home_lat"))
        if lon and lat:
            return lon, lat
    except Exception:
        pass

    raise LuckinOrderError("没有可用坐标，请在设置里填写瑞幸默认经纬度，或先开启定位。")


def _distance_value(shop: dict[str, Any]) -> float:
    value = _first_present(shop, ("distance", "distanceMeter", "distance_m", "sortDistance"))
    parsed = _as_float(value)
    return parsed if parsed is not None else 10**12


def _extract_shops(data: Any) -> list[dict[str, Any]]:
    shops = []
    for item in _iter_dicts(data):
        dept_id = _first_present(item, ("deptId", "dept_id", "shopId", "storeId"))
        name = _first_present(item, ("deptName", "dept_name", "shopName", "storeName", "name"))
        if dept_id and name:
            shops.append(item)
    return shops


def _choose_shop(data: Any, keyword: str = "") -> dict[str, Any]:
    shops = _extract_shops(data)
    if keyword:
        norm_kw = _normalize_text(keyword)
        filtered = [
            shop for shop in shops
            if norm_kw in _normalize_text(_shop_name(shop) + _shop_address(shop))
        ]
        if filtered:
            shops = filtered
    if not shops:
        raise LuckinOrderError("没有找到可下单门店。")
    return sorted(shops, key=_distance_value)[0]


def _extract_products(data: Any) -> list[dict[str, Any]]:
    products = []
    for item in _iter_dicts(data):
        product_id = _first_present(item, ("productId", "product_id", "spuId"))
        sku_code = _first_present(item, ("skuCode", "sku_code", "skuId"))
        name = _first_present(item, ("productName", "product_name", "name", "goodsName"))
        has_attrs = any(key in item for key in ("productAttrs", "attrs", "attrList", "attributes"))
        if product_id and sku_code and (name or has_attrs):
            products.append(item)
    return products


def _choose_product(data: Any) -> dict[str, Any]:
    products = _extract_products(data)
    if not products:
        raise LuckinOrderError("没有找到匹配的瑞幸商品。")
    return copy.deepcopy(products[0])


def _shop_id(shop: dict[str, Any]) -> str:
    return _as_text(_first_present(shop, ("deptId", "dept_id", "shopId", "storeId")))


def _shop_name(shop: dict[str, Any]) -> str:
    return _as_text(_first_present(shop, ("deptName", "dept_name", "shopName", "storeName", "name")))


def _shop_address(shop: dict[str, Any]) -> str:
    return _as_text(_first_present(shop, ("address", "deptAddress", "shopAddress")))


def _product_id(product: dict[str, Any]) -> str:
    return _as_text(_first_present(product, ("productId", "product_id", "spuId")))


def _sku_code(product: dict[str, Any]) -> str:
    return _as_text(_first_present(product, ("skuCode", "sku_code", "skuId")))


def _product_name(product: dict[str, Any]) -> str:
    return _as_text(_first_present(product, ("productName", "product_name", "name", "goodsName")))


def _normalize_text(text: str) -> str:
    return re.sub(r"[\s,，。.!！?？、；;:：|/\\\-]+", "", (text or "").lower())


def _spec_aliases(spec: str) -> tuple[str, ...]:
    return next((aliases for canonical, aliases in _SPEC_ALIASES if canonical == spec), ())


def _spec_names(spec: str) -> set[str]:
    return {_normalize_text(alias) for alias in _spec_aliases(spec) if _normalize_text(alias)}


def _parse_amount(order_text: str) -> int:
    match = re.search(r"([1-9]\d*)\s*(?:杯|份)", order_text)
    if match:
        return max(1, min(9, int(match.group(1))))
    for cn, num in _CN_NUMBERS.items():
        if re.search(rf"{cn}\s*(?:杯|份)", order_text):
            return num
    return 1


def _wanted_specs(order_text: str) -> list[str]:
    norm = _normalize_text(order_text)
    found = []
    for order, (canonical, _) in enumerate(_SPEC_ALIASES):
        for alias in _spec_names(canonical):
            pos = norm.find(alias)
            while pos >= 0:
                found.append((pos, pos + len(alias), -len(alias), order, canonical))
                pos = norm.find(alias, pos + 1)
    wanted = []
    used_spans = []
    for start, end, _, _, canonical in sorted(found, key=lambda item: (item[0], item[2], item[3])):
        if any(start < used_end and end > used_start for used_start, used_end in used_spans):
            continue
        if canonical not in wanted:
            wanted.append(canonical)
        used_spans.append((start, end))
    return wanted


def _product_query(order_text: str) -> str:
    query = order_text
    aliases = [alias for canonical, _ in _SPEC_ALIASES for alias in _spec_aliases(canonical)]
    for alias in sorted(aliases, key=len, reverse=True):
        query = query.replace(alias, " ")
    for filler in _FILLER_TERMS:
        query = query.replace(filler, " ")
    query = re.sub(r"\d+\s*(?:杯|份)", " ", query)
    for cn in _CN_NUMBERS:
        query = re.sub(rf"{cn}\s*(?:杯|份)", " ", query)
    query = re.sub(r"[\s,，。.!！?？、；;:：|/\\\-]+", " ", query).strip()
    return query or order_text.strip()


def _product_attrs(product: dict[str, Any]) -> list[dict[str, Any]]:
    attrs = _first_present(product, ("productAttrs", "attrs", "attrList", "attributes"))
    return attrs if isinstance(attrs, list) else []


def _sub_attrs(attr: dict[str, Any]) -> list[dict[str, Any]]:
    subs = _first_present(attr, ("productSubAttrs", "subAttrs", "subAttrList", "values", "children"))
    return subs if isinstance(subs, list) else []


def _attr_id(attr: dict[str, Any]) -> str:
    return _as_text(_first_present(attr, ("attributeId", "attrId", "id")))


def _sub_attr_id(sub_attr: dict[str, Any]) -> str:
    return _as_text(_first_present(sub_attr, ("attributeId", "subAttributeId", "attrId", "id")))


def _attr_name(attr: dict[str, Any]) -> str:
    return _as_text(_first_present(attr, ("attributeName", "attrName", "name")))


def _sub_attr_name(sub_attr: dict[str, Any]) -> str:
    return _as_text(_first_present(sub_attr, ("attributeName", "attrName", "name", "label")))


def _is_selected(sub_attr: dict[str, Any]) -> bool:
    for key in ("selected", "checked", "isSelected", "defaultSelected", "isDefault"):
        value = sub_attr.get(key)
        text = str(value).strip().lower()
        if value is True or text in {"true", "1", "yes"} or value == 1:
            return True
    return False


def _is_selectable(sub_attr: dict[str, Any]) -> bool:
    for key in ("canSelected", "canSelect", "selectable", "enabled", "available", "isAvailable", "saleable"):
        if key not in sub_attr:
            continue
        value = sub_attr.get(key)
        text = str(value).strip().lower()
        if value is False or value == 0 or text in {"false", "0", "no", "n", "disabled", "unavailable"}:
            return False
    return True


def _attr_group_score(spec: str, attr: dict[str, Any]) -> int:
    group = _SPEC_GROUPS.get(spec)
    if not group:
        return 1
    attr_norm = _normalize_text(_attr_name(attr))
    hints = tuple(_normalize_text(h) for h in _SPEC_GROUP_HINTS.get(group, ()))
    return 0 if attr_norm and any(h and h in attr_norm for h in hints) else 1


def _available_attr_options(product: dict[str, Any], *, max_groups: int = 5, max_items: int = 8) -> str:
    groups = []
    for attr in _product_attrs(product):
        attr_name = _attr_name(attr) or "规格"
        names = []
        for sub in _sub_attrs(attr):
            name = _sub_attr_name(sub)
            if name and _is_selectable(sub):
                names.append(name)
        names = list(dict.fromkeys(names))
        if not names:
            continue
        if len(names) > max_items:
            names = [*names[:max_items], "更多"]
        groups.append(f"{attr_name}：{' / '.join(names)}")
    if not groups:
        return ""
    if len(groups) > max_groups:
        groups = [*groups[:max_groups], "更多规格"]
    return "；".join(groups)


def _match_attr_operation(
    product: dict[str, Any],
    spec: str,
    matched_groups: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    matched_groups = matched_groups or set()
    spec_names = _spec_names(spec)
    attrs = list(enumerate(_product_attrs(product)))
    attrs.sort(key=lambda item: (_attr_group_score(spec, item[1]), item[0]))
    fallback: tuple[dict[str, Any], dict[str, Any]] | None = None

    for _, attr in attrs:
        attr_key = _attr_id(attr) or _attr_name(attr)
        if attr_key in matched_groups:
            continue
        for sub in _sub_attrs(attr):
            if not _is_selectable(sub):
                continue
            sub_norm = _normalize_text(_sub_attr_name(sub))
            if not sub_norm:
                continue
            if sub_norm in spec_names:
                return attr, sub
            if fallback is None and any(name and (name in sub_norm or sub_norm in name) for name in spec_names):
                fallback = (attr, sub)
    return fallback


def _match_attr_operations(product: dict[str, Any], wanted_specs: list[str]) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    matches = []
    matched_groups = set()
    for spec in wanted_specs:
        match = _match_attr_operation(product, spec, matched_groups)
        if not match:
            continue
        attr, sub = match
        if not _is_selected(sub):
            matches.append((spec, attr, sub))
        attr_key = _attr_id(attr) or _attr_name(attr)
        if attr_key:
            matched_groups.add(attr_key)
    return matches


def _product_item(product: dict[str, Any], amount: int) -> dict[str, Any]:
    product_id = _product_id(product)
    sku_code = _sku_code(product)
    if not product_id or not sku_code:
        raise LuckinOrderError("商品缺少 productId 或 skuCode，无法创建订单。")
    return {
        "productId": product_id,
        "skuCode": sku_code,
        "amount": amount,
    }


async def _apply_specs(
    dept_id: str,
    product: dict[str, Any],
    wanted_specs: list[str],
    amount: int,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    if not wanted_specs:
        return product, warnings

    matched_groups = set()
    for spec in wanted_specs:
        match = _match_attr_operation(product, spec, matched_groups)
        if not match:
            options = _available_attr_options(product)
            suffix = f"；该商品可选规格：{options}" if options else ""
            warnings.append(f"未匹配到规格 {spec}，已保留默认规格{suffix}")
            continue
        attr, sub = match
        attr_key = _attr_id(attr) or _attr_name(attr)
        if attr_key:
            matched_groups.add(attr_key)
        if _is_selected(sub):
            continue
        attr_id = _attr_id(attr)
        sub_id = _sub_attr_id(sub)
        if not attr_id or not sub_id:
            warnings.append(f"规格 {spec} 缺少属性 ID，已保留默认规格")
            continue
        try:
            data = await _call_luckin_tool(
                "switchProduct",
                {
                    "deptId": dept_id,
                    "productId": _product_id(product),
                    "skuCode": _sku_code(product),
                    "amount": amount,
                    "attrOperationParam": {
                        "attributeId": attr_id,
                        "subAttr": {
                            "attributeId": sub_id,
                            "operation": 3,
                        },
                    },
                },
            )
            updated = _choose_product(data)
            product.update(updated)
        except Exception as exc:
            warnings.append(f"规格 {spec} 切换失败：{exc}")
    return product, warnings


def _selected_attr_names(product: dict[str, Any]) -> str:
    desc = _as_text(_first_present(product, ("additionDesc", "attrDesc", "skuName", "skuDesc")))
    if desc:
        return desc
    names = []
    for attr in _product_attrs(product):
        for sub in _sub_attrs(attr):
            if _is_selected(sub):
                name = _as_text(_first_present(sub, ("attributeName", "attrName", "name", "label")))
                if name:
                    names.append(name)
    return " / ".join(dict.fromkeys(names))


def _coupon_code_list(preview_data: Any) -> list[Any]:
    coupon_list = _find_value(preview_data, ("couponCodeList", "couponCodes", "couponList"))
    return coupon_list if isinstance(coupon_list, list) else []


def _order_value(order_data: Any, key: str) -> Any:
    return _find_value(order_data, (key,))


def _format_money(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        return f"{value}元"
    text = str(value).strip()
    if not text:
        return ""
    if text.endswith("元"):
        return text
    return f"{text}元" if re.search(r"\d", text) else text


async def place_luckin_order(order_text: str) -> dict[str, Any]:
    if not SETTINGS.get("luckin_mcp_enabled"):
        raise LuckinOrderError("瑞幸 MCP 还没有启用。")
    if not _luckin_token():
        raise LuckinOrderError("瑞幸 MCP Token 为空。")

    longitude, latitude = _resolve_coordinates()
    amount = _parse_amount(order_text)
    shop_keyword = str(SETTINGS.get("luckin_default_shop_keyword") or "").strip()
    await _ensure_luckin_server(reset=True)

    shop_args: dict[str, Any] = {"longitude": longitude, "latitude": latitude}
    if shop_keyword:
        shop_args["deptName"] = shop_keyword
    shops_data = await _call_luckin_tool("queryShopList", shop_args)
    shop = _choose_shop(shops_data, shop_keyword)
    dept_id = _shop_id(shop)
    if not dept_id:
        raise LuckinOrderError("门店缺少 deptId，无法下单。")

    query = _product_query(order_text)
    try:
        product_data = await _call_luckin_tool("searchProductForMcp", {"deptId": dept_id, "query": query})
    except Exception:
        product_data = await _call_luckin_tool("searchProductForMcp", {"deptId": dept_id, "query": order_text.strip()})
    product = _choose_product(product_data)
    product, warnings = await _apply_specs(dept_id, product, _wanted_specs(order_text), amount)

    product_list = [_product_item(product, amount)]
    preview_data = await _call_luckin_tool("previewOrder", {"deptId": dept_id, "productList": product_list})
    create_args: dict[str, Any] = {
        "deptId": dept_id,
        "productList": product_list,
        "longitude": longitude,
        "latitude": latitude,
    }
    coupon_codes = _coupon_code_list(preview_data)
    if coupon_codes:
        create_args["couponCodeList"] = coupon_codes
    order_data = await _call_luckin_tool("createOrder", create_args)

    return {
        "ok": True,
        "query": order_text.strip(),
        "product_name": _product_name(product),
        "attrs": _selected_attr_names(product),
        "amount": amount,
        "shop_name": _shop_name(shop),
        "shop_address": _shop_address(shop),
        "order_id": _as_text(_order_value(order_data, "orderIdStr") or _order_value(order_data, "orderId")),
        "pay_url": _as_text(_order_value(order_data, "payOrderUrl")),
        "pay_qr_url": _as_text(_order_value(order_data, "payOrderQrCodeUrl")),
        "need_pay": _order_value(order_data, "needPay"),
        "discount_price": _order_value(order_data, "discountPrice"),
        "warnings": warnings,
        "raw_preview": preview_data,
        "raw_order": order_data,
    }


async def query_luckin_order_detail(order_id: str) -> dict[str, Any]:
    if not SETTINGS.get("luckin_mcp_enabled"):
        raise LuckinOrderError("瑞幸 MCP 还没有启用。")
    if not _luckin_token():
        raise LuckinOrderError("瑞幸 MCP Token 为空。")

    clean_order_id = _as_text(order_id)
    if not clean_order_id:
        raise LuckinOrderError("订单号为空，无法查询。")
    if not re.fullmatch(r"[A-Za-z0-9_-]{4,80}", clean_order_id):
        raise LuckinOrderError("订单号格式不正确。")

    await _ensure_luckin_server()
    detail_data = await _call_luckin_tool("queryOrderDetailInfo", {"orderId": clean_order_id})
    code_info = _find_value(detail_data, ("takeMealCodeInfo",))
    if not isinstance(code_info, dict):
        code_info = {}
    shop_info = _find_value(detail_data, ("shopInfo",))
    if not isinstance(shop_info, dict):
        shop_info = {}

    return {
        "ok": True,
        "order_id": clean_order_id,
        "order_status": _find_value(detail_data, ("orderStatus",)),
        "order_status_name": _as_text(_find_value(detail_data, ("orderStatusName",))),
        "about_time": _as_text(_find_value(detail_data, ("aboutTime",))),
        "take_meal_time": _as_text(_find_value(detail_data, ("takeMealTime",))),
        "take_meal_code": _as_text(_first_present(code_info, ("code", "takeMealCode", "mealCode", "pickupCode"))),
        "take_order_id": _as_text(_first_present(code_info, ("takeOrderId", "takeMealOrderId", "pickupOrderId"))),
        "shop_name": _shop_name(shop_info),
        "shop_address": _shop_address(shop_info),
    }


def render_luckin_result(result: dict[str, Any]) -> str:
    if not result.get("ok"):
        return f"瑞幸咖啡下单未完成：{result.get('message') or '未知错误'}"

    lines = ["瑞幸咖啡订单已创建，等你确认支付。"]
    if result.get("product_name"):
        product_line = f"- 商品：{result['product_name']}"
        if result.get("attrs"):
            product_line += f"（{result['attrs']}）"
        if result.get("amount", 1) != 1:
            product_line += f" x{result['amount']}"
        lines.append(product_line)
    if result.get("shop_name"):
        shop_line = f"- 门店：{result['shop_name']}"
        if result.get("shop_address"):
            shop_line += f"｜{result['shop_address']}"
        lines.append(shop_line)

    discount_price = _format_money(result.get("discount_price"))
    if discount_price:
        lines.append(f"- 金额：{discount_price}")
    if result.get("order_id"):
        lines.append(f"- 订单号：{result['order_id']}")

    if result.get("pay_qr_url"):
        lines.append("下方二维码可直接扫码支付；支付前再核对门店。")
    elif result.get("pay_url"):
        lines.append("下方卡片里有备用支付入口。")
    else:
        lines.append("MCP 未返回支付入口；请到瑞幸订单页确认。")
    warnings = [str(w).strip() for w in (result.get("warnings") or []) if str(w).strip()]
    if warnings:
        lines.append("备注：" + "；".join(warnings))
    return "\n".join(lines)


def luckin_payment_attachments(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    attachments = []
    for result in results or []:
        if not result.get("ok"):
            continue
        qr_url = _as_text(result.get("pay_qr_url"))
        pay_url = _as_text(result.get("pay_url"))
        if not qr_url and not pay_url:
            continue
        title = _as_text(result.get("product_name")) or "瑞幸咖啡订单"
        attrs = _as_text(result.get("attrs"))
        if attrs:
            title = f"{title}（{attrs}）"
        attachments.append({
            "type": "luckin_payment",
            "title": title,
            "shop": _as_text(result.get("shop_name")),
            "address": _as_text(result.get("shop_address")),
            "amount": _format_money(result.get("discount_price")),
            "order_id": _as_text(result.get("order_id")),
            "note": "；".join(str(w).strip() for w in (result.get("warnings") or []) if str(w).strip()),
            "qr_url": qr_url,
            "pay_url": pay_url,
            "url": qr_url,
        })
    return attachments


async def handle_luckin_commands(text: str, *, append_summary: bool = False) -> tuple[str, list[dict[str, Any]]]:
    matches = [m.strip() for m in LUCKIN_CMD_PATTERN.findall(text or "") if m.strip()]
    if not matches:
        return text, []

    cleaned = LUCKIN_CMD_PATTERN.sub("", text or "").strip()
    results: list[dict[str, Any]] = []
    for raw in matches:
        try:
            result = await place_luckin_order(raw)
        except Exception as exc:
            result = {"ok": False, "query": raw, "message": str(exc)}
        results.append(result)

    if append_summary:
        summaries = [render_luckin_result(result) for result in results]
        if summaries:
            cleaned = (cleaned + "\n\n" + "\n\n".join(summaries)).strip()
    elif not cleaned and any(result.get("ok") for result in results):
        has_warning = any(result.get("warnings") for result in results if result.get("ok"))
        cleaned = "瑞幸咖啡订单已创建，但有规格提醒，扫码前请看下面卡片。" if has_warning else "瑞幸咖啡订单已创建，下面扫码确认支付。"
    elif not cleaned and results:
        cleaned = "\n".join(render_luckin_result(result) for result in results if not result.get("ok")).strip()
    return cleaned, results


async def process_luckin_commands(text: str) -> str:
    cleaned, _ = await handle_luckin_commands(text, append_summary=True)
    return cleaned
