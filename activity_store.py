import json
import uuid
from pathlib import Path
from collections.abc import Mapping
from typing import NotRequired, TypedDict, cast


import pandas as pd


from reward_system.reward_logic import (
    DEFAULT_REWARD_TABLE,
    REWARD_VERSION,
    DEFAULT_QUALITY_RULES,
    DEFAULT_TIME_RULES,
    DEFAULT_BASE_MODE,
    DEFAULT_BASE_PARAMS,
)




class RuleVersion(TypedDict):
    id: str
    name: str
    version: str
    table: list[dict[str, object]]
    quality_rules: list[dict[str, object]]
    time_rules: list[dict[str, object]]
    base_mode: str
    base_params: dict[str, object]




class Activity(TypedDict):
    id: str
    name: str
    period: str
    start_date: str
    end_date: str
    status: str
    remark: NotRequired[str]
    rule_versions: list[RuleVersion]

  # pyright: ignore[reportUnknownVariableType]



DATA_DIR = Path(__file__).resolve().parent / "data"
ACTIVITIES_FILE = DATA_DIR / "activities.json"


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _default_rule() -> RuleVersion:
    return {
        "id": "rule_default",
        "name": "默认规则",
        "version": REWARD_VERSION,
        "table": cast(list[dict[str, object]], DEFAULT_REWARD_TABLE.to_dict(orient="records")),
        "quality_rules": cast(list[dict[str, object]], DEFAULT_QUALITY_RULES),
        "time_rules": cast(list[dict[str, object]], DEFAULT_TIME_RULES),
        "base_mode": DEFAULT_BASE_MODE,
        "base_params": cast(dict[str, object], DEFAULT_BASE_PARAMS),
    }




def _default_activity() -> Activity:
    return {
        "id": str(uuid.uuid4()),
        "name": "默认活动",
        "period": "第一期",
        "start_date": "",
        "end_date": "",
        "status": "进行中",
        "remark": "系统自动创建的默认活动",
        "rule_versions": [_default_rule()],
    }




def ensure_activities_file() -> None:
    _ensure_data_dir()
    if not ACTIVITIES_FILE.exists():
        with ACTIVITIES_FILE.open("w", encoding="utf-8") as f:
            json.dump([_default_activity()], f, ensure_ascii=False, indent=2)


def _normalize_rule_versions(rules: object) -> list[RuleVersion]:
    result: list[RuleVersion] = []
    if not isinstance(rules, list):
        return [_default_rule()]
    for item in rules:
        if isinstance(item, dict):
            result.append(
                {
                    "id": str(item.get("id", "rule_default")),
                    "name": str(item.get("name", "默认规则")),
                    "version": str(item.get("version", REWARD_VERSION)),
                    "table": cast(
                        list[dict[str, object]],
                        item.get("table") or DEFAULT_REWARD_TABLE.to_dict(orient="records"),
                    ),
                    "quality_rules": cast(
                        list[dict[str, object]], item.get("quality_rules") or DEFAULT_QUALITY_RULES
                    ),
                    "time_rules": cast(list[dict[str, object]], item.get("time_rules") or DEFAULT_TIME_RULES),
                    "base_mode": str(item.get("base_mode", DEFAULT_BASE_MODE)),
                    "base_params": cast(dict[str, object], item.get("base_params", DEFAULT_BASE_PARAMS)),
                }
            )


    return result or [_default_rule()]


def load_activities() -> list[Activity]:
    ensure_activities_file()
    with ACTIVITIES_FILE.open("r", encoding="utf-8") as f:
        raw_data = json.load(f)
    data: list[Activity] = raw_data if isinstance(raw_data, list) else []
    if not data:
        data = [_default_activity()]

    changed = False
    for act in data:
        rules = _normalize_rule_versions(act.get("rule_versions"))
        if rules and rules[0].get("version") != REWARD_VERSION:
            rules[0]["table"] = cast(list[dict[str, object]], DEFAULT_REWARD_TABLE.to_dict(orient="records"))
            rules[0]["quality_rules"] = cast(list[dict[str, object]], DEFAULT_QUALITY_RULES)
            rules[0]["time_rules"] = cast(list[dict[str, object]], DEFAULT_TIME_RULES)
            rules[0]["base_mode"] = DEFAULT_BASE_MODE
            rules[0]["base_params"] = cast(dict[str, object], DEFAULT_BASE_PARAMS)
            rules[0]["version"] = REWARD_VERSION
            changed = True
        # 兼容旧数据缺少新字段
        if rules:
            if "quality_rules" not in rules[0]:
                rules[0]["quality_rules"] = cast(list[dict[str, object]], DEFAULT_QUALITY_RULES)
                changed = True
            if "time_rules" not in rules[0]:
                rules[0]["time_rules"] = cast(list[dict[str, object]], DEFAULT_TIME_RULES)
                changed = True
            if "base_mode" not in rules[0]:
                rules[0]["base_mode"] = DEFAULT_BASE_MODE
                changed = True
            if "base_params" not in rules[0]:
                rules[0]["base_params"] = cast(dict[str, object], DEFAULT_BASE_PARAMS)
                changed = True
        act["rule_versions"] = rules


    if changed:
        save_activities(data)
    return data




def save_activities(activities: list[Activity]) -> None:

    _ensure_data_dir()
    with ACTIVITIES_FILE.open("w", encoding="utf-8") as f:
        json.dump(activities, f, ensure_ascii=False, indent=2)


def add_activity(payload: Mapping[str, str]) -> Activity:
    activities = load_activities()
    new_activity: Activity = {
        "id": str(uuid.uuid4()),
        "name": payload.get("name", "新活动"),
        "period": payload.get("period", ""),
        "start_date": payload.get("start_date", ""),
        "end_date": payload.get("end_date", ""),
        "status": payload.get("status", "草稿"),
        "remark": payload.get("remark", ""),
        "rule_versions": [_default_rule()],
    }

    activities.append(new_activity)
    save_activities(activities)
    return new_activity



def update_activity_rule(
    activity_id: str,
    table_df: pd.DataFrame,
    quality_rules: list[dict[str, object]] | None = None,
    time_rules: list[dict[str, object]] | None = None,
    base_mode: str | None = None,
    base_params: dict[str, object] | None = None,
) -> None:

    activities = load_activities()
    for act in activities:
        if act.get("id") == activity_id:
            rules = act.get("rule_versions") or [_default_rule()]
            table_records = cast(list[dict[str, object]], table_df.to_dict(orient="records"))
            rules[0]["table"] = table_records
            if quality_rules is not None:
                rules[0]["quality_rules"] = cast(list[dict[str, object]], quality_rules)
            if time_rules is not None:
                rules[0]["time_rules"] = cast(list[dict[str, object]], time_rules)
            if base_mode is not None:
                rules[0]["base_mode"] = base_mode
            if base_params is not None:
                rules[0]["base_params"] = base_params
            act["rule_versions"] = cast(list[RuleVersion], rules)
            break
    save_activities(activities)





def update_activity_meta(activity_id: str, payload: Mapping[str, str]) -> None:
    activities = load_activities()
    for act in activities:
        if act.get("id") == activity_id:
            act["name"] = payload.get("name", act.get("name", ""))
            act["period"] = payload.get("period", act.get("period", ""))
            act["start_date"] = payload.get("start_date", act.get("start_date", ""))
            act["end_date"] = payload.get("end_date", act.get("end_date", ""))
            act["status"] = payload.get("status", act.get("status", ""))
            act["remark"] = payload.get("remark", act.get("remark", ""))
            if not act.get("rule_versions"):
                act["rule_versions"] = [_default_rule()]
            break
    save_activities(activities)



def get_activity_by_id(activity_id: str) -> Activity | None:

    for act in load_activities():
        if act.get("id") == activity_id:
            return act
    return None


def delete_activity(activity_id: str) -> None:

    activities = load_activities()
    # 至少保留 1 个活动，避免空列表导致界面无法使用
    if len(activities) <= 1:
        raise ValueError("至少保留一个活动，无法删除最后一个活动")
    filtered = [act for act in activities if act.get("id") != activity_id]
    # 若未找到匹配则不写回
    if len(filtered) == len(activities):
        return
    save_activities(filtered)

