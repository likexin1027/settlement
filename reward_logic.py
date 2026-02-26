import io
from pathlib import Path
from typing import cast

import pandas as pd  # type: ignore[reportMissingTypeStubs]





# 默认奖励梯度表（可在前端被用户调整）
DEFAULT_REWARD_TABLE: pd.DataFrame = pd.DataFrame(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
    [
        {"梯度": "100w+", "阈值": 1_000_000, "抖音/视频号": 300, "小红书": 900, "B站": 1800},
        {"梯度": "50w+", "阈值": 500_000, "抖音/视频号": 150, "小红书": 450, "B站": 900},
        {"梯度": "20w+", "阈值": 200_000, "抖音/视频号": 60, "小红书": 180, "B站": 360},
        {"梯度": "10w+", "阈值": 100_000, "抖音/视频号": 30, "小红书": 90, "B站": 180},
        {"梯度": "5w+", "阈值": 50_000, "抖音/视频号": 15, "小红书": 45, "B站": 90},
        {"梯度": "3w+", "阈值": 30_000, "抖音/视频号": 10, "小红书": 30, "B站": 60},
        {"梯度": "1w+", "阈值": 10_000, "抖音/视频号": 5, "小红书": 15, "B站": 30},
    ]
)

DEFAULT_QUALITY_RULES: list[dict[str, object]] = [
    {"名称": "短视频点赞≥10w", "字段": "点赞", "阈值": 100_000, "加成": 300, "仅短视频": True, "适用渠道": "全部"},
    {"名称": "播放≥200w", "字段": "播放量", "阈值": 2_000_000, "加成": 1000, "仅短视频": False, "适用渠道": "全部"},
    {"名称": "评论≥5000", "字段": "评论数", "阈值": 5_000, "加成": 200, "仅短视频": False, "适用渠道": "全部"},
]

DEFAULT_TIME_RULES: list[dict[str, object]] = [
    {
        "名称": "热点/主题加50",
        "关键词": ["热点推荐", "新春主题", "长期主题", "2月主题"],
        "播放下限": 10_000,
        "加成": 50,
    }
]

DEFAULT_BASE_MODE = "档位"
DEFAULT_BASE_PARAMS: dict[str, object] = {
    "mode": DEFAULT_BASE_MODE,
    "tiers": DEFAULT_REWARD_TABLE.to_dict(orient="records"),
    "cpm": {"抖音/视频号": 0.30, "小红书": 0.90, "B站": 1.80},
    "pool": {"total": 10000.0, "min_play": 10000.0},
}

REWARD_VERSION = "v20250305"



# 基础必需字段（渠道、播放量、作品类型）

REQUIRED_BASE_COLUMNS = ["渠道", "播放量", "作品类型"]
# 身份字段至少需要有一个（按账号标识做分组与封顶）
IDENTITY_COLUMNS = ["账号ID", "账号名称", "账号昵称"]
OPTIONAL_TEXT_COLUMNS = [
    "作品标题",
    "标题",
    "内容",
    "备注",
    "视频标题",
    "视频链接",
]

COLUMN_ALIASES = {
    "平台": "渠道",
    "平台渠道": "渠道",
    "channel": "渠道",
    "播放数": "播放量",
    "播放": "播放量",
    "阅读数": "播放量",
    "阅读量": "播放量",
    "播放量(次)": "播放量",
    "浏览量": "播放量",
    "点赞数": "点赞",
    "点赞量": "点赞",
    "喜欢": "点赞",
    "love": "点赞",
    "comments": "评论数",
    "评论": "评论数",
    "评论量": "评论数",
    "视频类型": "作品类型",
    "类型": "作品类型",
    "账号": "账号名称",
    "作者": "账号名称",
    "作者昵称": "账号名称",
    "达人昵称": "账号名称",
    "博号昵称": "账号名称",
    "昵称": "账号昵称",
    "达人ID": "账号ID",
    "作者ID": "账号ID",
    "UID": "账号ID",
    "uid": "账号ID",
    "期次": "期数",
    "批次": "期数",
    "轮次": "期数",
    "期别": "期数",
    "热搜": "B站热搜",
    "热门": "B站热门",
    "是否B站热搜": "B站热搜",
    "是否热搜": "B站热搜",
    "是否B站热门": "B站热门",
    "是否热门": "B站热门",
}

EXCLUDE_KEYWORDS = ["bug", "建议", "拉踩"]


DATA_DIR = Path(__file__).resolve().parent / "data"
SAMPLE_DATA_PATH = DATA_DIR / "sample_data.csv"


def load_sample_data() -> pd.DataFrame:  # pyright: ignore[reportUnknownParameterType, reportUnknownMemberType]
    """读取示例数据（用于无上传时的体验）。"""
    return cast(pd.DataFrame, pd.read_csv(SAMPLE_DATA_PATH))





def normalize_channel(raw: object) -> str:
    if not isinstance(raw, str):
        return ""
    text = raw.strip().lower()

    if any(k in text for k in ["抖音", "douyin", "视频号", "wechat video", "wx视频号"]):
        return "抖音/视频号"
    if "小红书" in text or "xhs" in text or text == "red":
        return "小红书"
    if "b站" in text or "bilibili" in text or "哔哩" in text:
        return "B站"
    return raw.strip()


def align_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping: dict[str, str] = {}
    for col in df.columns:
        key = str(col).strip()
        target = COLUMN_ALIASES.get(key, key)
        mapping[col] = target
    return df.rename(columns=mapping)


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def pick_base_reward(channel: str, plays: float, reward_table: pd.DataFrame) -> float:
    if not channel:
        return 0
    chan = normalize_channel(channel)
    table = reward_table.sort_values("阈值", ascending=False)
    for _, row in table.iterrows():
        if plays >= row["阈值"]:
            return float(row.get(chan, 0) or 0)
    return 0.0


def detect_exclusion(row: pd.Series, text_columns: list[str]) -> str | None:
    haystack = " ".join(str(row.get(col, "")) for col in text_columns).lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw.lower() in haystack:
            return f"含排除关键词:{kw}"
    return None


def bool_from_any(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "是", "有", "热搜", "热门"}


def coalesce_row(row: pd.Series, cols: list[str]) -> str:
    for col in cols:
        if col in row and pd.notna(row[col]) and str(row[col]).strip() != "":
            return str(row[col]).strip()
    return ""




def _extract_rule_config(rule: object) -> tuple[pd.DataFrame, list[dict[str, object]], list[dict[str, object]], str, dict[str, object]]:
    if isinstance(rule, pd.DataFrame):
        return rule, DEFAULT_QUALITY_RULES, DEFAULT_TIME_RULES, DEFAULT_BASE_MODE, DEFAULT_BASE_PARAMS
    if isinstance(rule, dict):
        base_mode = str(rule.get("base_mode", rule.get("mode", DEFAULT_BASE_MODE)))
        base_params = cast(dict[str, object], rule.get("base_params", DEFAULT_BASE_PARAMS))
        base_table_obj = (
            base_params.get("tiers")
            or rule.get("table")
            or rule.get("base_table")
            or DEFAULT_REWARD_TABLE.to_dict(orient="records")
        )
        base_df = base_table_obj if isinstance(base_table_obj, pd.DataFrame) else pd.DataFrame(base_table_obj)
        quality_rules = rule.get("quality_rules") or rule.get("excellent_rules") or DEFAULT_QUALITY_RULES
        time_rules = rule.get("time_rules") or rule.get("extra_rules") or DEFAULT_TIME_RULES
        return base_df, cast(list[dict[str, object]], quality_rules), cast(list[dict[str, object]], time_rules), base_mode, base_params
    return DEFAULT_REWARD_TABLE, DEFAULT_QUALITY_RULES, DEFAULT_TIME_RULES, DEFAULT_BASE_MODE, DEFAULT_BASE_PARAMS


def compute_rewards(df: pd.DataFrame, rule: object) -> pd.DataFrame:
    base_table, quality_rules, time_rules, base_mode, base_params = _extract_rule_config(rule)
    work = align_columns(df.copy())



    missing_base = [c for c in REQUIRED_BASE_COLUMNS if c not in work.columns]
    if missing_base:
        raise ValueError(f"缺少必要字段: {missing_base}")

    has_identity = any(col in work.columns for col in IDENTITY_COLUMNS)
    if not has_identity:
        raise ValueError("缺少账号标识（账号ID/账号名称/账号昵称 至少一列）")

    work["渠道"] = work["渠道"].apply(normalize_channel)


    work["播放量"] = to_numeric(work["播放量"])
    if "点赞" in work.columns:
        work["点赞"] = to_numeric(work["点赞"])
    else:
        work["点赞"] = 0
    if "评论数" in work.columns:
        work["评论数"] = to_numeric(work["评论数"])
    else:
        work["评论数"] = 0

    # 期数/批次处理，默认“默认”
    if "期数" not in work.columns:
        work["期数"] = "默认"
    work["期数"] = work["期数"].fillna("默认").astype(str)

    # 账号标识：优先 账号ID，其次账号名称，再次账号昵称
    def pick_identity(row: pd.Series) -> str:
        ident = coalesce_row(row, ["账号ID", "账号名称", "账号昵称"])
        return ident or "未知账号"

    work["账号标识"] = work.apply(pick_identity, axis=1)

    text_columns = [c for c in REQUIRED_BASE_COLUMNS + OPTIONAL_TEXT_COLUMNS if c in work.columns]
    work["排除原因"] = work.apply(lambda r: detect_exclusion(r, text_columns), axis=1)

    # 基础奖励三种模式
    def calc_base_reward(r: pd.Series) -> float:
        mode = base_mode or DEFAULT_BASE_MODE
        if mode == "CPM":
            cpm_cfg = cast(dict[str, object], base_params.get("cpm", {}))
            rate = float(cpm_cfg.get(r.get("渠道", ""), cpm_cfg.get("默认", 0)))
            plays = float(r.get("播放量", 0))
            return plays / 1000.0 * rate
        if mode == "瓜分":
            pool_cfg = cast(dict[str, object], base_params.get("pool", {}))
            pool_total = float(pool_cfg.get("total", 0))
            min_play = float(pool_cfg.get("min_play", 0))
            if plays_sum_ref[0] == 0:  # 预先计算后设置
                return 0.0
            if float(r.get("播放量", 0)) < min_play:
                return 0.0
            return pool_total * float(r.get("播放量", 0)) / plays_sum_ref[0]
        # 默认档位模式
        return pick_base_reward(r["渠道"], r["播放量"], base_table)

    # 若为瓜分模式，先计算符合门槛的播放总和
    plays_sum_ref = [0.0]
    if base_mode == "瓜分":
        pool_cfg = cast(dict[str, object], base_params.get("pool", {}))
        min_play = float(pool_cfg.get("min_play", 0))
        qualifies = work["播放量"] >= min_play
        plays_sum_ref[0] = float(work.loc[qualifies, "播放量"].sum())

    work["基础奖励"] = work.apply(calc_base_reward, axis=1)

    # 限时奖励：配置化规则，可累加
    def calc_time_bonus(r: pd.Series) -> float:
        bonus_total = 0.0
        text = str(r.get("作品类型", ""))
        plays = float(r.get("播放量", 0))
        for rule_item in time_rules:
            min_plays = float(rule_item.get("播放下限", 0))
            kw_list = rule_item.get("关键词") or []
            kw_list = kw_list if isinstance(kw_list, list) else [kw_list]
            add = float(rule_item.get("加成", 0))
            if plays < min_plays:
                continue
            if kw_list and not any(str(kw) in text for kw in kw_list):
                continue
            bonus_total += add
        return bonus_total

    work["限时奖励"] = work.apply(calc_time_bonus, axis=1)


    def bilibili_extra(r: pd.Series) -> int:

        if r["渠道"] != "B站":
            return 0
        text = str(r.get("作品类型", ""))
        hot_flag = bool_from_any(r.get("B站热门")) or bool_from_any(r.get("热门"))
        top_flag = bool_from_any(r.get("B站热搜")) or bool_from_any(r.get("热搜"))
        extra_list: list[int] = []
        if "热搜" in text:
            extra_list.append(100)
        if "热门" in text:
            extra_list.append(200)
        if top_flag:
            extra_list.append(100)
        if hot_flag:
            extra_list.append(200)
        return max(extra_list) if extra_list else 0

    work["平台加成"] = work.apply(bilibili_extra, axis=1)

    def quality_extra(r: pd.Series) -> float:
        bonus = 0.0
        type_str = str(r.get("作品类型", ""))
        is_short = "短视频" in type_str or r.get("渠道") in ["抖音/视频号", "小红书"]
        for rule_item in quality_rules:
            field = str(rule_item.get("字段", ""))
            threshold = float(rule_item.get("阈值", 0))
            add = float(rule_item.get("加成", 0))
            only_short = bool(rule_item.get("仅短视频", False))
            target_channel = str(rule_item.get("适用渠道", "全部"))
            if only_short and not is_short:
                continue
            if target_channel != "全部" and r.get("渠道") != target_channel:
                continue
            value = float(r.get(field, 0) or 0)
            if value >= threshold:
                bonus += add
        return bonus

    work["优质加成"] = work.apply(quality_extra, axis=1)

    work["总奖励"] = work[["基础奖励", "限时奖励", "平台加成", "优质加成"]].sum(axis=1)

    work["超额标记"] = ""

    # 作品标识：账号名称 + 视频标题（若缺则退化为账号标识 + 任意标题列）

    def pick_title(row: pd.Series) -> str:
        for col in ["视频标题", "作品标题", "标题"]:
            if col in row and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return "未命名作品"

    work["作品标识"] = work.apply(
        lambda r: f"{r.get('账号名称', r.get('账号标识', '未知账号'))}｜{pick_title(r)}",
        axis=1,
    )



    # 重新计算总奖励（基础/限时/平台加成不受封顶影响）
    work["总奖励"] = work[["基础奖励", "限时奖励", "平台加成", "优质加成"]].sum(axis=1)

    excluded_mask = work["排除原因"].notna()
    work.loc[excluded_mask, "总奖励"] = 0

    work = work.sort_values(["期数", "账号标识", "作品标识", "总奖励"], ascending=[True, True, True, False]).reset_index(drop=True)


    work["状态"] = work.apply(
        lambda r: r["排除原因"]
        if pd.notna(r["排除原因"])
        else (r["超额标记"] if r["超额标记"] else "计入"),
        axis=1,
    )

    display_cols = [
        "期数",
        "渠道",
        "账号标识",
        "账号ID",
        "账号名称",
        "账号昵称",
        "作品标识",
        "作品类型",
        "播放量",
        "点赞",
        "评论数",
        "基础奖励",
        "限时奖励",
        "平台加成",
        "优质加成",
        "总奖励",
        "状态",
    ]
    present_display = [c for c in display_cols if c in work.columns]
    extra_cols = [c for c in work.columns if c not in present_display]
    return work[present_display + extra_cols]




def build_download_buffer(result_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        result_df.to_excel(writer, index=False, sheet_name="结算结果")
    return output.getvalue()
