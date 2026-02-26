import io
import tempfile
import csv
import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from openpyxl import load_workbook  # type: ignore
from typing import TYPE_CHECKING, cast



if TYPE_CHECKING:
    from typing import Any as Xlsx2csvType  # 避免开发环境未安装 xlsx2csv 的类型导入报错  # pyright: ignore[reportUnusedImport]


try:
    import importlib

    _xlsx2csv = importlib.import_module("xlsx2csv")
    Xlsx2csv = getattr(_xlsx2csv, "Xlsx2csv", None)
    HAS_XLSX2CSV = Xlsx2csv is not None
except Exception:  # pragma: no cover
    Xlsx2csv = None  # type: ignore
    HAS_XLSX2CSV = False  # pyright: ignore[reportConstantRedefinition]



HAS_CALAMINE = False  # calamine 非必需；如需更强兼容可自行安装

from reward_system.reward_logic import (
    DEFAULT_REWARD_TABLE,  # pyright: ignore[reportUnknownVariableType]
    DEFAULT_QUALITY_RULES,
    DEFAULT_TIME_RULES,
    build_download_buffer,  # pyright: ignore[reportUnknownVariableType]
    compute_rewards,  # pyright: ignore[reportUnknownVariableType]
    load_sample_data,  # pyright: ignore[reportUnknownVariableType]
)

from reward_system.activity_store import (
    add_activity,
    get_activity_by_id,
    load_activities,
    update_activity_rule,  # pyright: ignore[reportUnknownVariableType]
    delete_activity,
    update_activity_meta,
)




BASE_DIR = Path(__file__).resolve().parent

st.set_page_config(page_title="活动奖励计算系统 | reward_system", layout="wide")

REQUIRED_HINT = (
    "必需：渠道/平台、播放量、作品类型，且账号ID/账号名称/账号昵称 至少一列。"
    "可选：点赞、评论数、期数、视频标题/作品标题、B站热搜/热门。"
)

STATUS_BADGE = {
    "草稿": "⚪️ 草稿",
    "进行中": "🟢 进行中",
    "已结束": "🔵 已结束",
}


def _parse_date(value: str | None) -> datetime.date | None:

    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value)
    except Exception:
        return None


def show_friendly_excel_error(message: str | None = None) -> None:

    tips = (
        "📁 文件读取失败：可能是腾讯文档/金山文档/WPS 导出的 Excel 样式不兼容。\n"
        "解决步骤：\n"
        "1) 在 Excel/WPS 打开后，‘文件→另存为’，格式选 CSV UTF-8 (.csv)，再上传。\n"
        "2) 或在 Excel 打开后，另存为新的 .xlsx，再上传。\n"
        "如果问题仍然存在，请检查文件是否损坏或联系管理员。"
    )
    if message:
        tips += f"\n（提示：{message}）"
    st.error(tips)




def read_excel_with_fallback(file_bytes: bytes) -> pd.DataFrame | None:  # pyright: ignore[reportUnknownParameterType, reportUnknownMemberType]
    # 优先 openpyxl（含样式时可能报 Fill 相关错误）
    try:
        return cast(pd.DataFrame, pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl"))  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    except Exception as exc_primary:  # noqa: BLE001
        msg = str(exc_primary).lower()
        if "fill" in msg or "openpyxl" in msg:
            st.info("检测到样式问题，正在尝试自动转换为 CSV 再读取……")  # pyright: ignore[reportUnusedCallResult, reportUnnecessaryTypeIgnoreComment]
        # 其次使用 xlsx2csv 仅读取值，不解析样式，兼容腾讯文档导出
        try:
            if not HAS_XLSX2CSV:
                raise RuntimeError("未安装 xlsx2csv，跳过自动转换。")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", newline="") as tmp:
                tmp_path = tmp.name
                Xlsx2csv(io.BytesIO(file_bytes), outputencoding="utf-8").convert(tmp_path)  # pyright: ignore[reportAny, reportOptionalCall]

            with open(tmp_path, "r", encoding="utf-8", newline="") as f:
                rows = list(csv.reader(f))
            if not rows:
                raise RuntimeError("文件内容为空或无法转换。")
            header, *body = rows
            header_seq = [str(h) for h in header]
            return cast(pd.DataFrame, pd.DataFrame(body, columns=header_seq))





        except Exception:
            # 最后使用低级 openpyxl 兼容模式（忽略样式）
            try:
                wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
                sheet = wb.active
                if sheet is None:
                    raise RuntimeError("未找到工作表")
                data = list(sheet.values)
                if not data:
                    raise RuntimeError("工作表为空")
                header, *rows = data
                header_seq = [str(h) for h in header]
                return cast(pd.DataFrame, pd.DataFrame(rows, columns=header_seq))

            except Exception:

                show_friendly_excel_error("自动转换/兼容模式未成功，需按提示另存为后再上传。")
                return None


    return None







def read_uploaded_file(file: io.BytesIO, name: str) -> pd.DataFrame | None:
    suffix = name.lower()
    if suffix.endswith(":memory:"):
        suffix = suffix[:-8]
    if suffix.endswith(".xlsx") or suffix.endswith(".xls"):
        return read_excel_with_fallback(file.read())
    return cast(pd.DataFrame, pd.read_csv(file))






def main() -> None:
    activities = load_activities()
    if "current_activity_id" not in st.session_state:
        st.session_state.current_activity_id = activities[0]["id"] if activities else ""

    # 活动管理核心区（常驻展开）
    st.sidebar.header("活动管理核心区")

    # 新建活动（可折叠，紧凑布局）
    with st.sidebar.expander("新建活动", expanded=False):
        with st.form("create_activity_form"):
            c1, c2 = st.columns(2)
            name = c1.text_input("活动名称", value="", key="create_name")
            period = c2.text_input("期数", value="", key="create_period")

            d1, d2 = st.columns(2)
            start_date_input = d1.date_input("开始日期", value=None, key="create_start")
            end_date_input = d2.date_input("结束日期", value=None, key="create_end")

            c3, c4 = st.columns(2)
            status_new = c3.selectbox("状态", ["草稿", "进行中", "已结束"], index=0, key="create_status")
            remark_new = c4.text_input("备注", value="", key="create_remark")

            submitted = st.form_submit_button("创建活动")



    if submitted:
        payload = {
            "name": name or "新活动",
            "period": period,
            "start_date": str(start_date_input) if start_date_input else "",
            "end_date": str(end_date_input) if end_date_input else "",
            "status": status_new,
            "remark": remark_new,
        }
        new_activity = add_activity(payload)
        st.session_state.current_activity_id = new_activity["id"]
        st.success("已创建活动")
        st.rerun()

    if not activities:
        st.sidebar.error("未找到活动，请新建一个活动")
        st.stop()

    # 活动下拉选择（含状态色标）
    option_labels = [
        f"{STATUS_BADGE.get(a.get('status',''), '⚪️')} {a['name']}｜{a.get('period','未设期数')}"
        for a in activities
    ]
    option_map = {label: act["id"] for label, act in zip(option_labels, activities)}
    current_label = next((lbl for lbl, aid in option_map.items() if aid == st.session_state.current_activity_id), option_labels[0])
    selected_label = st.sidebar.selectbox("选择活动", option_labels, index=option_labels.index(current_label))
    st.session_state.current_activity_id = option_map[selected_label]


    current_activity = get_activity_by_id(st.session_state.current_activity_id) or activities[0]

    st.title(f"活动奖励计算系统 - {current_activity.get('name', '未命名')}")
    st.caption("上传作品数据 → 调整梯度 → 预览结果 → 下载Excel")

    st.sidebar.markdown(
        f"**当前活动：** {STATUS_BADGE.get(current_activity.get('status',''), '⚪️')} {current_activity.get('name','未命名')}"
    )

    # 当前活动详情卡片（直接编辑，紧凑布局）
    st.sidebar.markdown("**当前活动详情**")

    c1, c2 = st.sidebar.columns(2)
    name_edit = c1.text_input("名称", value=current_activity.get("name", ""), key="act_name")
    period_edit = c2.text_input("期数", value=current_activity.get("period", ""), key="act_period")

    d1, d2 = st.sidebar.columns(2)
    start_date_val = _parse_date(current_activity.get("start_date"))
    end_date_val = _parse_date(current_activity.get("end_date"))
    start_date_edit = d1.date_input("开始日期", value=start_date_val, key="act_start")
    end_date_edit = d2.date_input("结束日期", value=end_date_val, key="act_end")

    c3, c4 = st.sidebar.columns(2)
    status_options = ["草稿", "进行中", "已结束"]
    status_edit = c3.selectbox(
        "状态",
        status_options,
        index=status_options.index(current_activity.get("status", "草稿")),
        key="act_status",
        format_func=lambda s: STATUS_BADGE.get(s, str(s)),
    )

    remark_edit = c4.text_input("备注", value=current_activity.get("remark", ""), key="act_remark")



    action_col1, action_col2 = st.sidebar.columns(2)
    if action_col1.button("保存活动信息"):
        update_activity_meta(
            current_activity["id"],
            {
                "name": name_edit,
                "period": period_edit,
                "start_date": str(start_date_edit) if start_date_edit else "",
                "end_date": str(end_date_edit) if end_date_edit else "",
                "status": status_edit,
                "remark": remark_edit,
            },
        )
        st.success("活动信息已更新")
        st.rerun()

    # 删除当前活动（两步确认弹窗式体验，同行放置）
    if "show_delete_confirm" not in st.session_state:
        st.session_state.show_delete_confirm = False

    if action_col2.button("🗑️ 删除当前活动"):
        st.session_state.show_delete_confirm = True

    if st.session_state.show_delete_confirm:
        st.sidebar.warning("确认删除当前活动？此操作不可恢复，且至少保留1个活动。")
        dc1, dc2 = st.sidebar.columns(2)
        if dc1.button("确认删除", key="confirm_delete_btn"):
            try:
                delete_activity(current_activity["id"])
            except Exception as exc:  # noqa: BLE001
                st.sidebar.error(str(exc))
            else:
                remaining = load_activities()
                if remaining:
                    st.session_state.current_activity_id = remaining[0]["id"]
                st.success("活动已删除")
            st.session_state.show_delete_confirm = False
            st.rerun()
        if dc2.button("取消", key="cancel_delete_btn"):
            st.session_state.show_delete_confirm = False
            st.rerun()





    # 梯度与规则（主区）
    st.subheader("梯度与规则")
    st.markdown("可在下表中直接修改奖励金额或阈值，阈值需为数字。")

    rule_versions = current_activity.get("rule_versions") or []
    rule_cfg = rule_versions[0] if rule_versions else {
        "table": DEFAULT_REWARD_TABLE.to_dict(orient="records"),
        "quality_rules": DEFAULT_QUALITY_RULES,
        "time_rules": DEFAULT_TIME_RULES,
    }

    current_rule_table: list[dict[str, object]] = cast(
        list[dict[str, object]], rule_cfg.get("table") or DEFAULT_REWARD_TABLE.to_dict(orient="records")
    )
    quality_rules_data: list[dict[str, object]] = cast(
        list[dict[str, object]], rule_cfg.get("quality_rules") or DEFAULT_QUALITY_RULES
    )
    time_rules_data: list[dict[str, object]] = cast(
        list[dict[str, object]], rule_cfg.get("time_rules") or DEFAULT_TIME_RULES
    )

    base_tab, quality_tab, time_tab = st.tabs(["基础奖励", "优秀奖励", "限时奖励"])

    # 先初始化容器，供保存时读取
    reward_table: pd.DataFrame = pd.DataFrame(current_rule_table)
    quality_table: pd.DataFrame = pd.DataFrame(quality_rules_data)
    time_table: pd.DataFrame = pd.DataFrame(time_rules_data)
    base_mode = str(rule_cfg.get("base_mode", "档位"))
    base_params = cast(dict[str, object], rule_cfg.get("base_params", {}))
    cpm_cfg = cast(dict[str, float], base_params.get("cpm", {}))
    pool_cfg = cast(dict[str, float], base_params.get("pool", {}))

    with base_tab:
        base_mode = st.radio(
            "基础奖励计算模式",
            ["档位", "CPM", "瓜分"],
            horizontal=True,
            index=["档位", "CPM", "瓜分"].index(base_mode if base_mode in ["档位", "CPM", "瓜分"] else "档位"),
            key="base_mode_radio",
        )

        if base_mode == "档位":
            st.markdown("**档位配置**")
            reward_table = st.data_editor(
                pd.DataFrame(current_rule_table),
                num_rows="dynamic",
                width="stretch",
                hide_index=True,
                key="tier_editor",
                column_config={"阈值": st.column_config.NumberColumn("阈值", format="%d", help="对应播放量下限")},
            )
        elif base_mode == "CPM":
            st.markdown("**CPM 配置（元/千次）**")
            col1, col2, col3 = st.columns(3)
            rate_dy = col1.number_input("抖音/视频号 CPM", value=float(cpm_cfg.get("抖音/视频号", 0.30)), step=0.1, format="%0.2f")
            rate_xhs = col2.number_input("小红书 CPM", value=float(cpm_cfg.get("小红书", 0.90)), step=0.1, format="%0.2f")
            rate_bili = col3.number_input("B站 CPM", value=float(cpm_cfg.get("B站", 1.80)), step=0.1, format="%0.2f")
            cpm_cfg = {"抖音/视频号": rate_dy, "小红书": rate_xhs, "B站": rate_bili}
        else:  # 瓜分
            st.markdown("**瓜分配置**")
            col1, col2 = st.columns(2)
            pool_total = col1.number_input("奖金池总额(元)", value=float(pool_cfg.get("total", 10000)), step=100.0)
            pool_min = col2.number_input("最低播放量门槛", value=float(pool_cfg.get("min_play", 10000)), step=100.0)
            pool_cfg = {"total": pool_total, "min_play": pool_min}

    with quality_tab:
        st.markdown("**优秀奖励规则**")
        quality_table = st.data_editor(
            pd.DataFrame(quality_rules_data),
            num_rows="dynamic",
            width="stretch",
            hide_index=True,
            key="quality_editor",
            column_config={
                "阈值": st.column_config.NumberColumn("阈值", format="%d"),
                "加成": st.column_config.NumberColumn("加成", format="%d"),
                "仅短视频": st.column_config.CheckboxColumn("仅短视频"),
            },
        )

    with time_tab:
        st.markdown("**限时奖励规则**")
        time_table = st.data_editor(
            pd.DataFrame(time_rules_data),
            num_rows="dynamic",
            width="stretch",
            hide_index=True,
            key="time_editor",
            column_config={
                "播放下限": st.column_config.NumberColumn("播放下限", format="%d"),
                "加成": st.column_config.NumberColumn("加成", format="%d"),
            },
        )

    if st.button("保存基础奖励配置"):
        # 过滤空行
        tiers_clean = [row for row in reward_table.to_dict(orient="records") if any(str(v).strip() for v in row.values())]
        update_activity_rule(
            current_activity["id"],
            pd.DataFrame(tiers_clean if base_mode == "档位" else reward_table),
            quality_rules=cast(list[dict[str, object]], quality_table.to_dict(orient="records")),
            time_rules=cast(list[dict[str, object]], time_table.to_dict(orient="records")),
            base_mode=base_mode,
            base_params={"tiers": tiers_clean, "cpm": cpm_cfg, "pool": pool_cfg},
        )
        st.success("基础奖励配置已保存到当前活动")




    # 数据上传区（默认展开）
    with st.sidebar.expander("📤 数据上传", expanded=True):
        st.caption("上传前请确认字段要求，支持拖拽或浏览上传。")
        st.markdown(REQUIRED_HINT)
        use_sample = st.button("使用示例数据", key="use_sample_btn")
        uploaded = st.file_uploader("上传数据文件 (CSV 或 Excel)", type=["csv", "xlsx", "xls"], key="uploader")

    # 辅助功能区（默认收起）
    with st.sidebar.expander("🛠️ 高级选项", expanded=False):
        st.caption("收纳不常用功能，减少干扰")
        st.button("导出活动配置", disabled=True, help="后续支持")
        st.button("导入活动配置", disabled=True, help="后续支持")
        st.button("清空当前数据", disabled=True, help="后续支持")

    if uploaded:
        df_uploaded = read_uploaded_file(io.BytesIO(uploaded.read()), uploaded.name)
        if df_uploaded is None:
            return
        df: pd.DataFrame = df_uploaded
        st.success(f"已加载文件：{uploaded.name}")
    elif use_sample:
        df = cast(pd.DataFrame, load_sample_data())
        st.info("已加载示例数据（reward_system/data/sample_data.csv）。")
    else:
        st.info("请在左侧上传文件，或点击“使用示例数据”。")
        st.stop()


    st.info(f"当前数据：{len(df)} 行，{len(df.columns)} 列。预览显示前 200 行（如有）。")
    with st.expander("查看原始数据", expanded=False):
        st.dataframe(df.head(200), use_container_width=True, height=360)

    rule_config_payload = {
        "table": reward_table,
        "quality_rules": quality_table.to_dict(orient="records"),
        "time_rules": time_table.to_dict(orient="records"),
        "base_mode": base_mode,
        "base_params": {"tiers": reward_table.to_dict(orient="records"), "cpm": cpm_cfg, "pool": pool_cfg},
    }

    try:
        result_df = cast(pd.DataFrame, compute_rewards(df, rule_config_payload))


    except Exception as exc:  # noqa: BLE001

        msg = str(exc)
        if "缺少必要字段" in msg or "缺少账号标识" in msg:
            st.error(f"数据缺列：{msg}。请按字段要求补充后重试。")
        else:
            st.error(f"计算出错：{msg}")
        return


    st.subheader("结算预览")
    total_award: float = float(result_df["总奖励"].sum())
    valid_count: int = int((result_df["总奖励"] > 0).sum())
    excluded_count: int = int((result_df["总奖励"] == 0).sum())


    col1, col2, col3 = st.columns(3)
    col1.metric("总发放金额（元）", f"{total_award:,.0f} 元")

    col2.metric("计入作品数", int(valid_count))
    col3.metric("未计入/0元", int(excluded_count))

    st.dataframe(result_df, use_container_width=True, height=560)

    st.subheader("下载结果")
    buffer = build_download_buffer(result_df)
    st.download_button(
        label="下载处理后的 Excel",
        data=buffer,
        file_name=f"{current_activity.get('name','activity')}_结算结果.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.caption(
        "规则摘要：基础梯度按渠道匹配阈值；热点/新春/长期/当月主题加50；B站热搜+100，热门+200（取其高，支持布尔列或文案）；短视频点赞≥10w加300，播放≥200w加1000，评论数≥5000加200；含 BUG/建议/拉踩 的记录不计入。"
    )



if __name__ == "__main__":
    main()

