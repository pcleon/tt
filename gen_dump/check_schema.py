#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySQL 8.0/5.7 数据库表结构复核比对工具 (自包含脚本).

本脚本用于在数据库迁移（如通过 migrate.py）之后，对源库（Source）与目标库（Target）的
表结构一致性进行全方位在线复核比对。

核心特性与比对维度：
1. 继承/复用 gen_dump/config.yaml 中的 migrations 映射配置与过滤规则；
2. 表存在性检查：精准识别目标库缺失表（Missing）与多余表（Extra）；
3. 结构化字段比对：核对字段类型、是否允许为 NULL、默认值、自增列标识（auto_increment）及字符集；
4. 键与索引比对：核验主键（Primary Key）及字段顺序、唯一索引与普通索引（包含列顺序及索引类型）；
5. 忽略动态计数：自动忽略 AUTO_INCREMENT=xxx 等动态表行数与计数值差异；
6. 存储引擎与字符集：对比表的存储引擎（Engine）与表级默认字符集（Collation）；
7. 视图与触发器：默认核验视图（VIEW）存在性，支持通过 --check-triggers 参数核验触发器；
8. 兼容性：严格兼容 Python 3.6+，不依赖除 PyMySQL、PyYAML 以外的外部库。

退出码规范：
- 0: 所有检查项完全一致；
- 1: 发现表结构或对象存在差异；
- 2: 数据库连接失败、配置异常或执行错误。

Usage:
    python3 check_schema.py [-c CONFIG] [--source SOURCE_IP] [--target TARGET_IP]
                            [-d DATABASE] [--check-triggers] [--no-color]
"""

import argparse
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor
import yaml

# 配置默认控制台日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SchemaChecker")

# 系统默认忽略的数据库集合（不进行复核比对）
SYSTEM_DATABASES: Set[str] = {
    "information_schema",
    "performance_schema",
    "mysql",
    "sys",
}

# 退出码常量定义
EXIT_CODE_SUCCESS = 0
EXIT_CODE_DIFF = 1
EXIT_CODE_ERROR = 2

# ANSI 颜色转义字符（支持终端高亮）
COLOR_RESET = "\033[0m"
COLOR_BOLD = "\033[1m"
COLOR_RED = "\033[31m"
COLOR_GREEN = "\033[32m"
COLOR_YELLOW = "\033[33m"
COLOR_BLUE = "\033[34m"
COLOR_CYAN = "\033[36m"


class ColorFormatter:
    """控制台彩色输出格式化辅助类."""

    def __init__(self, enable_color: bool = True) -> None:
        """初始化 ColorFormatter 实例.

        Args:
            enable_color: 是否启用 ANSI 终端彩色高亮输出.
        """
        self.enable_color = enable_color

    def red(self, text: str) -> str:
        """格式化红色文本.

        Args:
            text: 待格式化的字符串.

        Returns:
            带颜色代码的字符串.
        """
        return f"{COLOR_RED}{text}{COLOR_RESET}" if self.enable_color else text

    def green(self, text: str) -> str:
        """格式化绿色文本.

        Args:
            text: 待格式化的字符串.

        Returns:
            带颜色代码的字符串.
        """
        return f"{COLOR_GREEN}{text}{COLOR_RESET}" if self.enable_color else text

    def yellow(self, text: str) -> str:
        """格式化黄色文本.

        Args:
            text: 待格式化的字符串.

        Returns:
            带颜色代码的字符串.
        """
        return f"{COLOR_YELLOW}{text}{COLOR_RESET}" if self.enable_color else text

    def cyan(self, text: str) -> str:
        """格式化青色文本.

        Args:
            text: 待格式化的字符串.

        Returns:
            带颜色代码的字符串.
        """
        return f"{COLOR_CYAN}{text}{COLOR_RESET}" if self.enable_color else text

    def bold(self, text: str) -> str:
        """格式化粗体文本.

        Args:
            text: 待格式化的字符串.

        Returns:
            带粗体代码的字符串.
        """
        return f"{COLOR_BOLD}{text}{COLOR_RESET}" if self.enable_color else text


class ColumnDefinition:
    """表字段（列）元数据结构定义类."""

    def __init__(
        self,
        name: str,
        data_type: str,
        is_nullable: str,
        default_value: Optional[str],
        extra: str,
        collation: Optional[str],
    ) -> None:
        """初始化 ColumnDefinition 实例.

        Args:
            name: 字段名称.
            data_type: 完整字段类型（如 varchar(255), bigint(20) unsigned）.
            is_nullable: 是否允许为 NULL ('YES' 或 'NO').
            default_value: 字段默认值字符串表达形式.
            extra: 额外属性（如 auto_increment, DEFAULT_GENERATED）.
            collation: 字段级排序规则（Collation）.
        """
        self.name = name
        self.data_type = data_type.lower()
        self.is_nullable = is_nullable.upper()
        self.default_value = default_value
        self.extra = extra.lower()
        self.collation = collation

    def is_auto_increment(self) -> bool:
        """判断该字段是否具有自增属性.

        Returns:
            若包含 auto_increment 则返回 True，否则返回 False.
        """
        return "auto_increment" in self.extra


class IndexDefinition:
    """索引元数据结构定义类."""

    def __init__(
        self,
        name: str,
        non_unique: int,
        columns: List[str],
        index_type: str,
    ) -> None:
        """初始化 IndexDefinition 实例.

        Args:
            name: 索引名称（主键固定为 PRIMARY）.
            non_unique: 是否非唯一索引（0 表示唯一索引/主键，1 表示普通非唯一索引）.
            columns: 构成索引的字段名称列表（严格按照顺序排序）.
            index_type: 索引算法类型（如 BTREE, HASH, FULLTEXT）.
        """
        self.name = name
        self.non_unique = non_unique
        self.columns = columns
        self.index_type = index_type.upper()


class TableStructure:
    """表或视图完整结构对象定义类."""

    def __init__(
        self,
        name: str,
        table_type: str,
        engine: Optional[str] = None,
        collation: Optional[str] = None,
    ) -> None:
        """初始化 TableStructure 实例.

        Args:
            name: 表或视图名称.
            table_type: 对象类型 ('BASE TABLE' 或 'VIEW').
            engine: 存储引擎名称 (如 InnoDB).
            collation: 表级默认排序规则.
        """
        self.name = name
        self.table_type = table_type
        self.engine = engine.upper() if engine else None
        self.collation = collation
        self.columns: Dict[str, ColumnDefinition] = {}
        self.primary_key: List[str] = []
        self.indexes: Dict[str, IndexDefinition] = {}

    def is_view(self) -> bool:
        """判断该对象是否为视图.

        Returns:
            若为 VIEW 则返回 True，否则返回 False.
        """
        return self.table_type == "VIEW"


class TriggerDefinition:
    """触发器元数据结构定义类."""

    def __init__(
        self,
        name: str,
        event_table: str,
        action_timing: str,
        event_manipulation: str,
        action_statement: str,
    ) -> None:
        """初始化 TriggerDefinition 实例.

        Args:
            name: 触发器名称.
            event_table: 绑定的事件表名.
            action_timing: 触发时机 (BEFORE / AFTER).
            event_manipulation: 触发动作 (INSERT / UPDATE / DELETE).
            action_statement: 触发执行语句主体.
        """
        self.name = name
        self.event_table = event_table
        self.action_timing = action_timing.upper()
        self.event_manipulation = event_manipulation.upper()
        self.action_statement = action_statement.strip()


class TableDifference:
    """单个数据表的结构差异明细封装类."""

    def __init__(self, table_name: str) -> None:
        """初始化 TableDifference 实例.

        Args:
            table_name: 发生差异对比的数据表名称.
        """
        self.table_name = table_name
        self.engine_diff: Optional[Tuple[Optional[str], Optional[str]]] = None
        self.collation_diff: Optional[Tuple[Optional[str], Optional[str]]] = None
        self.missing_columns: List[str] = []
        self.extra_columns: List[str] = []
        self.column_mismatches: List[str] = []
        self.pk_diff: Optional[Tuple[List[str], List[str]]] = None
        self.missing_indexes: List[str] = []
        self.extra_indexes: List[str] = []
        self.index_mismatches: List[str] = []

    def has_difference(self) -> bool:
        """判断当前表是否存在任何结构差异.

        Returns:
            若存在结构不一致项返回 True，完全一致返回 False.
        """
        return bool(
            self.engine_diff
            or self.collation_diff
            or self.missing_columns
            or self.extra_columns
            or self.column_mismatches
            or self.pk_diff
            or self.missing_indexes
            or self.extra_indexes
            or self.index_mismatches
        )


class DatabaseDifference:
    """单个数据库的比对差异总揽封装类."""

    def __init__(self, db_name: str) -> None:
        """初始化 DatabaseDifference 实例.

        Args:
            db_name: 数据库名称.
        """
        self.db_name = db_name
        self.target_missing_db: bool = False
        self.missing_tables: List[str] = []
        self.extra_tables: List[str] = []
        self.table_diffs: Dict[str, TableDifference] = {}
        self.missing_views: List[str] = []
        self.extra_views: List[str] = []
        self.missing_triggers: List[str] = []
        self.extra_triggers: List[str] = []
        self.trigger_mismatches: List[str] = []

    def has_difference(self) -> bool:
        """判断当前数据库是否存在任何差异.

        Returns:
            若存在不一致返回 True，完全匹配返回 False.
        """
        if self.target_missing_db:
            return True
        if self.missing_tables or self.extra_tables:
            return True
        if self.missing_views or self.extra_views:
            return True
        if self.missing_triggers or self.extra_triggers or self.trigger_mismatches:
            return True
        for diff in self.table_diffs.values():
            if diff.has_difference():
                return True
        return False


class SchemaInspector:
    """数据库表结构元数据探测提取器."""

    def __init__(self, connection_params: Dict[str, Any]) -> None:
        """初始化 SchemaInspector 实例.

        Args:
            connection_params: PyMySQL 连接字典 (host, port, user, password).
        """
        self.params = connection_params

    def _get_connection(self) -> pymysql.Connection:
        """建立并返回 PyMySQL 数据库连接.

        Returns:
            有效的 PyMySQL 数据库连接对象.

        Raises:
            pymysql.MySQLError: 数据库连接失败时抛出.
        """
        return pymysql.connect(
            host=self.params["host"],
            port=int(self.params["port"]),
            user=self.params["user"],
            password=str(self.params["password"]),
            charset="utf8mb4",
            cursorclass=DictCursor,
            connect_timeout=5,
            autocommit=True,
        )

    def list_databases(
        self,
        ignore_dbs: Set[str],
        include_dbs: Set[str],
        db_filter: Optional[str] = None,
    ) -> List[str]:
        """查询实例上符合过滤规则的业务数据库清单.

        Args:
            ignore_dbs: 忽略的数据库黑名单.
            include_dbs: 指定包含的数据库白名单.
            db_filter: 命令行显式指定的单个数据库筛选条件.

        Returns:
            经过过滤后的数据库名称列表.
        """
        conn = self._get_connection()
        matched_dbs: List[str] = []
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW DATABASES")
                rows = cur.fetchall()
                for row in rows:
                    db_name = row["Database"]
                    # 1. 过滤系统内置库
                    if db_name in SYSTEM_DATABASES:
                        continue
                    # 2. 单库参数过滤
                    if db_filter and db_name != db_filter:
                        continue
                    # 3. 黑名单过滤
                    if db_name in ignore_dbs:
                        continue
                    # 4. 白名单过滤
                    if include_dbs and "*" not in include_dbs and db_name not in include_dbs:
                        continue
                    matched_dbs.append(db_name)
        finally:
            conn.close()

        matched_dbs.sort()
        return matched_dbs

    def check_database_exists(self, db_name: str) -> bool:
        """检查目标库是否存在指定名称的数据库.

        Args:
            db_name: 待核验的数据库名称.

        Returns:
            若存在返回 True，不存在返回 False.
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s",
                    (db_name,),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()

    def inspect_database_structures(
        self,
        db_name: str,
        check_triggers: bool = False,
    ) -> Tuple[Dict[str, TableStructure], Dict[str, TriggerDefinition]]:
        """批量抽取指定数据库内所有表、视图以及触发器的结构元数据.

        Args:
            db_name: 目标数据库名.
            check_triggers: 是否同时抽取触发器元数据.

        Returns:
            包含两项元素的元组: (表/视图结构字典, 触发器结构字典).
        """
        tables: Dict[str, TableStructure] = {}
        triggers: Dict[str, TriggerDefinition] = {}

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. 查询所有表与视图清单 (TABLES)
                cur.execute(
                    """
                    SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COLLATION
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                    """,
                    (db_name,),
                )
                table_rows = cur.fetchall()
                for r in table_rows:
                    t_name = r["TABLE_NAME"]
                    t_type = r["TABLE_TYPE"]
                    engine = r.get("ENGINE")
                    collation = r.get("TABLE_COLLATION")
                    tables[t_name] = TableStructure(
                        name=t_name,
                        table_type=t_type,
                        engine=engine,
                        collation=collation,
                    )

                # 2. 查询所有表的字段定义 (COLUMNS)
                cur.execute(
                    """
                    SELECT
                        TABLE_NAME,
                        COLUMN_NAME,
                        COLUMN_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        EXTRA,
                        COLLATION_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME, ORDINAL_POSITION
                    """,
                    (db_name,),
                )
                col_rows = cur.fetchall()
                for cr in col_rows:
                    t_name = cr["TABLE_NAME"]
                    if t_name in tables:
                        c_name = cr["COLUMN_NAME"]
                        tables[t_name].columns[c_name] = ColumnDefinition(
                            name=c_name,
                            data_type=cr["COLUMN_TYPE"],
                            is_nullable=cr["IS_NULLABLE"],
                            default_value=cr["COLUMN_DEFAULT"],
                            extra=cr["EXTRA"] or "",
                            collation=cr.get("COLLATION_NAME"),
                        )

                # 3. 查询所有索引及主键定义 (STATISTICS)
                cur.execute(
                    """
                    SELECT
                        TABLE_NAME,
                        INDEX_NAME,
                        NON_UNIQUE,
                        SEQ_IN_INDEX,
                        COLUMN_NAME,
                        INDEX_TYPE
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                    """,
                    (db_name,),
                )
                idx_rows = cur.fetchall()
                for ir in idx_rows:
                    t_name = ir["TABLE_NAME"]
                    if t_name not in tables:
                        continue
                    idx_name = ir["INDEX_NAME"]
                    col_name = ir["COLUMN_NAME"]
                    non_unique = int(ir["NON_UNIQUE"])
                    index_type = ir.get("INDEX_TYPE", "BTREE")

                    # 主键独立提取记录
                    if idx_name.upper() == "PRIMARY":
                        tables[t_name].primary_key.append(col_name)

                    # 聚合索引列信息
                    if idx_name not in tables[t_name].indexes:
                        tables[t_name].indexes[idx_name] = IndexDefinition(
                            name=idx_name,
                            non_unique=non_unique,
                            columns=[col_name],
                            index_type=index_type,
                        )
                    else:
                        tables[t_name].indexes[idx_name].columns.append(col_name)

                # 4. 若开启，查询触发器定义 (TRIGGERS)
                if check_triggers:
                    cur.execute(
                        """
                        SELECT
                            TRIGGER_NAME,
                            EVENT_OBJECT_TABLE,
                            ACTION_TIMING,
                            EVENT_MANIPULATION,
                            ACTION_STATEMENT
                        FROM information_schema.TRIGGERS
                        WHERE TRIGGER_SCHEMA = %s
                        """,
                        (db_name,),
                    )
                    trig_rows = cur.fetchall()
                    for tr in trig_rows:
                        trig_name = tr["TRIGGER_NAME"]
                        triggers[trig_name] = TriggerDefinition(
                            name=trig_name,
                            event_table=tr["EVENT_OBJECT_TABLE"],
                            action_timing=tr["ACTION_TIMING"],
                            event_manipulation=tr["EVENT_MANIPULATION"],
                            action_statement=tr.get("ACTION_STATEMENT") or "",
                        )
        finally:
            conn.close()

        return tables, triggers


class SchemaComparator:
    """源与目标库表结构细粒度对比器."""

    def __init__(self, check_triggers: bool = False) -> None:
        """初始化 SchemaComparator 实例.

        Args:
            check_triggers: 是否开启触发器对比.
        """
        self.check_triggers = check_triggers

    def compare_database(
        self,
        db_name: str,
        src_tables: Dict[str, TableStructure],
        src_triggers: Dict[str, TriggerDefinition],
        tgt_tables: Dict[str, TableStructure],
        tgt_triggers: Dict[str, TriggerDefinition],
    ) -> DatabaseDifference:
        """执行指定数据库源端与目标端对象结构对比.

        Args:
            db_name: 数据库名称.
            src_tables: 源端抽取的表与视图字典.
            src_triggers: 源端抽取的触发器字典.
            tgt_tables: 目标端抽取的表与视图字典.
            tgt_triggers: 目标端抽取的触发器字典.

        Returns:
            记录该数据库全部结构差异的 DatabaseDifference 对象.
        """
        diff = DatabaseDifference(db_name)

        # 区分基表与视图
        src_base_tables = {k: v for k, v in src_tables.items() if not v.is_view()}
        tgt_base_tables = {k: v for k, v in tgt_tables.items() if not v.is_view()}

        src_views = {k: v for k, v in src_tables.items() if v.is_view()}
        tgt_views = {k: v for k, v in tgt_tables.items() if v.is_view()}

        # 1. 基表存在性比对
        src_table_names = set(src_base_tables.keys())
        tgt_table_names = set(tgt_base_tables.keys())

        diff.missing_tables = sorted(list(src_table_names - tgt_table_names))
        diff.extra_tables = sorted(list(tgt_table_names - src_table_names))

        # 2. 共有基表的细粒度结构比对
        common_tables = sorted(list(src_table_names & tgt_table_names))
        for t_name in common_tables:
            t_diff = self.compare_single_table(src_base_tables[t_name], tgt_base_tables[t_name])
            if t_diff.has_difference():
                diff.table_diffs[t_name] = t_diff

        # 3. 视图存在性比对
        src_view_names = set(src_views.keys())
        tgt_view_names = set(tgt_views.keys())
        diff.missing_views = sorted(list(src_view_names - tgt_view_names))
        diff.extra_views = sorted(list(tgt_view_names - src_view_names))

        # 4. 触发器比对（若开启）
        if self.check_triggers:
            src_trig_names = set(src_triggers.keys())
            tgt_trig_names = set(tgt_triggers.keys())
            diff.missing_triggers = sorted(list(src_trig_names - tgt_trig_names))
            diff.extra_triggers = sorted(list(tgt_trig_names - src_trig_names))

            common_triggers = sorted(list(src_trig_names & tgt_trig_names))
            for tr_name in common_triggers:
                st = src_triggers[tr_name]
                tt = tgt_triggers[tr_name]
                if (
                    st.event_table.lower() != tt.event_table.lower()
                    or st.action_timing != tt.action_timing
                    or st.event_manipulation != tt.event_manipulation
                ):
                    diff.trigger_mismatches.append(
                        f"触发器 '{tr_name}' 事件定义不一致: "
                        f"源端 ({st.action_timing} {st.event_manipulation} ON `{st.event_table}`) vs "
                        f"目标端 ({tt.action_timing} {tt.event_manipulation} ON `{tt.event_table}`)"
                    )

        return diff

    def compare_single_table(
        self,
        src_t: TableStructure,
        tgt_t: TableStructure,
    ) -> TableDifference:
        """对比单张数据表的存储引擎、字符集、字段及索引定义.

        Args:
            src_t: 源端表结构对象.
            tgt_t: 目标端表结构对象.

        Returns:
            记录该表结构差异的 TableDifference 对象.
        """
        t_diff = TableDifference(src_t.name)

        # 1. 比对存储引擎
        if src_t.engine and tgt_t.engine and src_t.engine != tgt_t.engine:
            t_diff.engine_diff = (src_t.engine, tgt_t.engine)

        # 2. 比对表级字符集/排序规则
        if src_t.collation and tgt_t.collation and src_t.collation != tgt_t.collation:
            t_diff.collation_diff = (src_t.collation, tgt_t.collation)

        # 3. 比对字段存在性
        src_cols = set(src_t.columns.keys())
        tgt_cols = set(tgt_t.columns.keys())

        t_diff.missing_columns = sorted(list(src_cols - tgt_cols))
        t_diff.extra_columns = sorted(list(tgt_cols - src_cols))

        # 4. 比对共有字段属性 (类型、NULL属性、默认值、自增属性)
        common_cols = sorted(list(src_cols & tgt_cols))
        for col_name in common_cols:
            sc = src_t.columns[col_name]
            tc = tgt_t.columns[col_name]

            # 字段类型差异比对
            if sc.data_type != tc.data_type:
                t_diff.column_mismatches.append(
                    f"字段 '{col_name}' 类型不匹配: 源端 `{sc.data_type}` vs 目标端 `{tc.data_type}`"
                )

            # NULL 可空属性差异比对
            if sc.is_nullable != tc.is_nullable:
                t_diff.column_mismatches.append(
                    f"字段 '{col_name}' NULL属性不匹配: 源端 NULL={sc.is_nullable} vs 目标端 NULL={tc.is_nullable}"
                )

            # 默认值差异比对（标准化处理 None 与 'NULL' 字符串）
            sc_def = sc.default_value if sc.default_value is not None else "NULL"
            tc_def = tc.default_value if tc.default_value is not None else "NULL"
            if sc_def != tc_def:
                t_diff.column_mismatches.append(
                    f"字段 '{col_name}' 默认值不匹配: 源端 '{sc_def}' vs 目标端 '{tc_def}'"
                )

            # 自增列属性比对
            if sc.is_auto_increment() != tc.is_auto_increment():
                sc_ai = "AUTO_INCREMENT" if sc.is_auto_increment() else "NON-AUTO"
                tc_ai = "AUTO_INCREMENT" if tc.is_auto_increment() else "NON-AUTO"
                t_diff.column_mismatches.append(
                    f"字段 '{col_name}' 自增属性不匹配: 源端 {sc_ai} vs 目标端 {tc_ai}"
                )

            # 字段级排序规则比对（仅当源端显式配置了不同字符排序时核对）
            if sc.collation and tc.collation and sc.collation != tc.collation:
                t_diff.column_mismatches.append(
                    f"字段 '{col_name}' 字符集排序不匹配: 源端 `{sc.collation}` vs 目标端 `{tc.collation}`"
                )

        # 5. 比对主键 (Primary Key) 定义及列顺序
        if src_t.primary_key != tgt_t.primary_key:
            t_diff.pk_diff = (src_t.primary_key, tgt_t.primary_key)

        # 6. 比对普通索引及唯一索引
        src_idx_names = set(src_t.indexes.keys())
        tgt_idx_names = set(tgt_t.indexes.keys())

        # 排除 PRIMARY 索引（已在 pk_diff 独立比对）
        src_custom_indexes = {k for k in src_idx_names if k.upper() != "PRIMARY"}
        tgt_custom_indexes = {k for k in tgt_idx_names if k.upper() != "PRIMARY"}

        t_diff.missing_indexes = sorted(list(src_custom_indexes - tgt_custom_indexes))
        t_diff.extra_indexes = sorted(list(tgt_custom_indexes - src_custom_indexes))

        common_indexes = sorted(list(src_custom_indexes & tgt_custom_indexes))
        for idx_name in common_indexes:
            si = src_t.indexes[idx_name]
            ti = tgt_t.indexes[idx_name]

            # 索引字段列包含及排列顺序比对
            if si.columns != ti.columns:
                t_diff.index_mismatches.append(
                    f"索引 '{idx_name}' 字段列或顺序不匹配: 源端 {si.columns} vs 目标端 {ti.columns}"
                )

            # 索引唯一性比对 (non_unique)
            if si.non_unique != ti.non_unique:
                s_u = "UNIQUE" if si.non_unique == 0 else "NON-UNIQUE"
                t_u = "UNIQUE" if ti.non_unique == 0 else "NON-UNIQUE"
                t_diff.index_mismatches.append(
                    f"索引 '{idx_name}' 唯一性约束不匹配: 源端 {s_u} vs 目标端 {t_u}"
                )

            # 索引类型比对 (BTREE, FULLTEXT 等)
            if si.index_type != ti.index_type:
                t_diff.index_mismatches.append(
                    f"索引 '{idx_name}' 算法类型不匹配: 源端 {si.index_type} vs 目标端 {ti.index_type}"
                )

        return t_diff


class SchemaCheckReporter:
    """比对差异控制台格式化打印与统计汇总生成器."""

    def __init__(self, color_formatter: ColorFormatter) -> None:
        """初始化 SchemaCheckReporter 实例.

        Args:
            color_formatter: 终端颜色格式化辅助对象.
        """
        self.fmt = color_formatter

    def report_database_diff(self, diff: DatabaseDifference) -> None:
        """向终端打印单个数据库的详细结构差异明细.

        Args:
            diff: 数据库差异封装对象.
        """
        if diff.target_missing_db:
            print(f"  {self.fmt.red('[MISSING DB]')} 目标库中完全不存在该数据库: `{diff.db_name}`")
            return

        if not diff.has_difference():
            print(f"  {self.fmt.green('[PASS]')} 数据库 `{diff.db_name}` 所有表结构、索引与对象完全一致。")
            return

        print(f"  {self.fmt.yellow('[DIFF DETECTED]')} 数据库 `{diff.db_name}` 发现以下结构差异:")

        # 缺失与多余表
        if diff.missing_tables:
            print(f"    - {self.fmt.red('缺失数据表 (Target Lacks):')} {', '.join(diff.missing_tables)}")
        if diff.extra_tables:
            print(f"    - {self.fmt.yellow('多余数据表 (Target Extra):')} {', '.join(diff.extra_tables)}")

        # 缺失与多余视图
        if diff.missing_views:
            print(f"    - {self.fmt.red('缺失视图 (Target Lacks Views):')} {', '.join(diff.missing_views)}")
        if diff.extra_views:
            print(f"    - {self.fmt.yellow('多余视图 (Target Extra Views):')} {', '.join(diff.extra_views)}")

        # 触发器差异
        if diff.missing_triggers:
            print(f"    - {self.fmt.red('缺失触发器 (Target Lacks Triggers):')} {', '.join(diff.missing_triggers)}")
        if diff.extra_triggers:
            print(f"    - {self.fmt.yellow('多余触发器 (Target Extra Triggers):')} {', '.join(diff.extra_triggers)}")
        for tm in diff.trigger_mismatches:
            print(f"    - {self.fmt.red('触发器定义分歧:')} {tm}")

        # 共有表的详细字段与索引差异
        for t_name, t_diff in diff.table_diffs.items():
            print(f"    {self.fmt.bold('● 数据表')} `{t_name}`:")
            if t_diff.engine_diff:
                src_eng, tgt_eng = t_diff.engine_diff
                print(f"      - 存储引擎不一致: 源端 {src_eng} vs 目标端 {tgt_eng}")
            if t_diff.collation_diff:
                src_col, tgt_col = t_diff.collation_diff
                print(f"      - 默认字符集排序不一致: 源端 {src_col} vs 目标端 {tgt_col}")
            if t_diff.missing_columns:
                print(f"      - {self.fmt.red('缺失字段:')} {', '.join(t_diff.missing_columns)}")
            if t_diff.extra_columns:
                print(f"      - {self.fmt.yellow('多余字段:')} {', '.join(t_diff.extra_columns)}")
            for cm in t_diff.column_mismatches:
                print(f"      - {self.fmt.red('字段属性分歧:')} {cm}")
            if t_diff.pk_diff:
                src_pk, tgt_pk = t_diff.pk_diff
                print(f"      - {self.fmt.red('主键定义不一致:')} 源端 {src_pk} vs 目标端 {tgt_pk}")
            if t_diff.missing_indexes:
                print(f"      - {self.fmt.red('缺失索引:')} {', '.join(t_diff.missing_indexes)}")
            if t_diff.extra_indexes:
                print(f"      - {self.fmt.yellow('多余索引:')} {', '.join(t_diff.extra_indexes)}")
            for im in t_diff.index_mismatches:
                print(f"      - {self.fmt.red('索引属性分歧:')} {im}")

    def print_summary(
        self,
        total_mappings: int,
        total_dbs_checked: int,
        total_tables_checked: int,
        diff_dbs_count: int,
        diff_tables_count: int,
    ) -> None:
        """输出最终整体复核统计结果汇总看板.

        Args:
            total_mappings: 检查的主机映射对总数.
            total_dbs_checked: 检查的数据库总数.
            total_tables_checked: 检查的数据表总数.
            diff_dbs_count: 存在差异的数据库数量.
            diff_tables_count: 存在差异的数据表数量.
        """
        print("\n" + "=" * 70)
        print(self.fmt.bold("               表结构复核检查结果统计汇总看板"))
        print("=" * 70)
        print(f"  主机映射任务数 (Host Mappings)  : {total_mappings}")
        print(f"  复核数据库总数 (Databases)      : {total_dbs_checked}")
        print(f"  复核数据表总数 (Tables)         : {total_tables_checked}")
        print("-" * 70)

        if diff_dbs_count == 0 and diff_tables_count == 0:
            status_text = self.fmt.green("【完全一致 PASS】源端与目标端表结构、字段、索引全量匹配！")
            print(f"  复核结论状态                    : {status_text}")
        else:
            status_text = self.fmt.red(
                f"【存在差异 FAILED】发现 {diff_dbs_count} 个数据库、{diff_tables_count} 张数据表存在结构分歧！"
            )
            print(f"  复核结论状态                    : {status_text}")
        print("=" * 70 + "\n")


class SchemaCheckRunner:
    """表结构复核编排执行主类."""

    def __init__(self, cli_args: argparse.Namespace) -> None:
        """初始化 SchemaCheckRunner 实例.

        Args:
            cli_args: 命令行解析得到的参数对象.
        """
        self.args = cli_args
        self.config_data = self._load_config(cli_args.config)
        self.color_formatter = ColorFormatter(enable_color=not cli_args.no_color)
        self.reporter = SchemaCheckReporter(self.color_formatter)
        self.comparator = SchemaComparator(check_triggers=cli_args.check_triggers)

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """读取并解析 YAML 配置文件.

        Args:
            config_path: 配置文件路径.

        Returns:
            配置字典数据.

        Raises:
            SystemExit: 当配置文件不存在或解析异常时退出.
        """
        if not os.path.exists(config_path):
            logger.error("配置文件不存在: %s", config_path)
            sys.exit(EXIT_CODE_ERROR)

        try:
            with open(config_path, "r", encoding="utf8") as f:
                return yaml.safe_load(f) or {}
        except Exception as err:
            logger.error("解析配置文件失败 [%s]: %s", config_path, err)
            sys.exit(EXIT_CODE_ERROR)

    def _resolve_tasks(self) -> List[Dict[str, Any]]:
        """根据配置文件及命令行参数生成待复核的主机任务列表.

        Returns:
            整理完成的主机连接参数任务列表.
        """
        common = self.config_data.get("common", {})
        raw_migrations = self.config_data.get("migrations", [])
        tasks: List[Dict[str, Any]] = []

        for item in raw_migrations:
            src_host = item.get("source")
            tgt_host = item.get("target")

            if not src_host or not tgt_host:
                continue

            if self.args.source and src_host != self.args.source:
                continue
            if self.args.target and tgt_host != self.args.target:
                continue

            src_params = {
                "host": src_host,
                "port": item.get("port", common.get("port", 3306)),
                "user": item.get("user", common.get("user", "root")),
                "password": item.get("password", common.get("password", "")),
            }

            tgt_params = {
                "host": tgt_host,
                "port": item.get("target_port", item.get("port", common.get("port", 3306))),
                "user": item.get("target_user", item.get("user", common.get("user", "root"))),
                "password": item.get("target_password", item.get("password", common.get("password", ""))),
            }

            tasks.append({"source": src_params, "target": tgt_params})

        return tasks

    def run(self) -> int:
        """执行全流程复核操作并返回状态码.

        Returns:
            EXIT_CODE_SUCCESS (0) 或 EXIT_CODE_DIFF (1) 或 EXIT_CODE_ERROR (2).
        """
        tasks = self._resolve_tasks()
        if not tasks:
            logger.warning("未匹配到任何需要复核的主机映射任务。请检查 config.yaml 或命令行过滤参数。")
            return EXIT_CODE_SUCCESS

        filter_cfg = self.config_data.get("filter", {})
        ignore_dbs: Set[str] = set(filter_cfg.get("ignore_databases") or [])
        include_dbs: Set[str] = set(filter_cfg.get("include_databases") or [])

        total_mappings = len(tasks)
        total_dbs_checked = 0
        total_tables_checked = 0
        diff_dbs_count = 0
        diff_tables_count = 0

        print("\n" + self.color_formatter.bold(">>> 开始执行数据库表结构一致性复核流程..."))

        for idx, task in enumerate(tasks, 1):
            src = task["source"]
            tgt = task["target"]

            header = (
                f"[{idx}/{total_mappings}] 比对映射任务: "
                f"源端 ({src['host']}:{src['port']}) -> 目标端 ({tgt['host']}:{tgt['port']})"
            )
            print("\n" + self.color_formatter.cyan("-" * 70))
            print(self.color_formatter.bold(header))
            print(self.color_formatter.cyan("-" * 70))

            src_inspector = SchemaInspector(src)
            tgt_inspector = SchemaInspector(tgt)

            # 1. 查询源端待检查数据库清单
            try:
                candidate_dbs = src_inspector.list_databases(
                    ignore_dbs=ignore_dbs,
                    include_dbs=include_dbs,
                    db_filter=self.args.database,
                )
            except Exception as err:
                logger.error("连接源端数据库失败 (%s:%s): %s", src["host"], src["port"], err)
                return EXIT_CODE_ERROR

            if not candidate_dbs:
                print("  未发现需要复核的业务数据库（已被黑白名单过滤或为空）。")
                continue

            for db_name in candidate_dbs:
                total_dbs_checked += 1
                print(f"\n* 正在复核数据库: {self.color_formatter.bold(db_name)}")

                # 检查目标库是否存在该数据库
                try:
                    tgt_db_exists = tgt_inspector.check_database_exists(db_name)
                except Exception as err:
                    logger.error("连接目标端数据库失败 (%s:%s): %s", tgt["host"], tgt["port"], err)
                    return EXIT_CODE_ERROR

                if not tgt_db_exists:
                    diff_db = DatabaseDifference(db_name)
                    diff_db.target_missing_db = True
                    self.reporter.report_database_diff(diff_db)
                    diff_dbs_count += 1
                    continue

                # 提取源端与目标端对象结构
                try:
                    src_tables, src_triggers = src_inspector.inspect_database_structures(
                        db_name, self.args.check_triggers
                    )
                    tgt_tables, tgt_triggers = tgt_inspector.inspect_database_structures(
                        db_name, self.args.check_triggers
                    )
                except Exception as err:
                    logger.error("提取数据库 [%s] 表结构元数据失败: %s", db_name, err)
                    return EXIT_CODE_ERROR

                # 累计表总数统计
                total_tables_checked += len(src_tables)

                # 比对结构差异
                db_diff = self.comparator.compare_database(
                    db_name,
                    src_tables,
                    src_triggers,
                    tgt_tables,
                    tgt_triggers,
                )

                if db_diff.has_difference():
                    diff_dbs_count += 1
                    diff_tables_count += len(db_diff.missing_tables) + len(db_diff.table_diffs)
                    self.reporter.report_database_diff(db_diff)
                else:
                    self.reporter.report_database_diff(db_diff)

        # 打印总体统计汇总
        self.reporter.print_summary(
            total_mappings=total_mappings,
            total_dbs_checked=total_dbs_checked,
            total_tables_checked=total_tables_checked,
            diff_dbs_count=diff_dbs_count,
            diff_tables_count=diff_tables_count,
        )

        if diff_dbs_count > 0 or diff_tables_count > 0:
            return EXIT_CODE_DIFF
        return EXIT_CODE_SUCCESS


def parse_cli_args() -> argparse.Namespace:
    """构建与解析命令行参数.

    Returns:
        解析后的 Namespace 命令行参数对象.
    """
    default_cfg = os.path.join(os.path.dirname(__file__), "config.yaml")
    parser = argparse.ArgumentParser(
        description="MySQL 8.0/5.7 数据库表结构复核比对工具 (兼容 Python 3.6+)"
    )

    parser.add_argument(
        "-c",
        "--config",
        default=default_cfg,
        help=f"配置文件 YAML 路径 (默认: {default_cfg})",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="精确筛选仅复核指定源 IP (source host) 的迁移映射",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="精确筛选仅复核指定目标 IP (target host) 的迁移映射",
    )
    parser.add_argument(
        "-d",
        "--database",
        default=None,
        help="精确筛选仅复核指定名称的单个数据库",
    )
    parser.add_argument(
        "--check-triggers",
        action="store_true",
        help="额外开启触发器 (Triggers) 的一致性复核",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="关闭终端 ANSI 彩色高亮显示",
    )

    return parser.parse_args()


def main() -> None:
    """程序入口主函数."""
    args = parse_cli_args()
    runner = SchemaCheckRunner(args)
    exit_code = runner.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
