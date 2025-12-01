#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Упрощённый MCP-сервер «HR Simple MCP».

Цель — отдавать LLM сразу насыщенные ответы на русском языке, чтобы сократить
количество инструментов: карточка сотрудника, обзор команды, заявления.

Команды:
    python simple_hr_mcp.py
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections import deque
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

from fastmcp import FastMCP
import psycopg2
from psycopg2.extras import RealDictCursor

# -----------------------------------------------------------------------------
# Константы
# -----------------------------------------------------------------------------

DSN = "postgresql://postgres:df8q74zm6@46.149.76.166/ml_vlm_mcp"
DEFAULT_REQUESTER_USER_ID = 3
ROOT = Path(__file__).parent.resolve()
FAQ_PATH = ROOT / "questions.csv"

mcp = FastMCP(
    name="HR Simple MCP",
    instructions=(
        "Ты подключён к HR данным о копании и сотрудниках. Перед стартом опиши цель через session.log_intent. "
        "Доступные инструменты:\n"
        "• profile_summary — карточка сотрудника (ФИО, роль, команда, зарплата, будущие отпуска, оценочный баланс).\n"
        "• my_team_overview — обзор МОЕЙ команды: участники, роли, дни рождения, видимость зарплат.\n"
        "• team_search — поиск другой команды по названию или id, без зарплат.\n"
        "• employee_search — поиск сотрудников по имени/фамилии, телефону или id.\n"
        "• task_tracker_my_tasks — задачи из трекера *WB Track* с дедлайнами и ссылкой.\n"
        "• statement_create/statement_list/statement_active/statement_cancel — работа с заявлениями.\n"
        "• calendar_my_events — события моего календаря на сегодня или неделю.\n"
        "• ndfl_order — заказать справку 2-НДФЛ за указанный год.\n"
        "• faq_by_category — ответы на вопросы из справочника по категории.\n"
        "• leave_history — история подтверждённых отпусков с датами и длительностью.\n"
        "• leave_balance_forecast — прогноз отпускных дней на дату с учётом планов.\n"
        "• team_salary_analytics — агрегаты по зарплатам команды (среднее/медиана/диапазон).\n"
        "• session_log_intent и session_intent_history — логирование намерений и просмотр последних записей.\n"
        "Все ответы возвращаются на русском языке, можно сразу использовать в ответе пользователю."
    ),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
LOGGER = logging.getLogger("hr_simple_mcp")
_INTENT_LOG: deque[Dict[str, Any]] = deque(maxlen=20)


# -----------------------------------------------------------------------------
# Работа с БД
# -----------------------------------------------------------------------------

class Pg:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        LOGGER.info("Подключение к PostgreSQL установлено.")

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def one(self, q: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
        try:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(q, params)
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as exc:
            LOGGER.exception("SQL(one) failed: %s | params=%s", q, params)
            raise

    def all(self, q: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        try:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(q, params)
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            LOGGER.exception("SQL(all) failed: %s | params=%s", q, params)
            raise

    def exec(self, q: str, params: Tuple = (), returning: bool = False) -> Optional[Dict[str, Any]]:
        try:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(q, params)
                if returning:
                    row = cur.fetchone()
                    self._conn.commit()
                    return dict(row) if row else None
                self._conn.commit()
                return None
        except Exception as exc:
            LOGGER.exception("SQL(exec) failed: %s | params=%s", q, params)
            self._conn.rollback()
            raise


DB = Pg(DSN)
try:
    DB.connect()
    version = DB.one("SELECT version();")
    LOGGER.info("PostgreSQL: %s", version["version"] if version else "unknown")
except Exception:
    LOGGER.exception("Не удалось подключиться к БД")
    sys.exit(1)


# -----------------------------------------------------------------------------
# Вспомогательные функции и ACL
# -----------------------------------------------------------------------------

def with_usage(payload: Dict[str, Any], usage: str) -> Dict[str, Any]:
    """
    Добавляет подсказку как работать с данными в ответе MCP.
    """
    payload["how_to_use"] = usage
    return payload


def ensure_statements_table() -> None:
    q = """
    CREATE TABLE IF NOT EXISTS employee_statements (
        id              BIGSERIAL PRIMARY KEY,
        employee_id     BIGINT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        category        VARCHAR(32) NOT NULL,
        body            TEXT NOT NULL,
        desired_start   DATE,
        desired_end     DATE,
        status          VARCHAR(32) NOT NULL DEFAULT 'новая',
        vacation_kind   VARCHAR(32),
        deleted_at      TIMESTAMPTZ,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    DB.exec(q)
    DB.exec("ALTER TABLE employee_statements ADD COLUMN IF NOT EXISTS vacation_kind VARCHAR(32);")
    DB.exec("ALTER TABLE employee_statements ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;")


ensure_statements_table()


def ensure_task_tracker_table() -> None:
    q = """
    CREATE TABLE IF NOT EXISTS task_tracker_tasks (
        id              BIGSERIAL PRIMARY KEY,
        employee_id     BIGINT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        task_title      TEXT NOT NULL,
        summary         TEXT NOT NULL,
        description     TEXT,
        created_at      DATE NOT NULL,
        deadline        DATE NOT NULL,
        sprint_number   INTEGER NOT NULL,
        details_url     TEXT NOT NULL,
        status          VARCHAR(32) NOT NULL DEFAULT 'в работе'
    );
    """
    DB.exec(q)


ensure_task_tracker_table()


def ensure_calendar_events_table() -> None:
    q = """
    CREATE TABLE IF NOT EXISTS calendar_events (
        id          BIGSERIAL PRIMARY KEY,
        employee_id BIGINT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        title       TEXT NOT NULL,
        starts_at   TIMESTAMPTZ NOT NULL,
        ends_at     TIMESTAMPTZ NOT NULL,
        visibility  VARCHAR(16) NOT NULL DEFAULT 'private'
    );
    """
    DB.exec(q)


ensure_calendar_events_table()


def ensure_ndfl_links_table() -> None:
    q = """
    CREATE TABLE IF NOT EXISTS ndfl_links (
        year INTEGER PRIMARY KEY,
        download_url TEXT NOT NULL
    );
    """
    DB.exec(q)


ensure_ndfl_links_table()


# -----------------------------------------------------------------------------
# Resources (схемы БД) и шаблоны
# -----------------------------------------------------------------------------


@mcp.resource(
    "resource://db/schema",
    name="db_schema",
    title="HR DB schema",
    description="Описание таблиц, колонок и назначений БД HR MCP.",
)
def db_schema_resource() -> str:
    return """
Таблицы HR MCP (PostgreSQL):

employees
- id (PK, bigint) — внутренний идентификатор
- external_id (bigint, nullable) — внешний id
- telegram_user_id/telegram_username (nullable)
- full_name (text), email (text), phone (text), position (text)
- hired_at, fired_at (date)
- country, city (text, optional)
- team_id (fk → teams.id), manager_id (fk → employees.id)
- is_active (bool)
- gender (varchar, optional), birth_date (date, optional)

teams
- id (PK)
- name (text, уникальный)
- external_id (varchar, nullable)
- lead_user_id (fk → employees.id, nullable)

leaves
- id (PK)
- employee_id (fk → employees.id)
- start_date, end_date (date)
- type (varchar, например annual/sick)
- status (pending|approved|rejected|cancelled)
- requested_at (timestamptz), approver_id (fk → employees), decision_at
- comment, approver_comment (text)

employee_statements
- id (PK)
- employee_id (fk → employees.id)
- category (varchar: отпуск|увольнение|другое)
- body (text) — текст заявления
- desired_start, desired_end (date, nullable)
- vacation_kind (varchar, nullable: очередной|за свой счёт)
- status (varchar, default 'новая')
- deleted_at (timestamptz, nullable) — soft delete/отмена
- created_at (timestamptz, default now)

salaries
- id (PK)
- employee_id (fk → employees.id)
- currency (varchar), amount (numeric) — ежемесячная ставка
- effective_from, effective_to (date nullable)

access_rules
- id (PK)
- user_id (fk → employees.id)
- action (varchar), scope (SELF|USER|TEAM|TEAM_ONLY|SUBORDINATES|ALL)
- target_user_id (nullable), team_id (nullable)
- allow (bool), created_at

response_templates
- id (PK)
- key (text), language (varchar)
- version (int), content (text), is_active (bool), updated_at

task_tracker_tasks
- id (PK)
- employee_id (fk → employees.id)
- task_title, summary, description (text)
- created_at (date), deadline (date), sprint_number (int)
- details_url (text), status (varchar)

calendar_events
- id (PK)
- employee_id (fk → employees.id)
- title (text)
- starts_at, ends_at (timestamptz)
- visibility (varchar, default 'private')

ndfl_links
- year (PK int)
- download_url (text)
    """.strip()


@mcp.resource(
    "resource://templates/profile_widget",
    name="profile_widget_template",
    title="HTML template for profile widget",
    description="Готовый HTML-шаблон виджета карточки сотрудника для ui://widgets/audit-dashboard. Подставь данные и верни HTML.",
    mime_type="text/html",
)
def profile_widget_template() -> str:
    return """
<div data-widget="ui://widgets/audit-dashboard" style="font-family: Arial, sans-serif; border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#fff;">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
    <div>
      <div style="font-size:18px; font-weight:700; color:#111827;">{full_name}</div>
      <div style="font-size:14px; color:#6b7280;">{position}</div>
    </div>
    <div style="text-align:right; font-size:14px; color:#6b7280;">
      <div>Команда: {team_name}</div>
      <div>Менеджер: {manager_name}</div>
    </div>
  </div>
  <div style="display:flex; gap:12px; margin:12px 0; flex-wrap:wrap;">
    <div style="padding:8px 10px; background:#f9fafb; border-radius:8px; min-width:140px;">
      <div style="font-size:12px; color:#6b7280;">Зарплата</div>
      <div style="font-size:16px; font-weight:600; color:#111827;">{salary}</div>
    </div>
    <div style="padding:8px 10px; background:#f9fafb; border-radius:8px; min-width:140px;">
      <div style="font-size:12px; color:#6b7280;">Баланс отпускных</div>
      <div style="font-size:16px; font-weight:600; color:#111827;">{balance_days} дн.</div>
    </div>
  </div>
  <div style="margin-bottom:10px;">
    <div style="font-size:12px; color:#6b7280; margin-bottom:4px;">Контакты</div>
    <div style="font-size:14px; color:#111827;">Email: {email}</div>
    <div style="font-size:14px; color:#111827;">Телефон: {phone}</div>
  </div>
  <div style="margin-bottom:10px;">
    <div style="font-size:12px; color:#6b7280; margin-bottom:4px;">Ближайшие отпуска</div>
    <ul style="margin:0; padding-left:16px; color:#111827; font-size:14px;">
      {vacations_block}
    </ul>
  </div>
  <div style="font-size:13px; color:#4b5563;">{summary}</div>
</div>
<!-- Подставляй значения или оставляй \"—\" если нет данных. vacations_block — это <li>start — end (status)</li> или текст \"нет\". -->
    """.strip()


@mcp.resource(
    "resource://templates/profile_widget_prompt",
    name="profile_widget_prompt",
    title="Profile widget instructions",
    description="Инструкция для заполнения HTML виджета профиля (ui://widgets/audit-dashboard) по данным profile_summary.",
)
def profile_widget_prompt() -> str:
    return (
        "Собери HTML виджет карточки сотрудника для ui://widgets/audit-dashboard. "
        "Корневой элемент: <div data-widget=\"ui://widgets/audit-dashboard\">. "
        "Внутри: шапка (ФИО, позиция), строка команды и менеджера, блок зарплаты (если есть), "
        "контакты (email/телефон, если есть), список ближайших отпусков (дата от–до, статус), "
        "баланс отпускных дней и короткое резюме. Используй простые классы или inline-стили, без внешних импортов. "
        "Не добавляй скрипты; выводи чистый HTML. "
        "Можно взять HTML-шаблон из resource://templates/profile_widget и заменить плейсхолдеры данными профиля."
    )


def _load_faq() -> List[Dict[str, Any]]:
    """
    Быстрая загрузка FAQ из questions.csv. Если файла нет — возвращает пустой список.
    """
    if not FAQ_PATH.exists():
        return []
    import csv

    try:
        with FAQ_PATH.open("r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        LOGGER.exception("Не удалось прочитать FAQ CSV")
        return []

    normalized: List[Dict[str, Any]] = []
    for r in rows:
        try:
            normalized.append(
                {
                    "question_id": int(r.get("question_id") or 0),
                    "category_id": int(r.get("category_id") or 0),
                    "category_title": r.get("category_title") or "",
                    "question_title": r.get("question_title") or "",
                    "answer_text": r.get("answer_text") or "",
                    "answer_text_md": r.get("answer_text_md") or "",
                }
            )
        except Exception:
            LOGGER.exception("Ошибка парсинга строки FAQ: %s", r)
            continue
    return normalized


def resolve_requester_user_id(candidate: Optional[int]) -> int:
    if candidate is not None:
        return candidate
    if DEFAULT_REQUESTER_USER_ID is not None:
        return DEFAULT_REQUESTER_USER_ID
    raise ValueError("Не указан requester_user_id и не задан DEFAULT_REQUESTER_USER_ID.")


def user_team_id(user_id: int) -> Optional[int]:
    row = DB.one("SELECT team_id FROM employees WHERE id = %s;", (user_id,))
    return row["team_id"] if row else None


def is_manager_of(manager_id: int, employee_id: int) -> bool:
    row = DB.one("SELECT 1 FROM employees WHERE id = %s AND manager_id = %s LIMIT 1;", (employee_id, manager_id))
    return row is not None


def has_access(user_id: int, action: str, target_user_id: Optional[int] = None, team_id: Optional[int] = None) -> bool:
    """
    Мини-ACL: используем таблицу access_rules, но добавляем шорткаты для руководителей.
    """
    if action == "READ_SELF" and target_user_id == user_id:
        return True

    if action in ("READ_TEAM", "VIEW_SALARY_SUBORDINATES"):
        # прямой руководитель или лидер команды
        if target_user_id and is_manager_of(user_id, target_user_id):
            return True
        if team_id:
            leader = DB.one("SELECT 1 FROM teams WHERE id = %s AND lead_user_id = %s LIMIT 1;", (team_id, user_id))
            if leader:
                return True

    params = (user_id, action, user_id, target_user_id, team_id)
    q = """
    SELECT 1
    FROM access_rules
    WHERE user_id = %s
      AND action = %s
      AND allow = TRUE
      AND (
            scope = 'ALL'
        OR (scope = 'SELF' AND user_id = %s)
        OR (scope = 'USER' AND target_user_id = %s)
        OR (scope IN ('TEAM','TEAM_ONLY') AND team_id = %s)
        OR (scope = 'SUBORDINATES')
      )
    LIMIT 1;
    """
    row = DB.one(q, params)
    if row:
        return True
    return action == "READ_COLLEAGUE"


def get_employee_basic(employee_id: int) -> Optional[Dict[str, Any]]:
    q = """
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.phone,
        e.position,
        e.hired_at,
        e.team_id,
        t.name AS team_name,
        e.manager_id,
        m.full_name AS manager_name
    FROM employees e
    LEFT JOIN teams t ON t.id = e.team_id
    LEFT JOIN employees m ON m.id = e.manager_id
    WHERE e.id = %s;
    """
    return DB.one(q, (employee_id,))


def get_current_salary(employee_id: int, as_of: Optional[dt.date] = None) -> Optional[Dict[str, Any]]:
    if as_of is None:
        as_of = dt.date.today()
    q = """
    SELECT currency, amount, effective_from, effective_to
    FROM salaries
    WHERE employee_id = %s
      AND effective_from <= %s
      AND (effective_to IS NULL OR effective_to >= %s)
    ORDER BY effective_from DESC
    LIMIT 1;
    """
    return DB.one(q, (employee_id, as_of, as_of))


def format_salary(salary: Optional[Dict[str, Any]]) -> str:
    if not salary:
        return "недоступна"
    amount = salary["amount"]
    currency = salary["currency"]
    return f"{amount:,.0f} {currency}".replace(",", " ")


def compute_accrued_leave_days(employee: Dict[str, Any], used_days: float) -> float:
    hired_at = employee.get("hired_at")
    if not isinstance(hired_at, dt.date):
        return max(0.0, 28.0 - used_days)
    today = dt.date.today()
    months = (today.year - hired_at.year) * 12 + (today.month - hired_at.month)
    accrued = max(0.0, months * 2.33 - used_days)
    return round(accrued, 1)


def upcoming_leaves(employee_id: int) -> List[Dict[str, Any]]:
    today = dt.date.today()
    q = """
    SELECT id, start_date, end_date, type, status
    FROM leaves
    WHERE employee_id = %s
      AND end_date >= %s
      AND status IN ('pending','approved')
    ORDER BY start_date;
    """
    rows = DB.all(q, (employee_id, today))
    for r in rows:
        r["start_date"] = r["start_date"].isoformat()
        r["end_date"] = r["end_date"].isoformat()
    return rows


def used_leave_days(employee_id: int) -> float:
    q = """
    SELECT COALESCE(SUM(end_date - start_date + 1), 0) AS days
    FROM leaves
    WHERE employee_id = %s
      AND status = 'approved';
    """
    row = DB.one(q, (employee_id,))
    return float(row["days"]) if row else 0.0


def months_between(start: dt.date, end: dt.date) -> int:
    """
    Целое количество месячных шагов между датами. Корректируем, если день конца меньше дня начала.
    """
    diff = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day < start.day:
        diff -= 1
    return max(0, diff)


def compute_accrued_leave_days_as_of(employee: Dict[str, Any], used_days: float, as_of: dt.date) -> float:
    hired_at = employee.get("hired_at")
    if not isinstance(hired_at, dt.date):
        # Без даты найма считаем от текущей даты как базу 28 дней + месяцы до as_of
        base_today_balance = compute_accrued_leave_days(employee, used_days)
        extra_months = months_between(dt.date.today(), as_of)
        return round(base_today_balance + extra_months * 2.33, 1)
    months = months_between(hired_at, as_of)
    accrued = max(0.0, months * 2.33 - used_days)
    return round(accrued, 1)


def load_team_members(team_id: int) -> List[Dict[str, Any]]:
    q = """
    SELECT
        e.id,
        e.full_name,
        e.position,
        e.email,
        e.birth_date,
        e.manager_id
    FROM employees e
    WHERE e.team_id = %s
    ORDER BY e.full_name;
    """
    rows = DB.all(q, (team_id,))
    today = dt.date.today()
    for r in rows:
        b = r.get("birth_date")
        if isinstance(b, dt.date):
            next_bday = next_birthday(b, today)
            r["birth_date"] = b.isoformat()
            r["days_to_birthday"] = (next_bday - today).days
            r["next_birthday_date"] = next_bday.isoformat()
        else:
            r["birth_date"] = None
            r["days_to_birthday"] = None
            r["next_birthday_date"] = None
    return rows


def _is_leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def next_birthday(birth_date: dt.date, today: dt.date) -> dt.date:
    year = today.year
    while True:
        day = birth_date.day
        if birth_date.month == 2 and birth_date.day == 29 and not _is_leap(year):
            day = 28
        candidate = dt.date(year, birth_date.month, day)
        if candidate >= today:
            return candidate
        year += 1


def current_sprint_number() -> int:
    return dt.date.today().isocalendar()[1]


def fetch_user_tasks(employee_id: int, sprint_number: Optional[int], limit: int) -> List[Dict[str, Any]]:
    filters = ["employee_id = %s"]
    params: List[Any] = [employee_id]
    if sprint_number is not None:
        filters.append("sprint_number = %s")
        params.append(sprint_number)
    params.append(limit)
    where_clause = " AND ".join(filters)
    q = f"""
    SELECT id, task_title, summary, created_at, deadline, sprint_number, details_url, status
    FROM task_tracker_tasks
    WHERE {where_clause}
    ORDER BY deadline, id
    LIMIT %s;
    """
    rows = DB.all(q, tuple(params))
    for row in rows:
        created_at = row.get("created_at")
        deadline = row.get("deadline")
        if isinstance(created_at, dt.date):
            row["created_at"] = created_at.isoformat()
        if isinstance(deadline, dt.date):
            row["deadline"] = deadline.isoformat()
    return rows


def normalize_phone(phone: str) -> str:
    return "".join(ch for ch in phone if ch.isdigit())


# -----------------------------------------------------------------------------
# Инструменты
# -----------------------------------------------------------------------------


@mcp.tool(
    name="leave_history",
    description="История подтверждённых отпусков сотрудника: даты начала/конца и длительность.",
)
def leave_history(
    requester_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    q = """
    SELECT id, start_date, end_date, type, status
    FROM leaves
    WHERE employee_id = %s AND status = 'approved'
    ORDER BY start_date DESC;
    """
    rows = DB.all(q, (user_id,))
    history: List[Dict[str, Any]] = []
    for r in rows:
        sd = r.get("start_date")
        ed = r.get("end_date")
        if isinstance(sd, dt.date):
            r["start_date"] = sd.isoformat()
        if isinstance(ed, dt.date):
            r["end_date"] = ed.isoformat()
        days = 0
        if isinstance(sd, dt.date) and isinstance(ed, dt.date):
            days = (ed - sd).days + 1
        r["days"] = days
        history.append(r)
    return with_usage(
        {
            "leaves": history,
            "count": len(history),
            "answer": f"Подтверждённых отпусков: {len(history)}.",
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Перечисли прошлые подтверждённые отпуска с датами и длительностью. Ответь на исходный вопрос и предложи уточнить будущие планы",
    )


@mcp.tool(
    name="leave_balance_forecast",
    description="Прогноз количества отпускных дней на будущую дату с учётом уже взятых и планируемых отпусков.",
)
def leave_balance_forecast(
    target_date: str,
    requester_user_id: Optional[int] = None,
    planned_days: Optional[int] = None,
    planned_start: Optional[str] = None,
    planned_end: Optional[str] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    try:
        as_of = dt.date.fromisoformat(target_date)
    except Exception:
        return {"error": "Некорректная target_date, ожидается YYYY-MM-DD."}

    employee = get_employee_basic(user_id)
    if not employee:
        return {"error": "Сотрудник не найден."}

    today = dt.date.today()
    # Текущий баланс на сегодня
    used_all = used_leave_days(user_id)
    current_balance_today = compute_accrued_leave_days(employee, used_all)

    # Будущие утверждённые отпуска между сегодня и целевой датой
    rows_future = DB.all(
        """
        SELECT start_date, end_date
        FROM leaves
        WHERE employee_id = %s
          AND status = 'approved'
          AND start_date >= %s
          AND end_date <= %s;
        """,
        (user_id, today, as_of),
    )
    approved_future_days = 0.0
    for r in rows_future:
        sd, ed = r.get("start_date"), r.get("end_date")
        if isinstance(sd, dt.date) and isinstance(ed, dt.date):
            approved_future_days += (ed - sd).days + 1

    # Дополнительное начисление с сегодняшнего дня до целевой даты
    extra_months = months_between(today, as_of)
    accrued_future = extra_months * 2.33
    base_balance_at_target = current_balance_today + accrued_future - approved_future_days

    planned = 0
    if planned_start and planned_end:
        try:
            ps, pe = dt.date.fromisoformat(planned_start), dt.date.fromisoformat(planned_end)
            planned = max(0, (pe - ps).days + 1)
        except Exception:
            return {"error": "planned_start/planned_end должны быть датами YYYY-MM-DD."}
    elif planned_days:
        planned = max(0, int(planned_days))

    balance_with_planned = base_balance_at_target - planned

    answer = (
        f"Текущий баланс {current_balance_today} дн. "
        f"До {as_of.isoformat()} начислится ещё {accrued_future:.2f} дн. "
        f"Базовый остаток на дату: {base_balance_at_target:.2f} дн. "
    )
    if approved_future_days > 0:
        answer += f"Уже утверждённые будущие отпуска до {as_of.isoformat()}: {approved_future_days} дн. "
    if planned:
        answer += f"С учётом планируемых {planned} дн остаток будет {balance_with_planned} дн."
    else:
        answer += "Дополнительных планируемых отпусков не учтено."

    return with_usage(
        {
            "as_of": as_of.isoformat(),
            "current_balance_today": current_balance_today,
            "accrued_future_days": accrued_future,
            "approved_future_days": approved_future_days,
            "base_balance_at_target": base_balance_at_target,
            "planned_days": planned,
            "balance_with_planned": balance_with_planned,
            "answer": answer,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Объясни пользователю из чего сложился баланс (начислено, использовано, будущие согласованные, планируемые), предложи уточнить даты/длительность.",
    )

# FAQ по категории: возвращает все вопросы и ответы выбранной категории.
@mcp.tool(
    name="faq_by_category",
    description=(
        "Вернуть все вопросы и ответы из справочника questions.csv по категории (title или id). "
        "Примеры категорий: Бенефиты и корпоративные программы скидок для сотрудников, Больничный, Вопросы по выплатам, "
        "Декретный отпуск, Доступы и VPN, Заполнение личного профиля в WB Team, Зарплата, Здоровье и ДМС, ИТ-ипотека, "
        "Изменение паспортных данных и номера телефона, Карта WB Банка, Кибербезопасность, Кодексы этики и обращения на горячую линию, "
        "Командировки и помощь в организации командировок, Конфликт интересов, Корпоративная техника для сотрудника, "
        "Обучение, Отпуск, Отсрочка от армии, Офисы, Переход в другую команду, Порекомендовать друга, Работа с почтой, "
        "Справка с места работы, Техническая поддержка, Увольнение."
    ),
)
def faq_by_category(category: str, limit: int = 50) -> Dict[str, Any]:
    records = _load_faq()
    if not records:
        return {"error": "FAQ недоступен (нет файла questions.csv)."}
    term = (category or "").strip().lower()
    if not term:
        return {"error": "Укажите категорию (id или название)."}
    matches: List[Dict[str, Any]] = []
    for r in records:
        cat_title = (r.get("category_title") or "").lower()
        if term == str(r.get("category_id")) or term in cat_title:
            matches.append(
                {
                    "question_id": r["question_id"],
                    "category_id": r["category_id"],
                    "category_title": r["category_title"],
                    "question_title": r["question_title"],
                    "answer_text": r.get("answer_text") or "",
                    "answer_text_md": r.get("answer_text_md") or "",
                }
            )
            if len(matches) >= max(1, min(limit, 200)):
                break
    if not matches:
        return {"error": "Вопросы не найдены по указанной категории.", "category": category}
    return with_usage(
        {
            "category": category,
            "items": matches,
            "found": len(matches),
            "answer": f"По категории найдено вопросов: {len(matches)}.",
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Отвечай на заданный вопрос и предложи подсказать другие вопросы по этой теме; используй список items для примеров.",
    )


# Карточка пользователя: "Подробно по пунктам собери карточку сотрудника и от образи пользователю"
@mcp.tool(
    name="profile_summary",
    description=(
        "Карточка сотрудника: возвращает ФИО, позицию, команду, зарплату, "
        "запланированные отпуска и оценочный баланс дней. Параметры requester_user_id/target_user_id позволяют "
        "управлять тем, чьи данные запрашиваются."
    ),
)
def profile_summary(
    requester_user_id: Optional[int] = None,
    target_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    target_id = target_user_id or user_id

    if not has_access(user_id, "READ_COLLEAGUE", target_user_id=target_id):
        return {"error": "Нет прав читать карточку сотрудника."}

    employee = get_employee_basic(target_id)
    if not employee:
        return {"error": "Сотрудник не найден."}

    salary = None
    if target_id == user_id or has_access(user_id, "VIEW_SALARY_SUBORDINATES", target_user_id=target_id):
        salary = get_current_salary(target_id)

    leaves = upcoming_leaves(target_id)
    used_days = used_leave_days(target_id)
    balance = compute_accrued_leave_days(employee, used_days)

    answer = (
        f"{employee['full_name']} ({employee.get('position') or 'без должности'}), команда: {employee.get('team_name') or 'не указано'}. "
        f"Зарплата: {format_salary(salary)}. "
        f"Запланировано отпусков: {len(leaves)}. "
        f"Оценочный баланс отпускных дней: {balance}."
    )

    return with_usage(
        {
            "employee": employee,
            "salary": salary,
            "planned_leaves": leaves,
            "estimated_leave_balance_days": balance,
            "answer": answer,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Подробно по пунктам собери карточку сотрудника и отобрази пользователю; если чего-то нет, честно скажи об отсутствии данных.",
    )


# Обзор моей команды: выводим ровно мою команду без подмены
@mcp.tool(
    name="my_team_overview",
    description=(
        "Обзор именно команды запрашивающего: список участников с ролями, датами рождения и зарплатами, "
        "а также краткий вывод. Если team_id не указан, используется команда запрашивающего."
    ),
)
def my_team_overview(
    requester_user_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    team = team_id or user_team_id(user_id)
    if not team:
        return {"error": "У сотрудника не заполнена команда."}

    if not has_access(user_id, "READ_TEAM", team_id=team):
        return {"error": "Нет прав смотреть команду."}

    info = DB.one(
        """
        SELECT t.id, t.name, t.lead_user_id, lead.full_name AS lead_name
        FROM teams t
        LEFT JOIN employees lead ON lead.id = t.lead_user_id
        WHERE t.id = %s;
        """,
        (team,),
    )
    members = load_team_members(team)
    today = dt.date.today()

    salaries_visible = False
    if has_access(user_id, "VIEW_SALARY_SUBORDINATES"):
        salaries_visible = True
        for person in members:
            sal = get_current_salary(person["id"])
            person["salary"] = sal
    else:
        for person in members:
            person["salary"] = None

    answer = (
        f"Команда «{info.get('name') if info else 'без названия'}», участников: {len(members)}. "
        f"Лидер: {info.get('lead_name') if info else 'не задан'}. "
        f"Зарплаты {'видны' if salaries_visible else 'недоступны'}. "
        f"Сегодня {today:%d.%m.%Y}, ближайший ДР: {nearest_birthday(members)}."
    )

    return with_usage(
        {
            "team": info,
            "members": members,
            "salaries_visible": salaries_visible,
            "answer": answer,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Покажи состав именно моей команды: перечисли участников, роли и ближайшие ДР; зарплаты включай только если поле salaries_visible=true.",
    )


def nearest_birthday(members: List[Dict[str, Any]]) -> str:
    upcoming = [m for m in members if isinstance(m.get("days_to_birthday"), int)]
    if not upcoming:
        return "не нашёл дат"
    next_person = min(upcoming, key=lambda x: x["days_to_birthday"])
    days = next_person["days_to_birthday"]
    date_str = next_person["birth_date"]
    return f"{next_person['full_name']} ({date_str}), через {days} дн."


# Список сотрудников: "Собери пользователю список сотрудников так как он попросил. Отображай только те данные которые он запросил"
@mcp.tool(
    name="employee_search",
    description="Поиск сотрудников по имени/фамилии (ILIKE), телефону (цифры) или id. Возвращает список совпадений.",
)
def employee_search(
    query: str,
    requester_user_id: Optional[int] = None,
    fields: Optional[List[str]] = None,
    limit: int = 15,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    term = query.strip()
    if not term:
        return {"error": "Пустой запрос."}

    digits = normalize_phone(term)
    words = [w for w in term.split() if w]
    only_first_name = len(words) == 1 and not digits
    default_fields = ["id", "full_name", "position", "team_name", "phone", "email"]
    allowed_fields = {"id", "full_name", "email", "phone", "position", "team_name", "team_id", "manager_name"}
    requested_fields = default_fields if not fields else [f for f in fields if f in allowed_fields]
    if not requested_fields:
        requested_fields = default_fields
    safe_limit = max(1, min(limit, 30))

    filters = []
    params: List[Any] = []
    note = ""
    if digits:
        filters.append(
            "(e.id::text = %s OR e.external_id::text = %s OR e.telegram_user_id::text = %s "
            "OR regexp_replace(e.phone, '\\\\D', '', 'g') ILIKE %s)"
        )
        params.extend([digits, digits, digits, f"%{digits}%"])
    else:
        for w in words:
            filters.append("e.full_name ILIKE %s")
            params.append(f"%{w}%")
        if only_first_name:
            team = user_team_id(user_id)
            if team:
                filters.append("e.team_id = %s")
                params.append(team)
                note = "Показаны только сотрудники вашей команды, уточните по телефону или id при необходимости."
            else:
                note = "Много совпадений по имени; при отсутствии команды покажем всех."

    where_clause = " AND ".join(filters) if filters else "TRUE"
    rows = DB.all(
        f"""
        SELECT e.id, e.full_name, e.email, e.phone, e.position, e.team_id, t.name AS team_name, m.full_name AS manager_name
        FROM employees e
        LEFT JOIN teams t ON t.id = e.team_id
        LEFT JOIN employees m ON m.id = e.manager_id
        WHERE {where_clause}
        ORDER BY e.full_name
        LIMIT %s;
        """,
        tuple(params + [safe_limit]),
    )
    matches: List[Dict[str, Any]] = []
    for r in rows:
        entry = {k: v for k, v in r.items() if k in requested_fields}
        matches.append(entry)

    answer_parts = [f"Найдено сотрудников: {len(matches)}."]
    if only_first_name and note:
        answer_parts.append(note)
    if len(matches) > 1:
        answer_parts.append("Чтобы уточнить, выберите запись по порядку, телефону или id.")
    answer = " ".join(answer_parts)

    return with_usage(
        {
            "query": term,
            "matches": matches,
            "answer": answer,
            "note": note,
            "requested_fields": requested_fields,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Собери пользователю список сотрудников так, как он попросил: выводи только нужные поля, объясни как уточнить результат (id, телефон или номер в списке).",
    )


@mcp.tool(
    name="team_search",
    description="Поиск другой команды по названию или id и состав без зарплат.",
)
def team_search(term: str, limit: int = 5) -> Dict[str, Any]:
    search = term.strip()
    if not search:
        return {"error": "Пустой запрос для команды."}
    digits = normalize_phone(search)
    filters = []
    params: List[Any] = []
    if digits:
        filters.append("(t.id::text = %s OR t.external_id::text = %s)")
        params.extend([digits, digits])
    filters.append("t.name ILIKE %s")
    params.append(f"%{search}%")

    rows = DB.all(
        f"""
        SELECT t.id, t.name, t.lead_user_id, lead.full_name AS lead_name
        FROM teams t
        LEFT JOIN employees lead ON lead.id = t.lead_user_id
        WHERE {' OR '.join(filters)}
        ORDER BY t.name
        LIMIT %s;
        """,
        tuple(params + [max(1, min(limit, 20))]),
    )

    teams_payload: List[Dict[str, Any]] = []
    for t in rows:
        members = DB.all(
            """
            SELECT e.id, e.full_name, e.position, e.email, e.manager_id, m.full_name AS manager_name
            FROM employees e
            LEFT JOIN employees m ON m.id = e.manager_id
            WHERE e.team_id = %s
            ORDER BY e.full_name;
            """,
            (t["id"],),
        )
        teams_payload.append({**t, "members": members})

    answer = f"Найдено команд: {len(teams_payload)}."
    return with_usage(
        {
            "query": search,
            "teams": teams_payload,
            "answer": answer,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Покажи списком найденные команды и их состав без зарплат; если совпадений много — предложи уточнить название или id.",
    )


@mcp.tool(
    name="team_salary_analytics",
    description="Аналитика зарплат команды: среднее/медиана/минимум/максимум и диапазон, опционально фильтр по позиции.",
)
def team_salary_analytics(
    requester_user_id: Optional[int] = None,
    team_id: Optional[int] = None,
    position_contains: Optional[str] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    team = team_id or user_team_id(user_id)
    if not team:
        return {"error": "У сотрудника не заполнена команда."}
    if not has_access(user_id, "VIEW_SALARY_SUBORDINATES", team_id=team):
        return {"error": "Нет прав смотреть зарплатную аналитику команды."}

    members = load_team_members(team)
    filter_term = (position_contains or "").lower().strip()
    ids: List[int] = []
    for m in members:
        if filter_term and filter_term not in (m.get("position") or "").lower():
            continue
        ids.append(m["id"])

    if not ids:
        return {"error": "Нет сотрудников по заданному фильтру.", "team_id": team}

    salaries: List[float] = []
    for emp_id in ids:
        sal = get_current_salary(emp_id)
        if sal and sal.get("amount") is not None:
            salaries.append(float(sal["amount"]))

    if not salaries:
        return {"error": "Нет данных о зарплатах для указанного набора сотрудников.", "team_id": team}

    salaries_sorted = sorted(salaries)
    avg = sum(salaries) / len(salaries)
    if len(salaries_sorted) % 2 == 1:
        median = salaries_sorted[len(salaries_sorted) // 2]
    else:
        median = (salaries_sorted[len(salaries_sorted) // 2 - 1] + salaries_sorted[len(salaries_sorted) // 2]) / 2

    payload = {
        "team_id": team,
        "count": len(salaries),
        "min": min(salaries),
        "max": max(salaries),
        "avg": round(avg, 2),
        "median": round(median, 2),
        "position_filter": filter_term if filter_term else None,
    }
    answer = (
        f"Аналитика зарплат: {len(salaries)} сотрудников, среднее {payload['avg']:.2f}, медиана {payload['median']:.2f}, "
        f"диапазон {payload['min']:.0f}–{payload['max']:.0f}."
    )
    if filter_term:
        answer = f"По позициям содержащим «{filter_term}»: " + answer

    return with_usage(
        {**payload, "answer": answer, "generated_at": dt.datetime.utcnow().isoformat() + "Z"},
        "Озвучь диапазон, среднее и медиану без раскрытия окладов по именам; при необходимости предложи уточнить позицию или команду.",
    )


@mcp.tool(
    name="task_tracker_my_tasks",
    description=(
        "Задачи из трекера задач под названием *WB Track*: показывает задачи пользователя с дедлайнами, датой создания, номером спринта, "
        "кратким описанием, статусом и ссылкой. Можно задать sprint_number (по умолчанию текущий) и limit."
    ),
)
def task_tracker_my_tasks(
    requester_user_id: Optional[int] = None,
    sprint_number: Optional[int] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    safe_limit = max(1, min(limit, 50))
    effective_sprint = sprint_number if sprint_number is not None else current_sprint_number()

    tasks_raw = fetch_user_tasks(user_id, effective_sprint, safe_limit)
    tasks = [
        {
            "task": row["task_title"],
            "deadline": row["deadline"],
            "created_at": row["created_at"],
            "sprint_number": row["sprint_number"],
            "summary": row["summary"],
            "details_url": row["details_url"],
            "status": row["status"],
        }
        for row in tasks_raw
    ]

    if not tasks:
        return {
            "sprint_number": effective_sprint,
            "tasks": [],
            "answer": "Для текущего спринта задачи не найдены.",
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        }

    today = dt.date.today()
    deadlines: List[dt.date] = []
    for t in tasks:
        try:
            if t["deadline"]:
                deadlines.append(dt.date.fromisoformat(t["deadline"]))
        except ValueError:
            continue
    upcoming = [d for d in deadlines if d >= today]
    nearest = min(upcoming or deadlines) if deadlines else None
    nearest_str = nearest.strftime("%d.%m") if nearest else "нет дат"

    answer = (
        f"Спринт #{effective_sprint}: найдено задач — {len(tasks)}. "
        f"Ближайший дедлайн {nearest_str}."
    )
    return with_usage(
        {
            "sprint_number": effective_sprint,
            "tasks": tasks,
            "answer": answer,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Покажи задачи списком, сохрани порядок из tasks; можно кратко озвучить дедлайны и ближайший, ссылки бери из details_url.",
    )


# Создать заявку: "Если данных не достаточно для создания заявки, запроси у пользователя подробности о них (например для отпуска  и увольнения обязательно требуется указать даты)"
@mcp.tool(
    name="statement_create",
    description=(
        "Создать заявление: подача заявления на отпуск, увольнение или в свободной форме. "
        "Категория обязательна (варианты: «отпуск», «увольнение», «другое»). Для отпусков и увольнений "
        "обязательно указывайте желаемые даты, для отпуска также требуется тип (очередной/за свой счёт)."
    ),
)
def statement_create(
    category: str,
    text: str,
    desired_start: Optional[str] = None,
    desired_end: Optional[str] = None,
    vacation_kind: Optional[str] = None,
    requester_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    allowed = {"отпуск", "увольнение", "другое"}
    if category.lower() not in allowed:
        return {"error": f"Категория должна быть одной из: {', '.join(sorted(allowed))}."}
    if not text.strip():
        return {"error": "Текст заявления пустой."}

    missing: List[str] = []
    ds = dt.date.fromisoformat(desired_start) if desired_start else None
    de = dt.date.fromisoformat(desired_end) if desired_end else None

    if category.lower() in {"отпуск", "увольнение"}:
        if ds is None or de is None:
            missing.append("укажите даты начала и конца")
    if category.lower() == "отпуск":
        valid_kinds = {"очередной", "за свой счёт", "за свой счет"}
        if not vacation_kind or vacation_kind.lower() not in valid_kinds:
            missing.append("уточните тип отпуска: очередной или за свой счёт")
        else:
            vacation_kind = "за свой счёт" if "счет" in vacation_kind.lower() else "очередной"

    if missing:
        return {
            "error": "Недостаточно данных для создания заявления: " + "; ".join(missing) + ".",
            "needed": missing,
            "how_to_use": "Запроси у пользователя недостающие поля (даты, тип отпуска) и повторно вызови создание заявления.",
        }

    if ds and de and ds > de:
        return {"error": "Дата начала не может быть позже даты окончания."}

    row = DB.exec(
        """
        INSERT INTO employee_statements (employee_id, category, body, desired_start, desired_end, vacation_kind)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, status, created_at;
        """,
        (user_id, category.lower(), text.strip(), ds, de, vacation_kind),
        returning=True,
    )
    return with_usage(
        {
            "created": {
                "statement_id": row["id"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat(),
            },
            "answer": f"Заявление #{row['id']} сохранено. Категория: {category}. Статус: {row['status']}.",
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Сообщи пользователю номер заявки и статусы; если был отпуск — упомяни тип и даты, для уточнений переспроси недостающие поля.",
    )


@mcp.tool(
    name="statement_list",
    description=(
        "Список заявлений: показывает собственные заявления (scope=self) или заявки всей команды "
        "(scope=team, нужны права руководителя). Возвращает статус, даты и автора."
    ),
)
def statement_list(
    requester_user_id: Optional[int] = None,
    scope: str = "self",
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    if scope not in {"self", "team"}:
        return {"error": "scope должен быть self или team"}

    params: Tuple[Any, ...]
    if scope == "self":
        q = """
        SELECT id, category, body, desired_start, desired_end, status, created_at
        FROM employee_statements
        WHERE employee_id = %s AND deleted_at IS NULL
        ORDER BY created_at DESC;
        """
        params = (user_id,)
    else:
        team = user_team_id(user_id)
        if not team:
            return {"error": "Нет команды, нельзя показать командные заявки."}
        if not has_access(user_id, "READ_TEAM", team_id=team):
            return {"error": "Нет прав на заявки команды."}
        q = """
        SELECT s.id, s.category, s.body, s.desired_start, s.desired_end, s.status, s.created_at,
               e.full_name AS author_name
        FROM employee_statements s
        JOIN employees e ON e.id = s.employee_id
        WHERE e.team_id = %s AND s.deleted_at IS NULL
        ORDER BY s.created_at DESC;
        """
        params = (team,)

    rows = DB.all(q, params)
    for r in rows:
        if isinstance(r.get("desired_start"), dt.date):
            r["desired_start"] = r["desired_start"].isoformat()
        if isinstance(r.get("desired_end"), dt.date):
            r["desired_end"] = r["desired_end"].isoformat()
        if isinstance(r.get("created_at"), dt.datetime):
            r["created_at"] = r["created_at"].isoformat()

    answer = (
        f"Найдено заявлений: {len(rows)}. "
        + ("Это ваши личные заявки." if scope == "self" else "Это заявки вашей команды.")
    )
    return with_usage(
        {"scope": scope, "statements": rows, "answer": answer, "generated_at": dt.datetime.utcnow().isoformat() + "Z"},
        "Покажи список заявлений в указанном scope; если нужно продолжить работу, уточняй по id заявки.",
    )


@mcp.tool(
    name="statement_active",
    description="Отобразить активные (не удалённые) заявления: свои или команды (scope=self|team).",
)
def statement_active(
    requester_user_id: Optional[int] = None,
    scope: str = "self",
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    if scope not in {"self", "team"}:
        return {"error": "scope должен быть self или team"}

    params: Tuple[Any, ...]
    if scope == "self":
        q = """
        SELECT id, category, body, desired_start, desired_end, status, created_at, vacation_kind
        FROM employee_statements
        WHERE employee_id = %s AND deleted_at IS NULL AND status <> 'отменена'
        ORDER BY created_at DESC;
        """
        params = (user_id,)
    else:
        team = user_team_id(user_id)
        if not team:
            return {"error": "Нет команды, нельзя показать командные заявки."}
        if not has_access(user_id, "READ_TEAM", team_id=team):
            return {"error": "Нет прав на заявки команды."}
        q = """
        SELECT s.id, s.category, s.body, s.desired_start, s.desired_end, s.status, s.created_at, s.vacation_kind,
               e.full_name AS author_name
        FROM employee_statements s
        JOIN employees e ON e.id = s.employee_id
        WHERE e.team_id = %s AND s.deleted_at IS NULL AND s.status <> 'отменена'
        ORDER BY s.created_at DESC;
        """
        params = (team,)

    rows = DB.all(q, params)
    for r in rows:
        if isinstance(r.get("desired_start"), dt.date):
            r["desired_start"] = r["desired_start"].isoformat()
        if isinstance(r.get("desired_end"), dt.date):
            r["desired_end"] = r["desired_end"].isoformat()
        if isinstance(r.get("created_at"), dt.datetime):
            r["created_at"] = r["created_at"].isoformat()

    return with_usage(
        {"scope": scope, "statements": rows, "answer": f"Активных заявлений: {len(rows)}.", "generated_at": dt.datetime.utcnow().isoformat() + "Z"},
        "Покажи только актуальные заявки (без удалённых и отменённых); объясни пользователю, как указать id для отмены или детализации.",
    )


@mcp.tool(
    name="statement_cancel",
    description="Отменить (soft delete) заявление по id. Доступно автору либо руководителю команды сотрудника.",
)
def statement_cancel(
    statement_id: int,
    requester_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    row = DB.one(
        """
        SELECT s.id, s.employee_id, s.status, s.deleted_at, s.category, s.desired_start, s.desired_end, s.vacation_kind,
               e.team_id
        FROM employee_statements s
        JOIN employees e ON e.id = s.employee_id
        WHERE s.id = %s;
        """,
        (statement_id,),
    )
    if not row:
        return {"error": "Заявка не найдена."}
    if row.get("deleted_at"):
        return {"error": "Заявка уже отменена."}

    owns = row["employee_id"] == user_id
    same_team = row.get("team_id")
    allowed = owns or is_manager_of(user_id, row["employee_id"]) or (
        same_team and has_access(user_id, "READ_TEAM", team_id=same_team)
    )
    if not allowed:
        return {"error": "Нет прав отменить эту заявку."}

    now = dt.datetime.utcnow()
    DB.exec(
        "UPDATE employee_statements SET deleted_at = %s, status = 'отменена' WHERE id = %s;",
        (now, statement_id),
    )
    return with_usage(
        {
            "cancelled_statement_id": statement_id,
            "answer": f"Заявка #{statement_id} помечена как отменённая.",
            "cancelled_at": now.isoformat() + "Z",
        },
        "Подтверди отмену пользователю и предложи заново создать заявку при необходимости, сохрани id для ссылок.",
    )


@mcp.tool(
    name="calendar_my_events",
    description="Мой календарь: события на сегодня или неделю (week).",
)
def calendar_my_events(
    requester_user_id: Optional[int] = None,
    scope: str = "today",
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    scope_norm = scope.lower()
    if scope_norm not in {"today", "week"}:
        return {"error": "scope должен быть today или week"}

    now = dt.datetime.now()
    start = dt.datetime.combine(now.date(), dt.time.min)
    if scope_norm == "today":
        end = start + dt.timedelta(days=1)
    else:
        end = start + dt.timedelta(days=7)

    events = DB.all(
        """
        SELECT id, title, starts_at, ends_at, visibility
        FROM calendar_events
        WHERE employee_id = %s
          AND starts_at < %s
          AND ends_at >= %s
        ORDER BY starts_at;
        """,
        (user_id, end, start),
    )
    for ev in events:
        if isinstance(ev.get("starts_at"), dt.datetime):
            ev["starts_at"] = ev["starts_at"].isoformat()
        if isinstance(ev.get("ends_at"), dt.datetime):
            ev["ends_at"] = ev["ends_at"].isoformat()

    window_label = "сегодня" if scope_norm == "today" else "неделю"
    answer = f"События на {window_label}: {len(events)}."
    return with_usage(
        {
            "scope": scope_norm,
            "events": events,
            "answer": answer,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Покажи события указанного периода; если их нет, прямо скажи об этом и предложи добавить встречу вручную.",
    )


@mcp.tool(
    name="ndfl_order",
    description="Заказать справку 2-НДФЛ за указанный год. Требуется год и факт работы в этот год.",
)
def ndfl_order(
    year: int,
    requester_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    current_year = dt.date.today().year
    if year < 2000 or year > current_year + 1:
        return {"error": "Некорректный год справки."}

    emp = DB.one("SELECT hired_at, fired_at, full_name FROM employees WHERE id = %s;", (user_id,))
    if not emp:
        return {"error": "Сотрудник не найден."}

    year_start = dt.date(year, 1, 1)
    year_end = dt.date(year, 12, 31)
    hired_at: Optional[dt.date] = emp.get("hired_at")
    fired_at: Optional[dt.date] = emp.get("fired_at")
    worked = hired_at and hired_at <= year_end and (fired_at is None or fired_at >= year_start)
    if not worked:
        return with_usage(
            {
                "year": year,
                "available": False,
                "answer": f"Сотрудник {emp.get('full_name') or ''} не работал в {year} году, справку 2-НДФЛ оформить нельзя.",
            },
            "Сообщи пользователю, что справка недоступна из-за отсутствия работы в выбранном году.",
        )

    link_row = DB.one("SELECT download_url FROM ndfl_links WHERE year = %s;", (year,))
    if not link_row:
        return with_usage(
            {
                "year": year,
                "available": False,
                "answer": "Ссылка на справку не задана в БД, обратитесь в бухгалтерию.",
            },
            "Сообщи пользователю, что ссылка отсутствует, и подсказать обратиться в бухгалтерию или запросить другой год.",
        )

    return with_usage(
        {
            "year": year,
            "available": True,
            "download_url": link_row["download_url"],
            "answer": f"Справка 2-НДФЛ за {year} готова, ссылка: {link_row['download_url']}.",
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        },
        "Выдай ссылку и уточни, что она на нужный год; при необходимости предложи сохранить или отправить пользователю.",
    )


@mcp.tool(
    name="session_log_intent",
    description=(
        "Зафиксировать намерение: сохраняет краткое текстовое описание цели пользователя. "
        "Полезно перед вызовом инструментов, чтобы сервер понимал контекст."
    ),
)
def session_log_intent(
    intent: str,
    requester_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    if not intent.strip():
        return {"error": "Опишите хотя бы одно предложение."}
    entry = {
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        "requester_user_id": user_id,
        "intent": intent.strip(),
    }
    _INTENT_LOG.append(entry)
    return with_usage(
        {"logged": entry, "recent": list(_INTENT_LOG)[-5:]},
        "Используй это как журнал намерений: подтверждай, что цель записана, и напоминай про последние намерения.",
    )


@mcp.tool(
    name="session_intent_history",
    description=(
        "История намерений: возвращает последние записанные intent'ы (по умолчанию 5, можно ограничить параметром limit)."
    ),
)
def session_intent_history(limit: int = 5) -> Dict[str, Any]:
    limit = max(1, min(limit, len(_INTENT_LOG)))
    return with_usage(
        {"intents": list(_INTENT_LOG)[-limit:]},
        "Расскажи последние намерения кратко, чтобы понять контекст пользователя, без лишних домыслов.",
    )


# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------

def main() -> None:
    try:
        mcp.run(transport="streamable-http")
    finally:
        DB.close()


if __name__ == "__main__":
    main()
