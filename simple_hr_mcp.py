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

mcp = FastMCP(
    name="HR Simple MCP",
    instructions=(
        "Ты подключён к HR Simple MCP. Перед стартом опиши цель через session.log_intent. "
        "Доступные инструменты:\n"
        "• profile_summary — «Мои данные»: ФИО, роль, команда, зарплата, будущие отпуска, "
        "  примерный баланс отпускных дней.\n"
        "• team_overview — «Моя команда»: состав, дни рождения, кто чем занимается, "
        "  зарплаты (если есть права) и короткий вывод.\n"
        "• statement_create — «Сформировать заявление» в свободной форме "
        "  (категории: отпуск, увольнение, свободная форма).\n"
        "• statement_list — «Список поданных заявлений» (свои или команды, если ты руководитель).\n"
        "• session_log_intent и session_intent_history — чтобы фиксировать и просматривать намерения.\n"
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
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    DB.exec(q)


ensure_statements_table()


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


# -----------------------------------------------------------------------------
# Инструменты
# -----------------------------------------------------------------------------

@mcp.tool(name="profile_summary", description="Мои данные: карточка сотрудника, зарплата, будущие отпуска, баланс дней.")
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

    return {
        "employee": employee,
        "salary": salary,
        "planned_leaves": leaves,
        "estimated_leave_balance_days": balance,
        "answer": answer,
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
    }


@mcp.tool(name="team_overview", description="Моя команда: участники, роли, ДР, зарплаты (если доступны) и вывод.")
def team_overview(
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

    return {
        "team": info,
        "members": members,
        "salaries_visible": salaries_visible,
        "answer": answer,
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
    }


def nearest_birthday(members: List[Dict[str, Any]]) -> str:
    upcoming = [m for m in members if isinstance(m.get("days_to_birthday"), int)]
    if not upcoming:
        return "не нашёл дат"
    next_person = min(upcoming, key=lambda x: x["days_to_birthday"])
    days = next_person["days_to_birthday"]
    date_str = next_person["birth_date"]
    return f"{next_person['full_name']} ({date_str}), через {days} дн."


@mcp.tool(name="statement_create", description="Создать заявление (категория + свободный текст).")
def statement_create(
    category: str,
    text: str,
    desired_start: Optional[str] = None,
    desired_end: Optional[str] = None,
    requester_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    user_id = resolve_requester_user_id(requester_user_id)
    allowed = {"отпуск", "увольнение", "другое"}
    if category.lower() not in allowed:
        return {"error": f"Категория должна быть одной из: {', '.join(sorted(allowed))}."}
    if not text.strip():
        return {"error": "Текст заявления пустой."}

    ds = dt.date.fromisoformat(desired_start) if desired_start else None
    de = dt.date.fromisoformat(desired_end) if desired_end else None
    row = DB.exec(
        """
        INSERT INTO employee_statements (employee_id, category, body, desired_start, desired_end)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, status, created_at;
        """,
        (user_id, category.lower(), text.strip(), ds, de),
        returning=True,
    )
    return {
        "created": {
            "statement_id": row["id"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
        },
        "answer": f"Заявление #{row['id']} сохранено. Категория: {category}. Статус: {row['status']}.",
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
    }


@mcp.tool(name="statement_list", description="Список поданных заявлений (свои или команды).")
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
        WHERE employee_id = %s
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
        WHERE e.team_id = %s
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
    return {"scope": scope, "statements": rows, "answer": answer, "generated_at": dt.datetime.utcnow().isoformat() + "Z"}


@mcp.tool(name="session_log_intent", description="Опишите намерение (что хотите увидеть).")
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
    return {"logged": entry, "recent": list(_INTENT_LOG)[-5:]}


@mcp.tool(name="session_intent_history", description="Покажи последние намерения (по умолчанию 5).")
def session_intent_history(limit: int = 5) -> Dict[str, Any]:
    limit = max(1, min(limit, len(_INTENT_LOG)))
    return {"intents": list(_INTENT_LOG)[-limit:]}


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
