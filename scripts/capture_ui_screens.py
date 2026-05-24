"""Автоматический обход UI приложения и скриншоты всех экранов.

Запуск:
    pip install --user --break-system-packages playwright
    python3 -m playwright install chromium
    python3 scripts/capture_ui_screens.py

Скриншоты сохраняются в docs/images/ui/.
"""

import os
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, Page

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "images" / "ui"
OUT.mkdir(parents=True, exist_ok=True)

FRONTEND = os.environ.get("FRONTEND_URL", "http://localhost:3002")
EMAIL = "admin@local.dev"
PASSWORD = "admin"

VIEWPORT = {"width": 1600, "height": 1000}

# Скрины по навигации; (имя файла, label кнопки nav-item)
SCREENS = [
    ("01_dashboard",      "Dashboard"),
    ("02_ai_agent",       "AI Agent"),
    ("03_sql_workspace",  "SQL Workspace"),
    ("04_pipelines",      "Pipelines"),
    ("05_airflow_dags",   "Airflow DAGs"),
    ("06_spark_jobs",     "Spark Jobs"),
    ("07_connections",    "Connections"),
    ("08_catalog",        "Data Catalog"),
    ("09_settings",       "Settings"),
    ("10_profile",        "Profile"),
]


def shot(page: Page, name: str, full: bool = True) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full)
    size = path.stat().st_size
    print(f"  shot {name} → {path.name} ({size//1024} KB)")


def login(page: Page) -> None:
    print(f"open {FRONTEND}")
    page.goto(FRONTEND, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector("input.input", timeout=15000)
    shot(page, "00_login_empty", full=False)

    inputs = page.locator("input.input")
    inputs.nth(0).fill(EMAIL)
    inputs.nth(1).fill(PASSWORD)
    page.locator('button.btn.btn-primary:has-text("Sign in")').click()
    # Wait until login panel disappears (dashboard or default screen renders)
    page.wait_for_selector("button.nav-item", timeout=20000)
    page.wait_for_timeout(800)
    print("logged in")


def navigate(page: Page, label: str) -> None:
    """Кликает по nav-item с заданным label.
    Profile-кнопка подписана user.full_name, поэтому ловим её отдельно (последняя)."""
    if label == "Profile":
        nav_items = page.locator("button.nav-item")
        nav_items.nth(nav_items.count() - 1).click()
    else:
        page.locator(f'button.nav-item:has-text("{label}")').first.click()
    page.wait_for_timeout(700)


def capture_chat_scenario(page: Page) -> None:
    """Отправить сообщение агенту и заскриншотить с ответом."""
    print("- AI Agent: scenario")
    navigate(page, "AI Agent")
    page.wait_for_timeout(500)
    shot(page, "02a_ai_agent_empty")

    # Создать новый чат (плюсик возле "Chats" / кнопка "Новый чат")
    for sel in ('button:has-text("Новый чат")', 'button:has-text("New chat")',
                'button[aria-label="New chat"]', '.chats-header button'):
        btn = page.locator(sel).first
        if btn.count() > 0:
            btn.click()
            page.wait_for_timeout(500)
            break

    # textarea для ввода запроса
    textarea = page.locator("textarea").first
    textarea.click()
    textarea.fill("Покажи топ-5 заказов по сумме за последние 7 дней")
    page.wait_for_timeout(300)
    shot(page, "02b_ai_agent_query_typed")

    # Send button (виден внизу справа в углу chat-input)
    sent = False
    for sel in ('button:has-text("Send")', 'button[aria-label="Send"]',
                'button[type="submit"]', 'button.send-button'):
        send_btn = page.locator(sel).first
        if send_btn.count() > 0 and send_btn.is_enabled():
            send_btn.click()
            sent = True
            break
    if not sent:
        # Fallback — Ctrl+Enter / Cmd+Enter (типичная связка для чат-инпутов)
        textarea.press("Meta+Enter")

    # Дождаться появления хотя бы одной user/assistant bubble в истории чата
    try:
        page.wait_for_selector(
            '.message, [class*="message"], [class*="bubble"], [class*="chat-message"]',
            timeout=15000, state="visible"
        )
    except Exception:
        pass
    page.wait_for_timeout(8000)
    shot(page, "02c_ai_agent_after_send")
    # Ждём дальше — пока агент отработает tool calls
    page.wait_for_timeout(25000)
    shot(page, "02d_ai_agent_response_full")


def capture_sql_scenario(page: Page) -> None:
    print("- SQL Workspace: scenario")
    navigate(page, "SQL Workspace")
    page.wait_for_timeout(700)
    shot(page, "03a_sql_workspace_empty")

    # Monaco editor — отправляем фокус и вставляем текст
    monaco = page.locator(".monaco-editor").first
    if monaco.count() > 0:
        monaco.click()
        page.keyboard.type("SELECT 1 AS hello, now() AS ts;")
        page.wait_for_timeout(300)
        shot(page, "03b_sql_workspace_with_query")
        # Найти кнопку «Run» или Execute
        for label in ("Выполнить", "Run", "Execute"):
            btn = page.locator(f'button:has-text("{label}")').first
            if btn.count() > 0:
                btn.click()
                page.wait_for_timeout(2500)
                shot(page, "03c_sql_workspace_result")
                break


def capture_connections_scenario(page: Page) -> None:
    print("- Connections: scenario")
    navigate(page, "Connections")
    page.wait_for_timeout(700)
    shot(page, "07a_connections_list")
    # Попробуем открыть форму добавления
    for label in ("Добавить", "Новое", "Add", "New"):
        btn = page.locator(f'button:has-text("{label}")').first
        if btn.count() > 0:
            btn.click()
            page.wait_for_timeout(500)
            shot(page, "07b_connections_add_form")
            # Закрыть
            page.keyboard.press("Escape")
            break


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport=VIEWPORT,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            device_scale_factor=2,
        )
        page = ctx.new_page()

        try:
            login(page)
        except Exception as e:
            print(f"login failed: {e}")
            shot(page, "ERR_login")
            return

        # Базовый обход всех экранов
        for name, label in SCREENS:
            print(f"- {name}: {label}")
            try:
                navigate(page, label)
                shot(page, name)
            except Exception as e:
                print(f"  ERROR navigating to {label}: {e}")

        # Углублённые сценарии
        try:
            capture_chat_scenario(page)
        except Exception as e:
            print(f"  agent scenario failed: {e}")
            shot(page, "ERR_agent")
        try:
            capture_sql_scenario(page)
        except Exception as e:
            print(f"  sql scenario failed: {e}")
            shot(page, "ERR_sql")
        try:
            capture_connections_scenario(page)
        except Exception as e:
            print(f"  connections scenario failed: {e}")

        browser.close()
        files = sorted(OUT.glob("*.png"))
        print(f"\nTotal: {len(files)} screenshots → {OUT}")


if __name__ == "__main__":
    main()
