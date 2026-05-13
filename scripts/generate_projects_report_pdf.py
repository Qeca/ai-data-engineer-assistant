from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "docs" / "projects_report.pdf"
FONT_DIR = Path("/System/Library/Fonts/Supplemental")
FONT_NAME = "TimesNewRoman"
FONT_BOLD_NAME = "TimesNewRoman-Bold"
GITHUB_PROFILE = "https://github.com/Qeca"


@dataclass(frozen=True)
class Project:
    title: str
    url: str
    description: str
    status: str = ""


MAIN_PROJECTS = (
    Project(
        "AI Data Engineer Assistant",
        "https://github.com/Qeca/ai-data-engineer-assistant",
        "Fullstack AI-приложение для data engineering workflows. Агент на LangGraph принимает запросы на естественном языке и выполняет действия через tools: read-only SQL, каталог данных, Airflow DAGs, Spark jobs, внешние MCP-интеграции, запись артефактов, Git-версии и Docker sandbox validation. Стек: Next.js, TypeScript, FastAPI, SQLAlchemy, LangGraph, OpenAI/OpenRouter tool calling, Docker Compose.",
        "",
    ),
    Project(
        "ITMO Agent",
        "https://github.com/Qeca/itmo-agent",
        "AI-агент для задач и сценариев вокруг ITMO. Проект уже оформлен README и выглядит как основной публичный агентский проект для портфолио.",
    ),
    Project(
        "ITMO Small Multiagent",
        "https://github.com/Qeca/itmo_small_multiagent",
        "Небольшой multi-agent framework с orchestrator, агентами и tool-интерфейсами. Есть CLI/Python/search/memory tools, FAISS-память, YAML prompts и примеры запуска. Хороший проект для демонстрации агентской архитектуры и tool orchestration.",
    ),
    Project(
        "Time Series Forecasting",
        "https://github.com/Qeca/timeseries",
        "ML/API-проект для прогнозирования временных рядов. Включает модели Informer/LSTM/Newsformer, FastAPI endpoint POST /predict/{horizon}, парсеры данных MOEX/CB/RBK и Telegram bot. Подходит для демонстрации ML engineering, API и data pipeline навыков.",
    ),
    Project(
        "Telegram Events Bot",
        "https://github.com/Qeca/BOT",
        "Telegram bot на aiogram для агрегации мероприятий. Есть роли student/admin, регистрация, список событий, запись на событие, подтверждение через геолокацию, admin-управление событиями/конкурсами и уведомления.",
    ),
    Project(
        "Mediaprofi",
        "https://github.com/Qeca/mediaprofi",
        "Frontend-прототип для SMM/контент-агентства: клиенты, публикации, статусы, календарь и планирование контента. Одностраничный HTML/CSS/JavaScript проект, полезен как лёгкий UI-пример.",
    ),
)

PRIVATE_PROJECTS = (
    Project(
        "Triton / vLLM Embeddings Benchmark",
        "https://github.com/Qeca/test_triton",
        "gRPC-сервис для сравнения скорости инференса эмбеддингов между vLLM и NVIDIA Triton на модели intfloat/e5-multilingual-large. В проекте есть proto/gRPC server, backend-переключение vLLM/Triton, конвертация модели под Triton, benchmark и autoscaling-эксперименты. Проект демонстрирует навыки ML infrastructure, serving и performance analysis.",
    ),
    Project(
        "Karak",
        "https://github.com/Qeca/karak",
        "Python/RAG/BI assistant: работа с документами, Chroma, CSV/SQL хранилища, smart QA, чат-панели и аналитические UI-компоненты. Проект показывает практический опыт разработки ассистентов для поиска, анализа и визуализации данных.",
    ),
)

def link(url: str, label: str | None = None) -> str:
    label = label or url
    return f'<a href="{escape(url)}" color="#1D4ED8">{escape(label)}</a>'


def paragraph(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


class ProjectsReportPdfBuilder:
    def build(self) -> None:
        self._register_fonts()
        styles = self._styles()
        doc = SimpleDocTemplate(
            str(OUTPUT_PATH),
            pagesize=A4,
            rightMargin=1.8 * cm,
            leftMargin=1.8 * cm,
            topMargin=1.7 * cm,
            bottomMargin=1.7 * cm,
            title="Портфолио проектов",
            author="Qeca",
        )

        story = [
            paragraph("Портфолио проектов", styles["Title"]),
            paragraph(f"GitHub: {link(GITHUB_PROFILE)}", styles["Meta"]),
            Spacer(1, 0.45 * cm),
            paragraph("Основные проекты", styles["Section"]),
        ]

        for project in MAIN_PROJECTS:
            self._append_project(story, styles, project)

        story.append(Spacer(1, 0.2 * cm))
        story.append(paragraph("Дополнительные исследовательские проекты", styles["Section"]))
        for project in PRIVATE_PROJECTS:
            self._append_project(story, styles, project)

        story.append(Spacer(1, 0.2 * cm))
        story.append(paragraph("Краткий список ссылок", styles["Section"]))
        all_links = (
            ("GitHub profile", GITHUB_PROFILE),
            *((project.title, project.url) for project in MAIN_PROJECTS),
            *((project.title, project.url) for project in PRIVATE_PROJECTS),
        )
        for title, url in all_links:
            story.append(paragraph(f"• {escape(title)}: {link(url)}", styles["Body"]))

        doc.build(story)

    @staticmethod
    def _append_project(story: list, styles: dict[str, ParagraphStyle], project: Project) -> None:
        story.append(Spacer(1, 0.18 * cm))
        story.append(paragraph(project.title, styles["ProjectTitle"]))
        story.append(paragraph(f"Репозиторий: {link(project.url)}", styles["Body"]))
        if project.status:
            story.append(paragraph(f"Статус: {escape(project.status)}", styles["Body"]))
        story.append(paragraph(escape(project.description), styles["Body"]))

    @staticmethod
    def _register_fonts() -> None:
        pdfmetrics.registerFont(TTFont(FONT_NAME, str(FONT_DIR / "Times New Roman.ttf")))
        pdfmetrics.registerFont(TTFont(FONT_BOLD_NAME, str(FONT_DIR / "Times New Roman Bold.ttf")))

    @staticmethod
    def _styles() -> dict[str, ParagraphStyle]:
        base = getSampleStyleSheet()
        return {
            "Title": ParagraphStyle(
                "Title",
                parent=base["Title"],
                fontName=FONT_BOLD_NAME,
                fontSize=18,
                leading=22,
                alignment=TA_LEFT,
                textColor=colors.HexColor("#111827"),
                spaceAfter=8,
            ),
            "Meta": ParagraphStyle(
                "Meta",
                parent=base["BodyText"],
                fontName=FONT_NAME,
                fontSize=10.5,
                leading=14,
                textColor=colors.HexColor("#374151"),
            ),
            "Section": ParagraphStyle(
                "Section",
                parent=base["Heading2"],
                fontName=FONT_BOLD_NAME,
                fontSize=13.5,
                leading=17,
                textColor=colors.HexColor("#111827"),
                spaceBefore=8,
                spaceAfter=4,
            ),
            "ProjectTitle": ParagraphStyle(
                "ProjectTitle",
                parent=base["Heading3"],
                fontName=FONT_BOLD_NAME,
                fontSize=11.5,
                leading=15,
                textColor=colors.HexColor("#111827"),
                spaceBefore=4,
                spaceAfter=2,
            ),
            "Body": ParagraphStyle(
                "Body",
                parent=base["BodyText"],
                fontName=FONT_NAME,
                fontSize=10.5,
                leading=14,
                textColor=colors.HexColor("#111827"),
                spaceAfter=3,
            ),
        }


def main() -> None:
    ProjectsReportPdfBuilder().build()
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
