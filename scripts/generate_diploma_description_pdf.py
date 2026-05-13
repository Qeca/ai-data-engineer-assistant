from pathlib import Path

from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Spacer, Paragraph


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "docs" / "diploma_description.md"
OUTPUT_PATH = ROOT / "docs" / "diploma_description.pdf"
FONT_DIR = Path("/System/Library/Fonts/Supplemental")


class DiplomaPdfBuilder:
    def __init__(self, source_path: Path, output_path: Path) -> None:
        self.source_path = source_path
        self.output_path = output_path
        self.font_name = "TimesNewRoman"
        self.bold_font_name = "TimesNewRoman-Bold"

    def build(self) -> None:
        self._register_fonts()
        title, paragraphs = self._read_markdown()
        doc = SimpleDocTemplate(
            str(self.output_path),
            pagesize=A4,
            rightMargin=2 * cm,
            leftMargin=3 * cm,
            topMargin=2 * cm,
            bottomMargin=2 * cm,
        )
        styles = self._styles()
        story = [Paragraph(title, styles["DiplomaTitle"]), Spacer(1, 0.8 * cm)]
        for paragraph in paragraphs:
            story.append(Paragraph(paragraph, styles["DiplomaBody"]))
            story.append(Spacer(1, 0.35 * cm))
        doc.build(story)

    def _register_fonts(self) -> None:
        pdfmetrics.registerFont(TTFont(self.font_name, str(FONT_DIR / "Times New Roman.ttf")))
        pdfmetrics.registerFont(TTFont(self.bold_font_name, str(FONT_DIR / "Times New Roman Bold.ttf")))

    def _read_markdown(self) -> tuple[str, list[str]]:
        lines = self.source_path.read_text(encoding="utf-8").splitlines()
        title = lines[0].removeprefix("#").strip()
        paragraphs: list[str] = []
        current: list[str] = []
        for line in lines[1:]:
            clean = line.strip()
            if not clean:
                if current:
                    paragraphs.append(" ".join(current))
                    current = []
                continue
            current.append(clean)
        if current:
            paragraphs.append(" ".join(current))
        return title, paragraphs

    def _styles(self) -> dict[str, ParagraphStyle]:
        base = getSampleStyleSheet()
        return {
            "DiplomaTitle": ParagraphStyle(
                "DiplomaTitle",
                parent=base["Title"],
                fontName=self.bold_font_name,
                fontSize=16,
                leading=20,
                alignment=TA_CENTER,
                spaceAfter=8,
            ),
            "DiplomaBody": ParagraphStyle(
                "DiplomaBody",
                parent=base["BodyText"],
                fontName=self.font_name,
                fontSize=14,
                leading=20,
                firstLineIndent=1.25 * cm,
                alignment=TA_JUSTIFY,
            ),
        }


def main() -> None:
    DiplomaPdfBuilder(SOURCE_PATH, OUTPUT_PATH).build()
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
