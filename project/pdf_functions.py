import fpdf
from project.main import CELL_HEIGHT_PDF, TEXT_HEIGHT_PDF
import text_colors


class PDFHelper:
    # noinspection PyMethodMayBeStatic
    def add_cols_names(self, pdf: fpdf.FPDF, col_names: tuple, cols_width: list, clr: text_colors.Colors) -> None:
        """
        Функция для добавления заголовков столбцов в таблицу
        :param pdf: pdf-документ для добавления
        :param col_names: названия столбцов таблицы
        :param cols_width: список ширин столбцов
        :param clr: объект класса Clr для работы с цветом
        :return: None
        """
        pdf.set_font('times b', size=18)  # Установка жирного шрифта для наименований столбцов
        for i in range(len(col_names)):
            pdf.set_fill_color(*clr.FILL_GRAY)  # Установка цвета для заливки последней ячейки серым
            if i != 1:
                width = pdf.get_string_width(col_names[i]) + 20
                pdf.cell(w=width, h=CELL_HEIGHT_PDF, txt=col_names[i],
                         border=1, align='C', fill=i == len(col_names) - 1)
                cols_width.append(width)  # Добавление ширины в список
                if i == len(col_names) - 1:
                    pdf.ln()
            else:
                width = pdf.get_string_width(col_names[i]) + 80  # Увеличенная ширина для корректной записи судов
                pdf.cell(w=width, h=CELL_HEIGHT_PDF, txt=col_names[i], border=1, align='C')
                cols_width.append(width)  # Добавление ширины столбца в список
        pdf.set_font('times', size=18)  # Установка обычного шрифта для заполнения документа

    # noinspection PyMethodMayBeStatic
    def space_left(self, pdf: fpdf.FPDF) -> float:
        """
        Функция для вычисления высоты свободной рабочей области
        :param pdf: pdf-документ для вычисления
        :return: высота свободной рабочей области
        """
        space = pdf.h - pdf.t_margin - pdf.b_margin - pdf.get_y()
        return space

    # noinspection PyMethodMayBeStatic
    def pdf_header(self, pdf: fpdf.FPDF, text: str, allign: str = 'L') -> None:
        """
        Функция для добавления текстовых заголовков в пдф-документ
        :param pdf: объект fpdf.FDF (документ отчета)
        :param text: текст для добавления в документ
        :param allign: выравнивание текста ('L', 'C', 'R')
        :return: None
        """
        pdf.c_margin = 0
        pdf.set_font('times b', size=18)
        pdf.cell(w=0, h=TEXT_HEIGHT_PDF, txt=text,
                 align=allign, ln=1)
        pdf.set_font('times', size=18)
