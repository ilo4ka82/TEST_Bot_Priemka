import io
import logging
import pandas as pd
import database_operations as db

logger = logging.getLogger(__name__)


def format_seconds_to_hhmmss(seconds_val) -> str:
    """Форматирует секунды в строку HH:MM:SS. Возвращает 'Активна' для некорректных значений."""
    if pd.isna(seconds_val) or not isinstance(seconds_val, (int, float)) or seconds_val < 0:
        return "Активна"
    seconds_val = int(seconds_val)
    hours = seconds_val // 3600
    minutes = (seconds_val % 3600) // 60
    secs = seconds_val % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_attendance_data(start_date, end_date, sector_key: str = None) -> list:
    """Возвращает данные посещаемости за период из БД."""
    return db.get_attendance_data_for_period(start_date, end_date, sector_key)


async def generate_excel_report(
    report_data: list,
    report_info: dict,
    selected_sector_key: str,
) -> bytes:
    """
    Генерирует Excel-отчёт по данным посещаемости.
    Возвращает байты готового .xlsx файла.
    """
    if not report_data:
        logger.info("Нет данных для генерации Excel-отчёта.")
        df_empty = pd.DataFrame([{"Сообщение": "Нет данных для отображения в выбранных параметрах"}])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_empty.to_excel(writer, sheet_name="Отчет", index=False)
        return output.getvalue()

    df_all_data = pd.DataFrame(report_data)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        def prepare_and_write_sheet(df_sheet_data, sheet_name_param):
            if df_sheet_data.empty:
                return

            df_details = df_sheet_data.copy()
            df_details["session_start_time"] = pd.to_datetime(df_details["session_start_time"], errors="coerce")
            df_details["session_end_time"] = pd.to_datetime(df_details["session_end_time"], errors="coerce")

            duration = df_details["session_end_time"] - df_details["session_start_time"]
            df_details["Длительность сессии"] = duration.dt.total_seconds().apply(format_seconds_to_hhmmss)

            df_details.rename(columns={
                "application_full_name": "ФИО",
                "username": "Telegram Username",
                "application_department": "Сектор",
                "session_start_time": "Начало сессии",
                "session_end_time": "Конец сессии",
            }, inplace=True)

            final_columns = ["ФИО", "Telegram Username", "Сектор", "Начало сессии", "Конец сессии", "Длительность сессии"]
            existing_columns = [col for col in final_columns if col in df_details.columns]
            df_sheet_final = df_details[existing_columns]

            df_sorted = df_sheet_final.sort_values(
                by=["ФИО", "Начало сессии"],
                ascending=[True, False],
            ).reset_index(drop=True)

            df_sorted["Начало сессии"] = df_sorted["Начало сессии"].dt.strftime("%Y-%m-%d %H:%M:%S").replace("NaT", "Активна")
            df_sorted["Конец сессии"] = df_sorted["Конец сессии"].dt.strftime("%Y-%m-%d %H:%M:%S").replace("NaT", "")

            new_rows = []
            last_name = None
            blank_row = pd.Series([""] * len(df_sorted.columns), index=df_sorted.columns)

            for _, row in df_sorted.iterrows():
                current_name = row["ФИО"]
                if last_name is not None and current_name != last_name:
                    new_rows.append(blank_row)
                new_rows.append(row)
                last_name = current_name

            df_with_spacing = pd.DataFrame(new_rows)
            df_with_spacing.to_excel(writer, sheet_name=sheet_name_param, index=False)

            worksheet = writer.book[sheet_name_param]
            for column_cells in worksheet.columns:
                try:
                    max_length = max(
                        (len(str(cell.value)) for cell in column_cells if cell.value is not None),
                        default=0,
                    )
                    worksheet.column_dimensions[column_cells[0].column_letter].width = max(max_length + 2, 10)
                except Exception as e:
                    logger.debug(f"Ошибка автоширины для столбца на листе '{sheet_name_param}': {e}")

            logger.info(f"Лист '{sheet_name_param}' успешно добавлен в Excel.")

        if selected_sector_key.upper() == "ALL":
            if "application_department" not in df_all_data.columns:
                logger.error("Столбец 'application_department' отсутствует — не могу разделить по секторам.")
                prepare_and_write_sheet(df_all_data, "Все_данные_ошибка_группировки")
            else:
                df_all_data["normalized_department_for_sheet"] = (
                    df_all_data["application_department"].astype(str).str.upper().fillna("Без_сектора")
                )
                for dept in df_all_data["normalized_department_for_sheet"].unique():
                    safe_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in dept)[:31]
                    df_dept = df_all_data[df_all_data["normalized_department_for_sheet"] == dept]
                    if not df_dept.empty:
                        prepare_and_write_sheet(df_dept.copy(), safe_name)
        else:
            sheet_name_base = report_info.get("sector_display_name", "Детализация").replace(" ", "_")
            safe_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in sheet_name_base)[:31]
            prepare_and_write_sheet(df_all_data, safe_name)

    logger.info("Excel-файл сгенерирован в памяти.")
    return output.getvalue()