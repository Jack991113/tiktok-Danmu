import math
import os
import platform
import subprocess
import time
import unicodedata
from typing import Any, Dict, List, Optional

IS_WINDOWS = platform.system() == 'Windows'
if IS_WINDOWS:
    try:
        import win32print
        import win32api
        import win32gui
        import win32ui
        import win32con
    except Exception:
        win32print = None
        win32api = None
        win32gui = None
        win32ui = None
        win32con = None


CANVAS_UNITS_PER_MM = 5.0


def _positive_float(value: Any, fallback: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(fallback)
    return parsed if parsed > 0 else float(fallback)


def _format_mm(value: float) -> str:
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def parse_paper_size_mm(value: Any, default: tuple[float, float] = (40.0, 30.0)) -> tuple[float, float]:
    raw = str(value or "").lower().replace("mm", "").replace(" ", "").replace("×", "x")
    parts = raw.split("x", 1)
    if len(parts) != 2:
        return float(default[0]), float(default[1])
    return _positive_float(parts[0], default[0]), _positive_float(parts[1], default[1])


def format_paper_size_mm(width_mm: Any, height_mm: Any) -> str:
    width = _positive_float(width_mm, 40.0)
    height = _positive_float(height_mm, 30.0)
    return f"{_format_mm(width)}x{_format_mm(height)}"


def mm_to_devmode_units(value_mm: Any) -> int:
    return int(math.floor((_positive_float(value_mm, 0.1) * 10.0) + 0.5))


def calculate_canvas_geometry(
    payload: Dict[str, Any],
    dpi_x: int,
    dpi_y: int,
    physical_offset_x: int = 0,
    physical_offset_y: int = 0,
) -> Dict[str, float]:
    canvas_w = max(1, int(payload.get("canvas_w", 200)))
    canvas_h = max(1, int(payload.get("canvas_h", 150)))
    paper_w_mm = _positive_float(payload.get("paper_mm_w"), canvas_w / CANVAS_UNITS_PER_MM)
    paper_h_mm = _positive_float(payload.get("paper_mm_h"), canvas_h / CANVAS_UNITS_PER_MM)
    page_w_px = paper_w_mm * max(1, int(dpi_x)) / 25.4
    page_h_px = paper_h_mm * max(1, int(dpi_y)) / 25.4
    return {
        "origin_x": -int(physical_offset_x),
        "origin_y": -int(physical_offset_y),
        "scale_x": page_w_px / canvas_w,
        "scale_y": page_h_px / canvas_h,
        "page_w_px": page_w_px,
        "page_h_px": page_h_px,
        "paper_w_mm": paper_w_mm,
        "paper_h_mm": paper_h_mm,
    }


def validate_physical_page_size(
    width_mm: Any,
    height_mm: Any,
    physical_width_px: int,
    physical_height_px: int,
    dpi_x: int,
    dpi_y: int,
) -> tuple[bool, str]:
    requested_w = _positive_float(width_mm, 40.0)
    requested_h = _positive_float(height_mm, 30.0)
    if min(int(physical_width_px), int(physical_height_px), int(dpi_x), int(dpi_y)) <= 0:
        return True, "physical_page=unavailable"
    actual_w = float(physical_width_px) * 25.4 / float(dpi_x)
    actual_h = float(physical_height_px) * 25.4 / float(dpi_y)
    tolerance_w = max(0.8, requested_w * 0.02)
    tolerance_h = max(0.8, requested_h * 0.02)
    matches = abs(actual_w - requested_w) <= tolerance_w and abs(actual_h - requested_h) <= tolerance_h
    detail = (
        f"requested={format_paper_size_mm(requested_w, requested_h)}mm; "
        f"physical={_format_mm(actual_w)}x{_format_mm(actual_h)}mm"
    )
    return matches, detail


def detect_printers() -> List[str]:
    """Return a list of available printer names.

    On Unix uses `lpstat`. On Windows tries `win32print` if available.
    """
    if IS_WINDOWS:
        if win32print is None:
            return []
        printers = []
        try:
            for flags, name, desc, addr in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL | win32print.PRINTER_ENUM_CONNECTIONS):
                printers.append(name)
        except Exception:
            return []
        return printers

    try:
        out = subprocess.check_output(["lpstat", "-p"], stderr=subprocess.DEVNULL, text=True)
        printers = []
        for line in out.splitlines():
            if line.startswith("printer "):
                parts = line.split()
                if len(parts) >= 2:
                    printers.append(parts[1])
        return printers
    except Exception:
        return []


def get_default_printer() -> str:
    if IS_WINDOWS and win32print is not None:
        try:
            return str(win32print.GetDefaultPrinter() or "").strip()
        except Exception:
            return ""
    return ""


def print_to_file(content: str, filename: str) -> str:
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
    return filename


def _canvas_font_candidates(family: str) -> List[str]:
    raw = str(family or "").strip()
    alias_map = {
        "TkDefaultFont": "Microsoft YaHei UI",
        "Helvetica": "Arial",
    }
    preferred = alias_map.get(raw, raw) if raw else ""
    out: List[str] = []
    for item in (preferred, raw, "Microsoft YaHei UI", "Arial", "SimSun", "NSimSun", "Consolas"):
        name = str(item or "").strip()
        if name and name not in out:
            out.append(name)
    return out or ["Microsoft YaHei UI"]


def _apply_canvas_letter_spacing(text: str, spacing: int) -> str:
    try:
        gap = max(-6, min(8, int(spacing)))
    except Exception:
        gap = 0
    if gap <= 0:
        return str(text or "")
    return (" " * gap).join(list(str(text or "")))


def _measure_text_px(hdc, text: str) -> int:
    try:
        return int(hdc.GetTextExtent(str(text or ""))[0])
    except Exception:
        return max(1, len(str(text or "")) * 8)


def _wrap_canvas_line_by_width(hdc, text: str, max_width_px: int) -> List[str]:
    raw = str(text or "")
    if max_width_px <= 4:
        return [raw] if raw else [""]
    if not raw:
        return [""]
    out: List[str] = []
    remaining = raw
    while remaining:
        width = 0
        cut = 0
        last_space = -1
        overflowed = False
        for idx, ch in enumerate(remaining):
            ch_width = max(1, _measure_text_px(hdc, ch))
            if cut > 0 and (width + ch_width) > max_width_px:
                overflowed = True
                break
            width += ch_width
            cut = idx + 1
            if ch.isspace():
                last_space = cut
        if cut <= 0:
            cut = 1
        if overflowed and 0 < last_space < cut:
            part = remaining[:last_space].rstrip()
            remaining = remaining[last_space:].lstrip()
        else:
            part = remaining[:cut]
            remaining = remaining[cut:]
        out.append(part or " ")
    return out or [""]


def _wrap_canvas_text(hdc, text: str, max_width_px: int, letter_spacing: int, paragraph_spacing: int) -> List[str]:
    parts = str(text or "").splitlines() or [""]
    line_parts: List[str] = []
    for part in parts:
        spaced = _apply_canvas_letter_spacing(part, letter_spacing)
        line_parts.extend(_wrap_canvas_line_by_width(hdc, spaced, max_width_px))
    if not line_parts:
        return [""]
    out: List[str] = []
    gap_rows = max(0, int(paragraph_spacing))
    for idx, line in enumerate(line_parts):
        out.append(line)
        if idx != (len(line_parts) - 1) and gap_rows:
            out.extend([""] * gap_rows)
    return out


def _draw_canvas_payload(
    hdc,
    payload: Dict[str, Any],
    printable_w: int,
    printable_h: int,
    dpi_x: int,
    dpi_y: int,
    margin_mm: Optional[float],
    physical_offset_x: int = 0,
    physical_offset_y: int = 0,
) -> str:
    canvas_w = max(100, int(payload.get("canvas_w", 420)))
    canvas_h = max(100, int(payload.get("canvas_h", 260)))
    geometry = calculate_canvas_geometry(
        payload,
        dpi_x=dpi_x,
        dpi_y=dpi_y,
        physical_offset_x=physical_offset_x,
        physical_offset_y=physical_offset_y,
    )
    origin_x = float(geometry["origin_x"])
    origin_y = float(geometry["origin_y"])
    scale_x = float(geometry["scale_x"])
    scale_y = float(geometry["scale_y"])
    base_scale = max(0.35, min(scale_x, scale_y))
    font_cache: Dict[tuple, Any] = {}

    def _get_font(family: str, size_px: int, bold: bool):
        key = (family, int(size_px), int(bool(bold)))
        font_obj = font_cache.get(key)
        if font_obj is not None:
            return font_obj
        weight = 700 if bold else 400
        for candidate in _canvas_font_candidates(family):
            try:
                font_obj = win32ui.CreateFont({
                    "name": candidate,
                    "height": -max(8, int(size_px)),
                    "weight": weight,
                })
                break
            except Exception:
                font_obj = None
        if font_obj is None:
            raise RuntimeError("canvas_gdi_font_failed")
        font_cache[key] = font_obj
        return font_obj

    for elem in sorted((payload.get("elements") or []), key=lambda x: (int(x.get("y", 0)), int(x.get("x", 0)))):
        text = str(elem.get("render_text", "") or "")
        if not text:
            continue
        x = int(round(origin_x + (int(elem.get("x", 0)) * scale_x)))
        y = int(round(origin_y + (int(elem.get("y", 0)) * scale_y)))
        w = max(12, int(round(int(elem.get("w", 160)) * scale_x)))
        h = max(12, int(round(int(elem.get("h", 34)) * scale_y)))
        inset_x = max(2, int(round(4 * base_scale)))
        inset_y = max(2, int(round(3 * base_scale)))
        family = str(elem.get("font_family", "Microsoft YaHei UI") or "Microsoft YaHei UI")
        font_px = max(2, int(round(max(2, int(elem.get("font_size", 12))) * dpi_y / 72.0)))
        font_obj = _get_font(family, font_px, bool(int(elem.get("bold", 0))))
        old_font = hdc.SelectObject(font_obj)
        try:
            line_height_px = max(int(hdc.GetTextExtent("Ag")[1]), font_px + max(2, int(round(base_scale * 2))))
            box_width = max(8, w - (inset_x * 2))
            wrapped_lines = _wrap_canvas_text(
                hdc,
                text,
                box_width,
                int(elem.get("letter_spacing", 0)),
                int(elem.get("paragraph_spacing", 0)),
            )
            total_height = max(line_height_px, len(wrapped_lines) * line_height_px)
            valign = str(elem.get("valign", "top")).lower()
            if valign in ("middle", "center"):
                yy = y + max(0, (h - total_height) // 2)
            elif valign == "bottom":
                yy = y + max(inset_y, h - total_height - inset_y)
            else:
                yy = y + inset_y
            align = str(elem.get("align", "left")).lower()
            for line in wrapped_lines:
                if yy > (y + h - line_height_px):
                    break
                text_width = max(0, _measure_text_px(hdc, line))
                if align == "center":
                    tx = x + max(0, (w - text_width) // 2)
                elif align == "right":
                    tx = x + max(inset_x, w - inset_x - text_width)
                else:
                    tx = x + inset_x
                hdc.TextOut(int(tx), int(yy), str(line))
                yy += line_height_px
        finally:
            try:
                hdc.SelectObject(old_font)
            except Exception:
                pass
    return (
        f"canvas_gdi: ok; printable={printable_w}x{printable_h}; "
        f"paper={format_paper_size_mm(geometry['paper_w_mm'], geometry['paper_h_mm'])}mm; "
        f"scale={scale_x:.3f}x{scale_y:.3f}"
    )


def _create_windows_printer_dc(printer: str, width_mm: Optional[float], height_mm: Optional[float]):
    if win32ui is None:
        raise RuntimeError("pywin32 unavailable (win32ui missing)")
    if width_mm and height_mm and win32print is not None and win32gui is not None and win32con is not None:
        try:
            hprinter = win32print.OpenPrinter(printer)
            try:
                devmode = win32print.GetPrinter(hprinter, 2)["pDevMode"]
            finally:
                win32print.ClosePrinter(hprinter)
            devmode.Fields |= int(getattr(win32con, "DM_PAPERSIZE", 0x00000002))
            devmode.Fields |= int(getattr(win32con, "DM_PAPERWIDTH", 0x00000008))
            devmode.Fields |= int(getattr(win32con, "DM_PAPERLENGTH", 0x00000004))
            devmode.Fields |= int(getattr(win32con, "DM_ORIENTATION", 0x00000001))
            devmode.Fields |= int(getattr(win32con, "DM_SCALE", 0x00000010))
            devmode.PaperSize = 0
            devmode.PaperWidth = mm_to_devmode_units(width_mm)
            devmode.PaperLength = mm_to_devmode_units(height_mm)
            # Width and height already describe the final label orientation.
            devmode.Orientation = int(getattr(win32con, "DMORIENT_PORTRAIT", 1))
            devmode.Scale = 100
            hdc_handle = win32gui.CreateDC("WINSPOOL", printer, devmode)
            hdc = win32ui.CreateDCFromHandle(hdc_handle)
            return hdc, f" size={format_paper_size_mm(width_mm, height_mm)}mm applied=True;"
        except Exception as exc:
            raise RuntimeError(
                f"custom paper {format_paper_size_mm(width_mm, height_mm)}mm rejected: {exc}"
            ) from exc
    hdc = win32ui.CreateDC()
    hdc.CreatePrinterDC(printer)
    if width_mm and height_mm:
        return hdc, f" size={format_paper_size_mm(width_mm, height_mm)}mm applied=False;"
    return hdc, ""


def send_to_printer(
    printer: str,
    filepath: str,
    width_mm: Optional[float] = None,
    height_mm: Optional[float] = None,
    canvas_mode: bool = False,
    preformatted_mode: bool = False,
    canvas_payload: Optional[Dict[str, Any]] = None,
    font_scale: float = 1.0,
    char_width_mm: Optional[float] = None,
    line_height_mm: Optional[float] = None,
    margin_mm: Optional[float] = None,
) -> bool:
    """Send file to system printer."""
    ok, _detail = send_to_printer_debug(
        printer,
        filepath,
        width_mm=width_mm,
        height_mm=height_mm,
        canvas_mode=canvas_mode,
        preformatted_mode=preformatted_mode,
        canvas_payload=canvas_payload,
        font_scale=font_scale,
        char_width_mm=char_width_mm,
        line_height_mm=line_height_mm,
        margin_mm=margin_mm,
    )
    return bool(ok)


def send_to_printer_debug(
    printer: str,
    filepath: str,
    width_mm: Optional[float] = None,
    height_mm: Optional[float] = None,
    canvas_mode: bool = False,
    preformatted_mode: bool = False,
    canvas_payload: Optional[Dict[str, Any]] = None,
    font_scale: float = 1.0,
    char_width_mm: Optional[float] = None,
    line_height_mm: Optional[float] = None,
    margin_mm: Optional[float] = None,
):
    """Return (ok, detail). detail contains attempted methods and errors."""
    if not printer:
        return False, "printer is empty"
    if not filepath or not os.path.exists(filepath):
        return False, f"file not found: {filepath}"

    if IS_WINDOWS:
        # Single-path strategy requested by user:
        # Use only pywin32 + Windows driver (GDI text rendering).
        if win32ui is None or win32con is None:
            return False, "pywin32 unavailable (win32ui/win32con missing)"
        if canvas_mode and isinstance(canvas_payload, dict):
            payload_w = _positive_float(canvas_payload.get("paper_mm_w"), width_mm or 40.0)
            payload_h = _positive_float(canvas_payload.get("paper_mm_h"), height_mm or 30.0)
            if width_mm and height_mm:
                if abs(float(width_mm) - payload_w) > 0.05 or abs(float(height_mm) - payload_h) > 0.05:
                    return False, (
                        "canvas paper size mismatch: "
                        f"job={format_paper_size_mm(width_mm, height_mm)}mm; "
                        f"template={format_paper_size_mm(payload_w, payload_h)}mm"
                    )
            width_mm, height_mm = payload_w, payload_h
        hdc = None
        doc_started = False
        doc_finished = False
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            if not text:
                text = " "
            hdc, size_msg = _create_windows_printer_dc(printer, width_mm, height_mm)
            printable_w = hdc.GetDeviceCaps(8)   # HORZRES
            printable_h = hdc.GetDeviceCaps(10)  # VERTRES
            dpi_x = max(120, int(hdc.GetDeviceCaps(getattr(win32con, "LOGPIXELSX", 88)) or 203))
            dpi_y = max(120, int(hdc.GetDeviceCaps(getattr(win32con, "LOGPIXELSY", 90)) or 203))
            physical_w = int(hdc.GetDeviceCaps(110) or 0)  # PHYSICALWIDTH
            physical_h = int(hdc.GetDeviceCaps(111) or 0)  # PHYSICALHEIGHT
            physical_offset_x = int(hdc.GetDeviceCaps(112) or 0)  # PHYSICALOFFSETX
            physical_offset_y = int(hdc.GetDeviceCaps(113) or 0)  # PHYSICALOFFSETY
            page_size_msg = ""
            if width_mm and height_mm:
                page_ok, page_size_msg = validate_physical_page_size(
                    width_mm,
                    height_mm,
                    physical_width_px=physical_w,
                    physical_height_px=physical_h,
                    dpi_x=dpi_x,
                    dpi_y=dpi_y,
                )
                if not page_ok:
                    return False, f"printer driver did not apply requested paper size: {page_size_msg}"
            try:
                margin_val = max(0.2, float(margin_mm if margin_mm is not None else 1.0))
            except Exception:
                margin_val = 1.0
            try:
                char_mm_val = max(0.8, float(char_width_mm if char_width_mm is not None else 1.5))
            except Exception:
                char_mm_val = 1.5
            try:
                line_mm_val = max(1.0, float(line_height_mm if line_height_mm is not None else 2.8))
            except Exception:
                line_mm_val = 2.8
            # Thermal-friendly defaults with user calibration support.
            left_margin = max(2, int(dpi_x * margin_val / 25.4))
            top_margin = max(2, int(dpi_y * margin_val / 25.4))
            y = top_margin
            line_height = max(8, int(dpi_y * line_mm_val / 25.4))
            try:
                fs = max(0.35, min(3.0, float(font_scale)))
            except Exception:
                fs = 1.0
            font_mm = max(0.8, min(8.0, 1.6 * fs))
            hdc.StartDoc(os.path.basename(filepath))
            doc_started = True
            hdc.StartPage()
            if canvas_mode and isinstance(canvas_payload, dict) and canvas_payload.get("elements"):
                detail = _draw_canvas_payload(
                    hdc,
                    canvas_payload,
                    printable_w=printable_w,
                    printable_h=printable_h,
                    dpi_x=dpi_x,
                    dpi_y=dpi_y,
                    margin_mm=margin_mm,
                    physical_offset_x=physical_offset_x,
                    physical_offset_y=physical_offset_y,
                )
                hdc.EndPage()
                hdc.EndDoc()
                doc_finished = True
                return True, f"{detail};{size_msg} {page_size_msg}; dpi={dpi_x}x{dpi_y}"
            font = None
            for fname in ("NSimSun", "SimSun", "Microsoft YaHei UI", "Consolas"):
                try:
                    font = win32ui.CreateFont({
                        "name": fname,
                        "height": -max(10, int(dpi_y * font_mm / 25.4)),
                        "weight": 400,
                    })
                    break
                except Exception:
                    font = None
            if font is None:
                return False, "pywin32 GDI text: create font failed"
            old_font = hdc.SelectObject(font)
            try:
                char_w = max(4, int(dpi_x * char_mm_val / 25.4))
                max_chars = max(10, int((printable_w - left_margin * 2) / char_w))

                def _char_cells(ch: str) -> int:
                    try:
                        return 2 if unicodedata.east_asian_width(str(ch)) in ("W", "F") else 1
                    except Exception:
                        return 1

                def _slice_by_cells(s: str, max_cells: int):
                    text = str(s or "")
                    if max_cells <= 0:
                        return "", text
                    used = 0
                    out = []
                    for idx, ch in enumerate(text):
                        cw = _char_cells(ch)
                        if used + cw > max_cells:
                            return "".join(out), text[idx:]
                        out.append(ch)
                        used += cw
                    return "".join(out), ""

                for raw_line in text.splitlines() or [" "]:
                    line = raw_line if raw_line else " "
                    if canvas_mode or preformatted_mode:
                        hdc.TextOut(left_margin, y, line)
                        y += line_height
                        if y >= (printable_h - top_margin):
                            hdc.EndPage()
                            hdc.StartPage()
                            y = top_margin
                        continue
                    while line:
                        part, rest = _slice_by_cells(line, max_chars)
                        if not part and rest:
                            part, rest = rest[0], rest[1:]
                        hdc.TextOut(left_margin, y, part)
                        line = rest
                        y += line_height
                        if y >= (printable_h - top_margin):
                            hdc.EndPage()
                            hdc.StartPage()
                            y = top_margin
            finally:
                hdc.SelectObject(old_font)
            hdc.EndPage()
            hdc.EndDoc()
            doc_finished = True
            return True, (
                f"pywin32 GDI text: ok;{size_msg} {page_size_msg}; "
                f"printable={printable_w}x{printable_h}; dpi={dpi_x}x{dpi_y}"
            )
        except Exception as e:
            return False, f"pywin32 GDI text: {e}"
        finally:
            if hdc is not None:
                if doc_started and not doc_finished:
                    try:
                        hdc.AbortDoc()
                    except Exception:
                        pass
                try:
                    hdc.DeleteDC()
                except Exception:
                    pass

    # POSIX default
    try:
        subprocess.check_call(["lp", "-d", printer, filepath])
        return True, "lp: ok"
    except Exception as e:
        return False, f"lp: {e}"


def set_printer_size(printer: str, width_mm: float, height_mm: float) -> bool:
    """Attempt to set the printer page size / DEVMODE to a custom size (mm).

    This is best-effort and requires `pywin32` on Windows. It updates the
    printer DEVMODE and reports failure if the driver rejects the custom size.
    """
    if not IS_WINDOWS or win32print is None:
        return False
    try:
        # Open printer and get current devmode
        hPrinter = win32print.OpenPrinter(printer)
        try:
            devmode = win32print.GetPrinter(hPrinter, 2)['pDevMode']
            dm_paper_size = int(getattr(win32con, "DM_PAPERSIZE", 0x00000002))
            dm_paper_width = int(getattr(win32con, "DM_PAPERWIDTH", 0x00000008))
            dm_paper_length = int(getattr(win32con, "DM_PAPERLENGTH", 0x00000004))
            dm_orientation = int(getattr(win32con, "DM_ORIENTATION", 0x00000001))
            dm_scale = int(getattr(win32con, "DM_SCALE", 0x00000010))
            devmode.Fields |= (dm_paper_size | dm_paper_width | dm_paper_length | dm_orientation | dm_scale)
            devmode.PaperSize = 0
            devmode.PaperWidth = mm_to_devmode_units(width_mm)
            devmode.PaperLength = mm_to_devmode_units(height_mm)
            devmode.Orientation = int(getattr(win32con, "DMORIENT_PORTRAIT", 1))
            devmode.Scale = 100
            win32print.SetPrinter(hPrinter, 2, {'pDevMode': devmode}, 0)
        finally:
            win32print.ClosePrinter(hPrinter)
        return True
    except Exception:
        return False
