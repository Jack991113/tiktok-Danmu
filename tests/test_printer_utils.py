import unittest

import printer_utils


class PrinterGeometryTests(unittest.TestCase):
    def test_paper_size_parser_preserves_decimal_millimeters(self):
        width_mm, height_mm = printer_utils.parse_paper_size_mm("40.5 x 30.2mm")

        self.assertEqual(width_mm, 40.5)
        self.assertEqual(height_mm, 30.2)
        self.assertEqual(printer_utils.format_paper_size_mm(width_mm, height_mm), "40.5x30.2")
        self.assertEqual(printer_utils.mm_to_devmode_units(40.5), 405)
        self.assertEqual(printer_utils.mm_to_devmode_units(30.2), 302)

    def test_canvas_geometry_maps_template_to_physical_page_millimeters(self):
        geometry = printer_utils.calculate_canvas_geometry(
            {
                "canvas_w": 200,
                "canvas_h": 150,
                "paper_mm_w": 40,
                "paper_mm_h": 30,
            },
            dpi_x=203,
            dpi_y=203,
            physical_offset_x=8,
            physical_offset_y=12,
        )

        self.assertAlmostEqual(geometry["scale_x"], (203 / 25.4) / 5, places=6)
        self.assertAlmostEqual(geometry["scale_y"], (203 / 25.4) / 5, places=6)
        self.assertEqual(geometry["origin_x"], -8)
        self.assertEqual(geometry["origin_y"], -12)
        self.assertAlmostEqual(
            geometry["origin_x"] + (5 * geometry["scale_x"]),
            (203 / 25.4) - 8,
            places=6,
        )

    def test_canvas_geometry_uses_explicit_paper_size_for_legacy_units(self):
        geometry = printer_utils.calculate_canvas_geometry(
            {
                "canvas_w": 420,
                "canvas_h": 260,
                "paper_mm_w": 40,
                "paper_mm_h": 30,
            },
            dpi_x=300,
            dpi_y=300,
        )

        self.assertAlmostEqual(geometry["scale_x"] * 420, 40 * 300 / 25.4, places=6)
        self.assertAlmostEqual(geometry["scale_y"] * 260, 30 * 300 / 25.4, places=6)

    def test_page_size_validation_rejects_wrong_or_swapped_driver_page(self):
        ok, detail = printer_utils.validate_physical_page_size(
            40,
            30,
            physical_width_px=round(40 * 203 / 25.4),
            physical_height_px=round(30 * 203 / 25.4),
            dpi_x=203,
            dpi_y=203,
        )
        self.assertTrue(ok, detail)

        swapped_ok, swapped_detail = printer_utils.validate_physical_page_size(
            40,
            30,
            physical_width_px=round(30 * 203 / 25.4),
            physical_height_px=round(40 * 203 / 25.4),
            dpi_x=203,
            dpi_y=203,
        )
        self.assertFalse(swapped_ok)
        self.assertIn("requested=40x30mm", swapped_detail)

    def test_windows_devmode_uses_custom_size_without_predefined_paper(self):
        class DevMode:
            Fields = 0
            PaperSize = 99
            PaperWidth = 0
            PaperLength = 0
            Orientation = 1
            Scale = 50

        devmode = DevMode()

        class Win32Print:
            @staticmethod
            def OpenPrinter(_printer):
                return "handle"

            @staticmethod
            def GetPrinter(_handle, _level):
                return {"pDevMode": devmode}

            @staticmethod
            def ClosePrinter(_handle):
                return None

        class Win32Gui:
            @staticmethod
            def CreateDC(_driver, _printer, passed_devmode):
                self.assertIs(passed_devmode, devmode)
                return "dc-handle"

        class Win32Ui:
            @staticmethod
            def CreateDCFromHandle(handle):
                return handle

        class Win32Con:
            DM_PAPERSIZE = 0x2
            DM_PAPERWIDTH = 0x8
            DM_PAPERLENGTH = 0x4
            DM_ORIENTATION = 0x1
            DM_SCALE = 0x10
            DMORIENT_PORTRAIT = 1
            DMORIENT_LANDSCAPE = 2

        saved = {
            name: getattr(printer_utils, name, None)
            for name in ("win32print", "win32gui", "win32ui", "win32con")
        }
        try:
            printer_utils.win32print = Win32Print
            printer_utils.win32gui = Win32Gui
            printer_utils.win32ui = Win32Ui
            printer_utils.win32con = Win32Con
            _dc, detail = printer_utils._create_windows_printer_dc("Thermal", 40.5, 30.2)
        finally:
            for name, value in saved.items():
                if value is None and hasattr(printer_utils, name):
                    delattr(printer_utils, name)
                else:
                    setattr(printer_utils, name, value)

        self.assertEqual(devmode.PaperSize, 0)
        self.assertEqual(devmode.PaperWidth, 405)
        self.assertEqual(devmode.PaperLength, 302)
        self.assertEqual(devmode.Orientation, 1)
        self.assertEqual(devmode.Scale, 100)
        self.assertIn("40.5x30.2mm", detail)


if __name__ == "__main__":
    unittest.main()
