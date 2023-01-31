# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import os
import sys


def vt_codes_enabled_in_windows_registry():
    """
    Check the Windows Registry to see if VT code handling has been enabled
    by default, see https://superuser.com/a/1300251/447564.
    """
    try:
        # winreg is only available on Windows.
        import winreg
    except ImportError:
        return False
    else:
        try:
            reg_key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Console")
            reg_key_value, _ = winreg.QueryValueEx(reg_key, "VirtualTerminalLevel")
        except FileNotFoundError:
            return False
        else:
            return reg_key_value == 1


def supports_color():
    """
    Return True if the running system's terminal supports color,
    and False otherwise.
    """

    is_a_tty = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    return is_a_tty and (
        sys.platform != "win32"
        or "ANSICON" in os.environ
        or "WT_SESSION" in os.environ  # Windows Terminal supports VT codes.
        or os.environ.get("TERM_PROGRAM") == "vscode"  # VSCode's built-in terminal supports colors.
        or vt_codes_enabled_in_windows_registry()
    )


class Bcolors:
    PURPLE = "\033[95m" if supports_color() else ""
    BLUE = "\033[94m" if supports_color() else ""
    CYAN = "\033[96m" if supports_color() else ""
    GREEN = "\033[92m" if supports_color() else ""
    BOLD = "\033[1m" if supports_color() else ""
    UNDERLINE = "\033[4m" if supports_color() else ""
    END = "\033[0m" if supports_color() else ""
