# logger_config.py
import logging
import sys
import re

# ---------- helpers ----------

_CTRL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def _sanitize_text(text: str) -> str:
    """Remove control characters (incl. NUL) except common whitespace."""
    if not isinstance(text, str):
        text = str(text)
    return _CTRL_RE.sub("", text)


# ---------- safe logger class ----------

class SafeLogger(logging.Logger):
    """
    A logger that tolerates calls like:
        logger.info("label:", value)
    even when the message has no %-placeholders.
    If args exist and '%' not in msg -> concatenates args into the message.
    Otherwise, uses normal %-style formatting.
    """
    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):
        if args and "%" not in str(msg):
            # Concatenate args as text, adding a space unless msg already ends with punctuation/space
            suffix = " ".join(map(str, args))
            sep = "" if str(msg).endswith((" ", ":", "·", "-", "—")) else " "
            msg = f"{msg}{sep}{suffix}"
            args = ()  # prevent downstream formatting attempts
        super()._log(level, msg, args, exc_info=exc_info, extra=extra, stack_info=stack_info, stacklevel=stacklevel)


# Install SafeLogger BEFORE any loggers are created
logging.setLoggerClass(SafeLogger)


# ---------- formatters ----------

class ColorFormatter(logging.Formatter):
    RESET  = "\x1b[0m"
    BOLD   = "\x1b[1m"
    BLACK  = "\x1b[30m"
    RED    = "\x1b[31m"
    GREEN  = "\x1b[32m"
    YELLOW = "\x1b[33m"
    BLUE   = "\x1b[34m"
    GRAY   = "\x1b[38m"

    COLORS = {
        logging.DEBUG:   GRAY + BOLD,
        logging.INFO:    BLUE + BOLD,
        logging.WARNING: YELLOW + BOLD,
        logging.ERROR:   RED,
        logging.CRITICAL: RED + BOLD,
    }

    def __init__(self, datefmt="%Y-%m-%d %H:%M:%S"):
        super().__init__(datefmt=datefmt, style="{")

    def format(self, record: logging.LogRecord) -> str:
        msg = _sanitize_text(record.getMessage())
        ts    = self.formatTime(record, self.datefmt)
        level = record.levelname
        name  = record.name

        level_color = self.COLORS.get(record.levelno, self.GRAY)
        name_color  = self.GREEN + self.BOLD
        time_color  = self.BLACK + self.BOLD

        out = (
            f"{time_color}[{ts}]{self.RESET} "
            f"{level_color}[{level:<8}]{self.RESET} "
            f"{name_color}{name}{self.RESET}: {msg}"
        )

        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            out += "\n" + _sanitize_text(exc_text)
        if record.stack_info:
            out += "\n" + self.formatStack(record.stack_info)
        return out


class PlainFormatter(logging.Formatter):
    def __init__(self, datefmt="%Y-%m-%d %H:%M:%S"):
        super().__init__("[{asctime}] [{levelname:<8}] {name}: {message}", datefmt=datefmt, style="{")

    def format(self, record: logging.LogRecord) -> str:
        # Let logging compute record.message via getMessage(), then sanitize it
        s = super().format(record)
        return _sanitize_text(s)


# ---------- setup ----------

def get_logger(
    name: str = "discord_bot",
    level: int = logging.INFO,
    logfile: str = "discord.log",
) -> logging.Logger:
    """
    Create/return a configured logger with:
      - colorized console output (TTY only)
      - plain file logging (UTF-8, TRUNCATE on start)
      - control-character sanitization
      - no duplicate handlers on repeated imports
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(level)
    logger.propagate = False

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(ColorFormatter() if sys.stdout.isatty() else PlainFormatter())
    logger.addHandler(ch)

    # File handler (truncate each start; use mode="a" to append instead)
    fh = logging.FileHandler(filename=logfile, encoding="utf-8", mode="w")
    fh.setLevel(level)
    fh.setFormatter(PlainFormatter())
    logger.addHandler(fh)

    return logger


# Module-level logger you can import directly:
logger = get_logger()
