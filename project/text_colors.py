class Colors:
    HEADER = '\033[95m'
    END = '\033[0m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    def header(self, text_to_wrap):
        return self.HEADER + text_to_wrap + self.END

    def bold(self, text_to_wrap):
        return self.BOLD + text_to_wrap + self.END

    def underline(self, text_to_wrap):
        return self.UNDERLINE + text_to_wrap + self.END

    def blue(self, text_to_wrap):
        return self.BLUE + text_to_wrap + self.END

    def cyan(self, text_to_wrap):
        return self.CYAN + text_to_wrap + self.END

    def green(self, text_to_wrap):
        return self.GREEN + text_to_wrap + self.END

    def warning(self, text_to_wrap):
        return self.WARNING + text_to_wrap + self.END

    def red(self, text_to_wrap):
        return self.RED + text_to_wrap + self.END
