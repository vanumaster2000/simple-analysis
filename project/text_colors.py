class Colors:
    _HEADER = '\033[95m'
    _END = '\033[0m'
    _BLUE = '\033[94m'
    _CYAN = '\033[96m'
    _GREEN = '\033[92m'
    _WARNING = '\033[93m'
    _RED = '\033[91m'
    _BOLD = '\033[1m'
    _UNDERLINE = '\033[4m'

    def header(self, text_to_wrap: str) -> str:
        """
        Окрашивает текст в ярко-фиолетовый цвет
        :param text_to_wrap: текст для окрашивания
        :return: строка с ANSI-последовательностью
        """
        return self._HEADER + str(text_to_wrap) + self._END

    def bold(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в полужирном начертании
        :param text_to_wrap: текст для утолщения
        :return: строка с ANSI-последовательностью
        """
        return self._BOLD + str(text_to_wrap) + self._END

    def underline(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в подчеркнутом начертании
        :param text_to_wrap: текст для подчеркивания
        :return: строка с ANSI-последовательностью
        """
        return self._UNDERLINE + str(text_to_wrap) + self._END

    def blue(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в синем цвете
        :param text_to_wrap: текст для окрашивания
        :return: строка с ANSI-последовательностью
        """
        return self._BLUE + str(text_to_wrap) + self._END

    def cyan(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в светло-голубом цвете
        :param text_to_wrap: текст для окрашивания
        :return: строка с ANSI-последовательностью
        """
        return self._CYAN + str(text_to_wrap) + self._END

    def green(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в зеленом цвете
        :param text_to_wrap: текст для окрашивания
        :return: строка с ANSI-последовательностью
        """
        return self._GREEN + str(text_to_wrap) + self._END

    def orange(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в оранжевом цвете
        :param text_to_wrap: текст для окрашивания
        :return: строка с ANSI-последовательностью
        """
        return self._WARNING + str(text_to_wrap) + self._END

    def red(self, text_to_wrap: str) -> str:
        """
        Возвращает текст в красном цвете
        :param text_to_wrap: текст для окрашивания
        :return: строка с ANSI-последовательностью
        """
        return self._RED + str(text_to_wrap) + self._END
